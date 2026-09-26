import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";

const rmGate = vi.hoisted(() => ({
  target: undefined as string | undefined,
  during: undefined as undefined | (() => Promise<void>),
}));

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    rm: async (path: Parameters<typeof actual.rm>[0], options?: Parameters<typeof actual.rm>[1]) => {
      const during = rmGate.during;
      if (during !== undefined && rmGate.target !== undefined && resolve(String(path)) === rmGate.target) {
        rmGate.during = undefined;
        await during();
      }
      return actual.rm(path, options);
    },
  };
});

import { PluginInstallTransactionSimulatedCrash } from "../../../src/plugins/cli/plugin-install-transaction.js";
import { installPluginOp, updatePluginOp } from "../../../src/plugins/cli/pluginOperations.js";
import { loadPlugins } from "../../../src/plugins/loader.js";

type Outcome = { readonly ok: true } | { readonly ok: false; readonly error: unknown };

async function writePlugin(root: string, version: string): Promise<string> {
  const pluginRoot = join(root, `demo-${version}`);
  await mkdir(join(pluginRoot, ".agenc-plugin"), { recursive: true });
  await writeFile(
    join(pluginRoot, ".agenc-plugin", "plugin.json"),
    `${JSON.stringify({ name: "demo", version, description: `demo ${version}`, commands: "./commands" })}\n`,
  );
  await mkdir(join(pluginRoot, "commands"), { recursive: true });
  await writeFile(join(pluginRoot, "commands", "hello.md"), `# Hello ${version}\n`);
  return pluginRoot;
}

async function version(pluginRoot: string): Promise<string | undefined> {
  try {
    return (JSON.parse(await readFile(join(pluginRoot, ".agenc-plugin", "plugin.json"), "utf8")) as { version?: string }).version;
  } catch (error) {
    return `<${(error as NodeJS.ErrnoException).code}>`;
  }
}

describe("plugin install recovery vs a concurrent operation on the same plugin", () => {
  it("never reports a successful update whose result recovery then removes", async () => {
    const root = await mkdtemp(join(tmpdir(), "plugin-race-"));
    const agencHome = join(root, "home");
    const workspaceRoot = join(root, "workspace");
    const pluginStorageRoot = join(agencHome, "plugins");
    await mkdir(workspaceRoot, { recursive: true });
    await mkdir(pluginStorageRoot, { recursive: true });
    const authority = {
      agencHome,
      pluginStorageRoot,
      sessionTempRoot: join(agencHome, "tmp"),
      workspaceRoot,
      env: Object.freeze({}) as NodeJS.ProcessEnv,
    };

    const v1 = await installPluginOp({ ...authority, source: await writePlugin(root, "1.0.0") });
    const destination = resolve(v1.destination);

    await expect(updatePluginOp({
      ...authority,
      pluginId: "demo",
      source: await writePlugin(root, "2.0.0"),
      installTransactionHooks: {
        afterPhase: async (phase) => {
          if (phase === "config-published") throw new PluginInstallTransactionSimulatedCrash(phase);
        },
      },
    })).rejects.toBeInstanceOf(PluginInstallTransactionSimulatedCrash);
    expect(await version(destination)).toBe("2.0.0");

    // A second update starts inside recovery's identity-check → rm window.
    // A blocking lock may hold it past the window, so recovery is released after 2 s.
    let second: Promise<Outcome> | undefined;
    const source3 = await writePlugin(root, "3.0.0");
    rmGate.target = destination;
    rmGate.during = async () => {
      second = updatePluginOp({ ...authority, pluginId: "demo", source: source3 }).then(
        (): Outcome => ({ ok: true }),
        (error: unknown): Outcome => ({ ok: false, error }),
      );
      await Promise.race([second, new Promise((resolveTimer) => setTimeout(resolveTimer, 2_000))]);
    };

    const loaded = await loadPlugins({
      pluginStorageRoot,
      workspaceRoot,
      config: { plugins: { enabled: true } },
      userConfigPath: join(agencHome, "config.toml"),
    });
    expect(second).toBeDefined();
    const outcome = await second!;
    const finalVersion = await version(destination);

    console.log(JSON.stringify({
      secondUpdate: outcome.ok ? "resolved" : `rejected: ${String((outcome.error as Error)?.message ?? outcome.error)}`,
      recoveryIssues: loaded.errors.filter((issue) => issue.type === "install-recovery").map((issue) => issue.message),
      finalVersion,
    }, null, 2));

    if (outcome.ok) expect(finalVersion).toBe("3.0.0");
  });
});
