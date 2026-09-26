import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";

const rmGate = vi.hoisted(() => ({
  target: undefined as string | undefined,
  during: undefined as undefined | (() => Promise<void>),
}));

// After `armAfterRm` sees its rm finish, the next readdir of `target` returns
// the entries read before `during` runs.
const readdirGate = vi.hoisted(() => ({
  armAfterRm: undefined as string | undefined,
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
      await actual.rm(path, options);
      if (readdirGate.armAfterRm !== undefined && resolve(String(path)) === readdirGate.armAfterRm) {
        readdirGate.armAfterRm = undefined;
      }
    },
    readdir: (async (path: string, options?: unknown) => {
      const entries = await (actual.readdir as (p: string, o?: unknown) => Promise<unknown>)(path, options);
      const during = readdirGate.during;
      if (during !== undefined && readdirGate.armAfterRm === undefined && readdirGate.target !== undefined
        && resolve(String(path)) === readdirGate.target) {
        readdirGate.during = undefined;
        await during();
      }
      return entries;
    }) as typeof actual.readdir,
  };
});

import { PluginInstallTransactionSimulatedCrash } from "../../../src/plugins/cli/plugin-install-transaction.js";
import { setPluginInstallDirectoryLockWaitHook } from "../../../src/plugins/cli/plugin-install-directory-lock.js";
import { installPluginOp, uninstallPluginOp, updatePluginOp } from "../../../src/plugins/cli/pluginOperations.js";
import { readCanonicalUserConfigSnapshotSync } from "../../../src/config/update-sync.js";
import { loadPlugins } from "../../../src/plugins/loader.js";

type Outcome =
  | { readonly ok: true; readonly version: string | undefined }
  | { readonly ok: false; readonly error: unknown };

interface DirectoryLockWaitModule {
  setPluginInstallDirectoryLockWaitHook(hook: (() => void) | undefined): void;
}

const DIRECTORY_LOCK_SPECIFIER = "../../../src/plugins/cli/plugin-install-directory-lock" + ".ts";

function directoryLockSourceExists(): boolean {
  return existsSync(fileURLToPath(new URL(DIRECTORY_LOCK_SPECIFIER, import.meta.url)));
}

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

async function raceWorld(): Promise<{
  readonly root: string;
  readonly agencHome: string;
  readonly workspaceRoot: string;
  readonly pluginStorageRoot: string;
  readonly authority: {
    readonly agencHome: string;
    readonly pluginStorageRoot: string;
    readonly sessionTempRoot: string;
    readonly workspaceRoot: string;
    readonly env: NodeJS.ProcessEnv;
  };
}> {
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
  return { root, agencHome, workspaceRoot, pluginStorageRoot, authority };
}

function demoConfigEntry(agencHome: string): unknown {
  const raw = readCanonicalUserConfigSnapshotSync(join(agencHome, "config.toml")).raw as {
    plugins?: { plugins?: Record<string, unknown> };
  };
  return raw.plugins?.plugins?.demo;
}

describe("plugin install recovery vs a concurrent operation on the same plugin", () => {
  it("never reports a successful update whose result recovery then removes", async () => {
    const { root, agencHome, workspaceRoot, pluginStorageRoot, authority } = await raceWorld();

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

    // The second update starts inside recovery's identity-check → rm window.
    // With a directory lock, it blocks until recovery releases that lock.
    // Without one, it can finish inside the window and recovery then removes it.
    let second: Promise<Outcome> | undefined;
    let barrierError: unknown;
    const source3 = await writePlugin(root, "3.0.0");
    const lockReady = directoryLockSourceExists()
      ? await import(DIRECTORY_LOCK_SPECIFIER) as DirectoryLockWaitModule
      : undefined;
    rmGate.target = destination;
    rmGate.during = async () => {
      try {
        if (lockReady !== undefined) {
          const reached = new Promise<void>((resolveBarrier, rejectBarrier) => {
            const timer = setTimeout(() => {
              lockReady.setPluginInstallDirectoryLockWaitHook(undefined);
              rejectBarrier(new Error("did not reach the lock"));
            }, 10_000);
            lockReady.setPluginInstallDirectoryLockWaitHook(() => {
              clearTimeout(timer);
              lockReady.setPluginInstallDirectoryLockWaitHook(undefined);
              resolveBarrier();
            });
          });
          second = updatePluginOp({ ...authority, pluginId: "demo", source: source3 }).then(
            (result): Outcome => ({ ok: true, version: result.plugin.version }),
            (error: unknown): Outcome => ({ ok: false, error }),
          );
          await reached;
          return;
        }
        second = updatePluginOp({ ...authority, pluginId: "demo", source: source3 }).then(
          (result): Outcome => ({ ok: true, version: result.plugin.version }),
          (error: unknown): Outcome => ({ ok: false, error }),
        );
        await second;
      } catch (error) {
        barrierError = error;
        throw error;
      }
    };

    try {
      const loaded = await loadPlugins({
        pluginStorageRoot,
        workspaceRoot,
        config: { plugins: { enabled: true } },
        userConfigPath: join(agencHome, "config.toml"),
      });
      if (barrierError !== undefined) throw barrierError;
      expect(second).toBeDefined();
      const outcome = await second!;
      const finalVersion = await version(destination);

      console.log(JSON.stringify({
        secondUpdate: outcome.ok ? "resolved" : `rejected: ${String((outcome.error as Error)?.message ?? outcome.error)}`,
        recoveryIssues: loaded.errors.filter((issue) => issue.type === "install-recovery").map((issue) => issue.message),
        finalVersion,
      }, null, 2));

      expect(outcome.ok).toBe(true);
      if (!outcome.ok) throw outcome.error;
      expect(outcome.version).toBe("3.0.0");
      expect(finalVersion).toBe("3.0.0");
    } finally {
      lockReady?.setPluginInstallDirectoryLockWaitHook(undefined);
    }
  });
});

describe("plugin uninstall vs a concurrent install of the same plugin", () => {
  it("does not remove the config of an install that commits during uninstall cleanup", async () => {
    const { root, agencHome, pluginStorageRoot, authority } = await raceWorld();
    const v1 = await installPluginOp({ ...authority, source: await writePlugin(root, "1.0.0") });
    const destination = resolve(v1.destination);
    const source2 = await writePlugin(root, "2.0.0");
    let start: () => void = () => {};
    let markWaiting: () => void = () => {};
    const waiting = new Promise<void>((resolveWaiting) => {
      markWaiting = resolveWaiting;
    });
    // Chained here, outside uninstall's call, so the install runs as an
    // independent operation and not as a nested call inside uninstall.
    const install = new Promise<void>((resolveStart) => {
      start = resolveStart;
    }).then(() => installPluginOp({ ...authority, source: source2 })).then(
      (result): Outcome => ({ ok: true, version: result.plugin.version }),
      (error: unknown): Outcome => ({ ok: false, error }),
    );
    try {
      setPluginInstallDirectoryLockWaitHook(() => markWaiting());
      readdirGate.armAfterRm = destination;
      readdirGate.target = await realpath(pluginStorageRoot);
      readdirGate.during = async () => {
        start();
        await Promise.race([install, waiting]);
      };
      await uninstallPluginOp({ ...authority, pluginId: "demo" });
      expect(readdirGate.during).toBeUndefined();
      const outcome = await install;
      expect(outcome.ok ? outcome.version : outcome.error).toBe("2.0.0");
      expect(await version(destination)).toBe("2.0.0");
      expect(demoConfigEntry(agencHome)).toMatchObject({ enabled: true });
    } finally {
      readdirGate.armAfterRm = undefined;
      readdirGate.target = undefined;
      readdirGate.during = undefined;
      setPluginInstallDirectoryLockWaitHook(undefined);
    }
  });
});
