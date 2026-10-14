import { afterEach, expect, test, vi } from "vitest";

import { withOneShotFastMode } from "../../../src/one-shot-fast-mode.js";
import { createExecCommandTool as createUnboundExecCommandTool } from "../../../src/tools/system/exec-command.js";
import * as shellPermission from "../../../src/tools/system/shell-mutation-permission.js";
import { attachToolRuntimeContext } from "../../../src/tools/runtimes/context.js";
import { bindExplicitDangerBoundary } from "../../helpers/explicit-danger-boundary.js";
import type { ExecCommandToolOutput, UnifiedExecProcessManagerLike } from "../../../src/unified-exec/types.js";

const createExecCommandTool = (
  config: Parameters<typeof createUnboundExecCommandTool>[0],
) => bindExplicitDangerBoundary(createUnboundExecCommandTool(config));

afterEach(() => {
  vi.restoreAllMocks();
});

function completedExecOutput(stdout: string): ExecCommandToolOutput {
  return {
    output: stdout,
    stdout,
    stderr: "",
    exitCode: 0,
    exit_code: 0,
    durationMs: 1,
    wall_time_seconds: 0.001,
    timedOut: false,
    truncated: false,
    original_token_count: 1,
  };
}

function managerTool() {
  const execCommand = vi.fn<UnifiedExecProcessManagerLike["execCommand"]>(
    async () => completedExecOutput("ran"),
  );
  const manager: UnifiedExecProcessManagerLike = {
    maxTimeoutMs: 30_000,
    execCommand,
    writeStdin: vi.fn(async () => completedExecOutput("")),
    closeAll: vi.fn(async () => {}),
  };
  return {
    execCommand,
    tool: createExecCommandTool({
      cwd: "/tmp/workspace",
      allowedPaths: ["/tmp/workspace"],
      unifiedExecManager: manager,
    }),
  };
}

function commandArgs(
  sessionId: string,
  cmd: string,
  sandboxMode: "danger_full_access" | "workspace_write",
): Record<string, unknown> {
  const args: Record<string, unknown> = {
    cmd,
    __agencSessionId: sessionId,
  };
  attachToolRuntimeContext(args, {
    callId: `call-${sessionId}`,
    toolName: "exec_command",
    runtimeKind: "function",
    classification: "exclusive",
    supportsParallelToolCalls: false,
    source: { type: "model" },
    submittedAtMs: 0,
    approvalPolicy: "never",
    requestedSandboxMode: sandboxMode,
    sandboxMode,
    approvalResolved: true,
    rawArgs: "{}",
    invocation: {
      session: {
        permissionModeRegistry: {
          current: () => ({
            mode: sandboxMode === "danger_full_access" ? "bypassPermissions" : "default",
            additionalWorkingDirectories: new Map(),
          }),
        },
        services: { runtimeOptions: { sessionTempRoot: "/tmp/workspace" } },
      },
      payload: { kind: "function", arguments: "{}" },
      turn: { subId: "turn-context", cwd: "/tmp/workspace" },
    },
  } as never);
  return args;
}

test("a nested agent stays on the shell write gate while its parent is in fast mode", async () => {
  const deferred = vi.spyOn(shellPermission, "deferredShellWorkspaceMutationPermission");
  const eager = vi.spyOn(shellPermission, "shellWorkspaceMutationPermission");
  const { tool, execCommand } = managerTool();
  const outside = "rm /srv/agenc-outside-workspace/outside.txt";

  await withOneShotFastMode("parent", async () => {
    const parent = await tool.execute(commandArgs("parent", outside, "danger_full_access"));
    expect(parent.isError).toBeUndefined();
    expect(deferred).toHaveBeenCalled();
    expect(eager).not.toHaveBeenCalled();
    expect(execCommand).toHaveBeenCalledTimes(1);

    deferred.mockClear();
    eager.mockClear();
    execCommand.mockClear();

    const child = await tool.execute(commandArgs("child", outside, "workspace_write"));
    expect(child.isError).toBe(true);
    expect(String(child.content)).toContain("shell_workspace_file_delete_disallowed");
    expect(eager).toHaveBeenCalled();
    expect(deferred).not.toHaveBeenCalled();
    expect(execCommand).not.toHaveBeenCalled();
  });
});
