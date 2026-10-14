import { AsyncLocalStorage } from "node:async_hooks";
import type { AgentRuntimeOptions } from "./session/runtime-options.js";
import type { Session } from "./session/session.js";
import type { TurnContext } from "./session/turn-context.js";

interface OneShotFastScope {
  readonly sessionId?: string;
}

const active = new AsyncLocalStorage<OneShotFastScope>();
/** Never inferred from environment variables or ordinary permission bypass. */
export function oneShotFastModeSelected(
  options: Partial<AgentRuntimeOptions> | undefined,
  config: { readonly bypassFastMode?: boolean },
): boolean {
  return options?.dangerouslyBypassApprovalsAndSandbox === true &&
    options.nonInteractive === true && options.relaxedOneShot === true && config.bypassFastMode !== false;
}
export function bypassFastModeEnabled(session: Session, ctx: TurnContext): boolean {
  return oneShotFastModeSelected(session.services.runtimeOptions, ctx.config) &&
    ctx.permissionMode === "bypassPermissions" && ctx.sandboxPolicy.value === "danger_full_access" &&
    session.services.configStore?.current().bypassFastMode !== false;
}
export function requiresAtomicSpendAdmission(session: Session): boolean {
  const scope = session.services.executionAdmission?.scope;
  return scope?.hasHardCostCap === true || scope?.hasHardTokenCap === true ||
    scope?.maxCostUsd !== undefined || scope?.maxTokens !== undefined ||
    scope?.maxModelCalls !== undefined;
}
export function oneShotFastModeActive(): boolean {
  return active.getStore() !== undefined;
}
export function oneShotFastModeAppliesTo(sessionId: string): boolean {
  const stored = active.getStore()?.sessionId;
  if (stored === undefined) return false;
  const requested = sessionId.trim();
  return requested.length > 0 && stored === requested;
}
export function withOneShotFastMode<T>(run: () => T): T;
export function withOneShotFastMode<T>(sessionId: string, run: () => T): T;
export function withOneShotFastMode<T>(
  sessionIdOrRun: string | (() => T),
  run?: () => T,
): T {
  if (typeof sessionIdOrRun === "function") {
    return active.run({}, sessionIdOrRun);
  }
  const sessionId = sessionIdOrRun.trim();
  return active.run(sessionId.length > 0 ? { sessionId } : {}, run as () => T);
}
