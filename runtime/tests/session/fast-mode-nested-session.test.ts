import { expect, test } from "vitest";
import { AdmissionDeniedError } from "../../src/budget/admission-client.js";
import { runAdmittedToolCall } from "../../src/budget/admitted-tool-call.js";
import {
  oneShotFastModeActive,
  oneShotFastModeAppliesTo,
  withOneShotFastMode,
} from "../../src/one-shot-fast-mode.js";
import type { Session } from "../../src/session/session.js";

function session(conversationId: string): Session {
  return {
    conversationId,
    services: {},
  } as unknown as Session;
}

const tool = {
  name: "fixture",
  recoveryCategory: "idempotent" as const,
};

test("a matching session keeps the fast tool path and an unmatched nested session does not", async () => {
  const parent = session("parent");
  const child = session("child");
  const result = { content: "parent-ok", isError: false };
  const parentResult = await withOneShotFastMode("parent", () => runAdmittedToolCall({
    session: parent,
    turnId: "turn",
    callId: "parent-call",
    args: {},
    tool,
    invoke: async () => result,
  }));
  expect(parentResult).toBe(result);
  expect(oneShotFastModeActive()).toBe(false);

  await expect(withOneShotFastMode("parent", () => runAdmittedToolCall({
    session: child,
    turnId: "turn",
    callId: "child-call",
    args: {},
    tool,
    invoke: async () => ({ content: "child-ok", isError: false }),
  }))).rejects.toThrow(AdmissionDeniedError);
  await expect(withOneShotFastMode("parent", () => runAdmittedToolCall({
    session: child,
    turnId: "turn",
    callId: "child-call",
    args: {},
    tool,
    invoke: async () => ({ content: "child-ok", isError: false }),
  }))).rejects.toThrow("admission_kernel_unavailable");
});

test("a hard call cap stays on atomic admission for the fast session and the nested session", async () => {
  const capped = (conversationId: string): Session => ({
    conversationId,
    services: {
      executionAdmission: {
        scope: { maxModelCalls: 1 },
        acquire: () => {
          throw new AdmissionDeniedError("model_call_budget_exceeded");
        },
      },
    },
  } as unknown as Session);
  const invoke = async () => ({ content: "ran", isError: false });
  await expect(withOneShotFastMode("parent", () => runAdmittedToolCall({
    session: capped("parent"), turnId: "turn", callId: "parent-cap", args: {}, tool, invoke,
  }))).rejects.toThrow("model_call_budget_exceeded");
  await expect(withOneShotFastMode("parent", () => runAdmittedToolCall({
    session: capped("child"), turnId: "turn", callId: "child-cap", args: {}, tool, invoke,
  }))).rejects.toThrow("model_call_budget_exceeded");
});

test("an omitted fast scope stays active for callers and does not apply to a session", () => {
  expect(withOneShotFastMode(() => {
    expect(oneShotFastModeActive()).toBe(true);
    expect(oneShotFastModeAppliesTo("parent")).toBe(false);
    return "cached";
  })).toBe("cached");
  expect(withOneShotFastMode("parent", () => oneShotFastModeAppliesTo("parent"))).toBe(true);
  expect(withOneShotFastMode("parent", () => oneShotFastModeAppliesTo("child"))).toBe(false);
});
