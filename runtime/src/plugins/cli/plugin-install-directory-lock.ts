import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, lstat, mkdir, open, realpath, unlink } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import { isRecord } from "../../utils/record.js";

const PLUGIN_INSTALL_OPS_DIR = ".plugin-install-ops";
const DIRECTORY_LOCK_POLL_MS = 20;
const MAX_LOCK_BYTES = 4096;
const RECLAIM_GUARD_SUFFIX = ".reclaim";
const DEFAULT_RECLAIM_GUARD_STALE_MS = 60_000;

// Residual windows:
// - Dead guard older than 60s. Takeover re-reads that file and unlinks the
//   path. A guard replaced between the read and the unlink is what gets
//   removed, and two such reclaimers can both unlink a lock.
// - Young dead guard. A dead guard younger than 60s stays. try-lock reports
//   the directory busy, so recovery skips it, and a blocking acquire polls
//   until the bound passes.
// - Foreign pid reuse. A dead owner's pid can belong to an unrelated live
//   process. The lock or guard looks live until that process exits.
// - Other pid namespace. kill(pid, 0) only sees this namespace. A holder in
//   another namespace can look dead, and EPERM is treated as live.
//
// Publish writes the owner into a unique temp file and links it onto the lock
// path, so an existing path is left alone. Release checks the lock bytes,
// unlinks that file, and only then drops the nonce. acquiredAtMs is not a
// lock timeout: a long install must not look expired.
const heldDirectoryLockNonces = new Set<string>();
const heldGuardNonces = new Set<string>();
const lockContext = new AsyncLocalStorage<ReadonlySet<string>>();

let directoryLockWaitHook: (() => void) | undefined;
let reclaimGuardStaleMs = DEFAULT_RECLAIM_GUARD_STALE_MS;

export type PluginInstallDirectoryLockReclaimPhase = "stale-observed" | "reclaim-settled";

export interface PluginInstallDirectoryLockReclaimEvent {
  readonly phase: PluginInstallDirectoryLockReclaimPhase;
  readonly lockDir: string;
  readonly seenText: string;
}

export type PluginInstallDirectoryLockPublishPhase = "staging-ready" | "before-release-remove";

export interface PluginInstallDirectoryLockPublishEvent {
  readonly phase: PluginInstallDirectoryLockPublishPhase;
  readonly lockDir: string;
}

let reclaimHook: ((event: PluginInstallDirectoryLockReclaimEvent) => Promise<void>) | undefined;
let publishHook: ((event: PluginInstallDirectoryLockPublishEvent) => Promise<void>) | undefined;

export interface PluginInstallDirectoryLock {
  release(): Promise<void>;
}

interface OwnerRecord {
  readonly pid: number;
  readonly nonce?: string;
  readonly acquiredAtMs?: number;
}

type ExistingLockAction = "retry" | "busy" | "wait";
type ReclaimOutcome = "removed" | "busy" | "changed";
type GuardInspection = "absent" | "busy" | "reclaimable";

/** Fires when a blocking acquire sees a live holder and is about to wait. */
export function setPluginInstallDirectoryLockWaitHook(hook: (() => void) | undefined): void {
  directoryLockWaitHook = hook;
}

/** Test seam. Production leaves this unset. */
export function setPluginInstallDirectoryLockReclaimHook(
  hook: ((event: PluginInstallDirectoryLockReclaimEvent) => Promise<void>) | undefined,
): void {
  reclaimHook = hook;
}

/**
 * Test seam. `staging-ready` runs after the temp file is durable and before
 * link. `before-release-remove` runs while this process still holds the nonce
 * and the lock bytes are still ours, before unlink.
 */
export function setPluginInstallDirectoryLockPublishHook(
  hook: ((event: PluginInstallDirectoryLockPublishEvent) => Promise<void>) | undefined,
): void {
  publishHook = hook;
}

/** Test seam for the crashed-guard age bound. `undefined` restores 60s. */
export function setPluginInstallDirectoryLockGuardStaleMs(ms: number | undefined): void {
  if (ms !== undefined && (!Number.isFinite(ms) || ms < 0)) {
    throw new Error("plugin install directory lock guard bound must be a non-negative finite number");
  }
  reclaimGuardStaleMs = ms ?? DEFAULT_RECLAIM_GUARD_STALE_MS;
}

export async function pluginInstallDirectoryLockDirectory(destination: string): Promise<string> {
  return lockFilePath(await pluginInstallDirectoryLockKey(destination));
}

export async function withPluginInstallDirectoryLock<T>(
  destination: string,
  body: () => Promise<T>,
): Promise<T> {
  const key = await pluginInstallDirectoryLockKey(destination);
  const current = lockContext.getStore();
  if (current?.has(key) === true) return body();
  const hold = await acquire(key, true);
  const next = new Set(current);
  next.add(key);
  try {
    return await lockContext.run(next, body);
  } finally {
    await hold.release();
  }
}

/** One attempt. A live holder, including this process, is busy: callers skip. */
export async function tryPluginInstallDirectoryLock(
  destination: string,
): Promise<PluginInstallDirectoryLock | undefined> {
  return acquire(await pluginInstallDirectoryLockKey(destination), false);
}

async function acquire(key: string, block: true): Promise<PluginInstallDirectoryLock>;
async function acquire(key: string, block: false): Promise<PluginInstallDirectoryLock | undefined>;
async function acquire(key: string, block: boolean): Promise<PluginInstallDirectoryLock | undefined> {
  for (let attempt = 0; block || attempt < 3; attempt += 1) {
    const lockPath = await prepareLockFile(key);
    const created = await holdLinkedFile(lockPath, heldDirectoryLockNonces, async () => {
      await emitPublish({ phase: "staging-ready", lockDir: lockPath });
    }, async () => {
      await emitPublish({ phase: "before-release-remove", lockDir: lockPath });
    });
    if (created !== undefined) return created;
    const action = await classifyExisting(lockPath);
    switch (action) {
      case "retry":
        continue;
      case "busy":
      case "wait":
        if (!block) return undefined;
        directoryLockWaitHook?.();
        await delay(DIRECTORY_LOCK_POLL_MS);
        continue;
      default: {
        const exhaustive: never = action;
        throw new Error(`unhandled install directory lock action: ${String(exhaustive)}`);
      }
    }
  }
  return undefined;
}

async function classifyExisting(lockPath: string): Promise<ExistingLockAction> {
  let info: Awaited<ReturnType<typeof lstat>>;
  try {
    info = await lstat(lockPath);
  } catch (error) {
    if (codeOf(error) === "ENOENT") return "retry";
    throw error;
  }
  if (info.isSymbolicLink()) {
    await unlink(lockPath).catch((error: unknown) => {
      if (codeOf(error) !== "ENOENT") throw error;
    });
    return "retry";
  }
  if (!info.isFile()) return "wait";
  const text = await readLockText(lockPath);
  const parsed = parseOwner(text);
  if (text === undefined || parsed === undefined) return "wait";
  if (holderIsLive(parsed, heldDirectoryLockNonces)) return "busy";
  const outcome = await reclaimStaleLock(lockPath, text);
  return outcome === "removed" ? "retry" : "wait";
}

async function holdLinkedFile(
  path: string,
  nonces: Set<string>,
  beforeLink?: () => Promise<void>,
  beforeUnlink?: () => Promise<void>,
): Promise<PluginInstallDirectoryLock | undefined> {
  const nonce = randomUUID();
  const body = ownerText(process.pid, nonce, Date.now());
  nonces.add(nonce);
  let published = false;
  try {
    if (!await linkNewFile(path, body, tempPath(path, nonce), beforeLink)) return undefined;
    published = true;
    let released = false;
    return {
      async release(): Promise<void> {
        if (released) return;
        released = true;
        try {
          await unlinkOwned(path, body, beforeUnlink);
        } finally {
          if (await readLockText(path) !== body) nonces.delete(nonce);
        }
      },
    };
  } finally {
    if (!published) nonces.delete(nonce);
  }
}

async function linkNewFile(
  target: string,
  body: string,
  temp: string,
  beforeLink?: () => Promise<void>,
): Promise<boolean> {
  let created = false;
  try {
    const handle = await open(temp, "wx", 0o600);
    created = true;
    try {
      await handle.writeFile(body);
      await handle.sync().catch((error: unknown) => {
        const code = codeOf(error);
        if (code !== "EINVAL" && code !== "ENOTSUP" && code !== "EISDIR" && code !== "EBADF") throw error;
      });
    } finally {
      await handle.close();
    }
    await beforeLink?.();
    await link(temp, target);
    return true;
  } catch (error) {
    if (codeOf(error) === "EEXIST" || codeOf(error) === "ENOENT") return false;
    throw error;
  } finally {
    if (created) await unlink(temp).catch(ignoreMissing);
  }
}

async function unlinkOwned(path: string, body: string, beforeUnlink?: () => Promise<void>): Promise<void> {
  if (await readLockText(path) !== body) return;
  await beforeUnlink?.();
  if (await readLockText(path) !== body) return;
  await unlink(path).catch(ignoreMissing);
}

async function reclaimStaleLock(lockPath: string, seenText: string): Promise<ReclaimOutcome> {
  await reclaimHook?.({ phase: "stale-observed", lockDir: lockPath, seenText });
  const guard = await acquireReclaimGuard(lockPath);
  let outcome: ReclaimOutcome = "busy";
  if (guard !== "busy") {
    try {
      outcome = await removeStaleLockIfUnchanged(lockPath, seenText);
    } finally {
      await guard.release();
    }
  }
  await reclaimHook?.({ phase: "reclaim-settled", lockDir: lockPath, seenText });
  return outcome;
}

async function removeStaleLockIfUnchanged(lockPath: string, seenText: string): Promise<ReclaimOutcome> {
  for (let check = 0; check < 2; check += 1) {
    const text = await readLockText(lockPath);
    if (text === undefined) return "removed";
    if (text !== seenText || holderIsLive(parseOwner(text), heldDirectoryLockNonces)) return "changed";
  }
  await unlink(lockPath).catch((error: unknown) => {
    if (codeOf(error) !== "ENOENT") throw error;
  });
  return "removed";
}

async function acquireReclaimGuard(lockPath: string): Promise<PluginInstallDirectoryLock | "busy"> {
  const guardPath = `${lockPath}${RECLAIM_GUARD_SUFFIX}`;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const created = await holdLinkedFile(guardPath, heldGuardNonces);
    if (created !== undefined) return created;
    const state = await inspectGuard(guardPath);
    switch (state) {
      case "absent":
        continue;
      case "reclaimable":
        if (!await removeStaleGuard(guardPath)) return "busy";
        continue;
      case "busy":
        return "busy";
      default: {
        const exhaustive: never = state;
        throw new Error(`unhandled install directory lock guard state: ${String(exhaustive)}`);
      }
    }
  }
  return "busy";
}

async function inspectGuard(guardPath: string): Promise<GuardInspection> {
  let info: Awaited<ReturnType<typeof lstat>>;
  try {
    info = await lstat(guardPath);
  } catch (error) {
    if (codeOf(error) === "ENOENT") return "absent";
    throw error;
  }
  if (info.isSymbolicLink() || !info.isFile()) return "busy";
  const text = await readLockText(guardPath);
  if (text === undefined) return "busy";
  return guardRecordIsReclaimable(text, info.mtimeMs) ? "reclaimable" : "busy";
}

async function removeStaleGuard(guardPath: string): Promise<boolean> {
  const seen = await readLockText(guardPath);
  const info = await lstat(guardPath).catch((error: unknown) => {
    if (codeOf(error) === "ENOENT") return undefined;
    throw error;
  });
  if (seen === undefined || info === undefined || !guardRecordIsReclaimable(seen, info.mtimeMs)) return false;
  const again = await readLockText(guardPath);
  const againInfo = await lstat(guardPath).catch((error: unknown) => {
    if (codeOf(error) === "ENOENT") return undefined;
    throw error;
  });
  if (
    again !== seen
    || againInfo === undefined
    || !againInfo.isFile()
    || !guardRecordIsReclaimable(again, againInfo.mtimeMs)
  ) return false;
  try {
    await unlink(guardPath);
  } catch (error) {
    if (codeOf(error) !== "ENOENT") throw error;
  }
  return true;
}

function guardRecordIsReclaimable(text: string, mtimeMs: number): boolean {
  const parsed = parseOwner(text);
  if (parsed === undefined) return Date.now() - mtimeMs >= reclaimGuardStaleMs;
  if (holderIsLive(parsed, heldGuardNonces)) return false;
  return Date.now() - (parsed.acquiredAtMs ?? mtimeMs) >= reclaimGuardStaleMs;
}

async function emitPublish(event: PluginInstallDirectoryLockPublishEvent): Promise<void> {
  await publishHook?.(event);
}

function ownerText(pid: number, nonce: string, acquiredAtMs: number): string {
  return `${JSON.stringify({ pid, nonce, acquiredAtMs })}\n`;
}

function tempPath(target: string, nonce: string): string {
  return `${target}.tmp-${process.pid}-${nonce}`;
}

async function readLockText(path: string): Promise<string | undefined> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    const info = await handle.stat();
    if (!info.isFile() || info.size > MAX_LOCK_BYTES) return undefined;
    const buffer = Buffer.alloc(info.size);
    await handle.read(buffer, 0, info.size, 0);
    return buffer.toString("utf8");
  } catch (error) {
    const code = codeOf(error);
    if (code === "ENOENT" || code === "ELOOP" || code === "EISDIR" || code === "ENOTDIR") return undefined;
    throw error;
  } finally {
    await handle?.close();
  }
}

function parseOwner(text: string | undefined): OwnerRecord | undefined {
  if (text === undefined) return undefined;
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!isRecord(raw) || !Number.isInteger(raw.pid) || (raw.pid as number) < 1) return undefined;
  const nonce = typeof raw.nonce === "string" && raw.nonce !== "" ? raw.nonce : undefined;
  const acquiredAtMs = typeof raw.acquiredAtMs === "number" && Number.isFinite(raw.acquiredAtMs)
    ? raw.acquiredAtMs
    : undefined;
  return {
    pid: raw.pid as number,
    ...(nonce === undefined ? {} : { nonce }),
    ...(acquiredAtMs === undefined ? {} : { acquiredAtMs }),
  };
}

function holderIsLive(parsed: OwnerRecord | undefined, nonces: ReadonlySet<string>): boolean {
  if (parsed === undefined) return false;
  if (parsed.pid === process.pid) return parsed.nonce !== undefined && nonces.has(parsed.nonce);
  return pidIsLive(parsed.pid);
}

function pidIsLive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid < 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return codeOf(error) === "EPERM";
  }
}

async function pluginInstallDirectoryLockKey(destination: string): Promise<string> {
  const resolved = resolve(destination);
  try {
    return await realpath(resolved);
  } catch (error) {
    if (codeOf(error) !== "ENOENT") throw error;
    let parent = dirname(resolved);
    try {
      parent = await realpath(parent);
    } catch (parentError) {
      if (codeOf(parentError) !== "ENOENT") throw parentError;
    }
    return join(parent, basename(resolved));
  }
}

function lockFilePath(key: string): string {
  const digest = createHash("sha256").update(key, "utf8").digest("hex");
  return join(dirname(key), PLUGIN_INSTALL_OPS_DIR, `install-dir-${digest}.lock`);
}

async function prepareLockFile(key: string): Promise<string> {
  const opsDir = join(dirname(key), PLUGIN_INSTALL_OPS_DIR);
  await mkdir(opsDir, { recursive: true, mode: 0o700 });
  const info = await lstat(opsDir);
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new Error(`plugin install directory lock cannot use ${opsDir}`);
  }
  return lockFilePath(key);
}

function codeOf(error: unknown): string | undefined {
  return (error as NodeJS.ErrnoException).code;
}

function ignoreMissing(error: unknown): void {
  if (codeOf(error) !== "ENOENT") throw error;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => {
    setTimeout(resolveDelay, ms);
  });
}
