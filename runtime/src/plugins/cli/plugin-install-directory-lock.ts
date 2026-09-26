import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, lstat, mkdir, open, rename, unlink } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import { isRecord } from "../../utils/record.js";
import { nearestExistingRealpath } from "../nearest-existing-realpath.js";

const PLUGIN_INSTALL_OPS_DIR = ".plugin-install-ops";
const DIRECTORY_LOCK_POLL_MS = 20;
const MAX_LOCK_BYTES = 4096;
const RECLAIM_GUARD_SUFFIX = ".reclaim";
const DEFAULT_RECLAIM_GUARD_STALE_MS = 60_000;
const MAX_RECLAIM_KEY_DEPTH = 4;
const LOWERCASE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

// This lock does not provide unconditional mutual exclusion.
//
// Publish writes a finished owner record into a unique temp file and link()s
// it onto the lock path. The canonical path is therefore never a live
// holder's half-written file. Empty, partial, symlink, directory, FIFO, and
// other non-owner entries are not deleted: after the reclaim guard confirms
// the same entry is still there, acquisition throws and leaves it for manual
// recovery. An unexpected directory is never removed.
//
// Release unlinks only after two reads of this process's owner bytes, and
// drops the nonce only after a later read shows those bytes are gone. A
// failed unlink keeps the nonce, and release() can be retried. Overlapping
// release attempts run one at a time. acquiredAtMs is not a lock timeout;
// age never expires a live owner. A read error is not proof that an owner
// is dead.
//
// Removal of a dead file. A dead guard (or dead key file) whose owner nonce
// is N is removed only by the one process that link-publishes
// `<lock>.reclaim-N`, so no two reclaimers remove the same guard. The dead
// lock itself is removed only by the holder of `<lock>.reclaim`. Removal
// renames the entry to a unique claim name, checks through the open file that
// the claimed bytes and dev/ino are the ones judged dead, and only then unlinks
// the claim. Anything else is linked back to its path.
//
// Lock key. The key is the entry, not what it points at: the realpath of the
// nearest existing ancestor of the parent, plus the final name after NFC and
// lowercase folding. A symlinked final component is not followed, so
// replacing it with a directory keeps the same key. Names that differ only by
// case or Unicode normalization share one lock on every platform.
//
// Residuals:
// - Young dead guard. try-lock reports the directory busy, so recovery skips
//   it, and a blocking acquire polls until the guard is 60s old.
// - A claimed entry that is not the one judged dead is linked back. If another
//   file was linked at that path in between, the claim is left in place and
//   acquisition reports it for manual recovery.
// - Foreign pid reuse. A dead owner's pid can belong to an unrelated live
//   process. The lock or guard looks live until that process exits.
// - Other pid namespace. kill(pid, 0) only sees this namespace. A holder in
//   another namespace can look dead, and EPERM is treated as live.
// - Same-async-context re-entry. A nested call on the same key runs the body
//   without taking another lock.
// - Path key. Two different entries that reach one directory (a symlink and
//   its target, or a bind mount) are two keys. Each key still serializes the
//   operations that replace its own entry.
// - A non-participant can unlink a lock, or replace a tree, between a check
//   and a removal.
// - link fails closed on filesystems without hard links (FAT, exFAT, some
//   SMB). Windows was not run.
const heldDirectoryLockNonces = new Set<string>();
const heldGuardNonces = new Set<string>();
const lockContext = new AsyncLocalStorage<ReadonlySet<string>>();

let directoryLockWaitHook: (() => void) | undefined;
let reclaimGuardStaleMs = DEFAULT_RECLAIM_GUARD_STALE_MS;

export class PluginInstallDirectoryLockCorruptError extends Error {
  readonly path: string;

  constructor(path: string) {
    super(`plugin install directory lock requires manual recovery: ${path}`);
    this.name = "PluginInstallDirectoryLockCorruptError";
    this.path = path;
  }
}

/** Body already finished. The lock file may still be held by this process. */
export class PluginInstallDirectoryLockReleaseError extends Error {
  readonly operationCompleted: true;

  constructor(cause: unknown) {
    super(
      `plugin install directory lock cleanup failed after the locked operation completed: ${errorMessage(cause)}`,
      { cause },
    );
    this.name = "PluginInstallDirectoryLockReleaseError";
    this.operationCompleted = true;
  }
}

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
type GuardInspection = "absent" | "busy" | "reclaimable" | "corrupt";
type LockStat = Awaited<ReturnType<typeof lstat>>;
type LockRead =
  | { readonly kind: "absent" }
  | { readonly kind: "unreadable" }
  | { readonly kind: "bytes"; readonly text: string } & EntryIdentity;

interface EntryIdentity {
  readonly dev: number | bigint;
  readonly ino: number | bigint;
  readonly mtimeMs: number;
}

/** A dead guard or key file as it was judged dead, read through one open fd. */
interface DeadEntry extends EntryIdentity {
  readonly text: string;
  readonly nonce: string;
}

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
  return withKeyLock(await pluginInstallDirectoryLockKey(destination), body);
}

/** Holds every destination's lock, taken in key order so two callers cannot deadlock. */
export async function withPluginInstallDirectoryLocks<T>(
  destinations: readonly string[],
  body: () => Promise<T>,
): Promise<T> {
  const keys = await Promise.all(destinations.map((destination) => pluginInstallDirectoryLockKey(destination)));
  // Code-unit order, so every process takes the same keys in the same order.
  const ordered = [...new Set(keys)].toSorted((a, b) => Number(a > b) - Number(a < b));
  const run = ordered.reduceRight<() => Promise<T>>((inner, key) => () => withKeyLock(key, inner), body);
  return run();
}

async function withKeyLock<T>(key: string, body: () => Promise<T>): Promise<T> {
  const current = lockContext.getStore();
  if (current?.has(key) === true) return body();
  const hold = await acquire(key, true);
  const next = new Set(current);
  next.add(key);
  let bodyResult!: T;
  let bodyError: unknown;
  let bodyOk = false;
  try {
    bodyResult = await lockContext.run(next, body);
    bodyOk = true;
  } catch (error) {
    bodyError = error;
  }
  try {
    await hold.release();
  } catch (cleanupError) {
    if (!bodyOk) throw bodyError;
    throw new PluginInstallDirectoryLockReleaseError(cleanupError);
  }
  if (!bodyOk) throw bodyError;
  return bodyResult;
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
  const info = await lstatIfPresent(lockPath);
  if (info === undefined) return "retry";
  if (info.isSymbolicLink() || !info.isFile()) {
    return rejectUnownedEntry(lockPath, info, undefined);
  }
  const read = await readLockBytes(lockPath);
  switch (read.kind) {
    case "absent":
      return "retry";
    case "unreadable":
      return rejectUnownedEntry(lockPath, info, undefined);
    case "bytes": {
      const parsed = parseOwner(read.text);
      if (parsed === undefined) return rejectUnownedEntry(lockPath, info, read.text);
      if (holderIsLive(parsed, heldDirectoryLockNonces)) return "busy";
      const outcome = await reclaimStaleLock(lockPath, read.text);
      return outcome === "removed" ? "retry" : "wait";
    }
    default: {
      const exhaustive: never = read;
      throw new Error(`unhandled lock read: ${String(exhaustive)}`);
    }
  }
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
    let tail: Promise<void> = Promise.resolve();
    return {
      async release(): Promise<void> {
        const attempt = tail.then(async () => {
          if (released) return;
          await unlinkOwned(path, body, beforeUnlink);
          const after = await readLockBytes(path);
          switch (after.kind) {
            case "absent":
              break;
            case "unreadable":
              throw new Error(`plugin install directory lock release could not confirm removal: ${path}`);
            case "bytes":
              if (after.text === body) {
                throw new Error(`plugin install directory lock still present after release: ${path}`);
              }
              break;
            default: {
              const exhaustive: never = after;
              throw new Error(`unhandled lock read: ${String(exhaustive)}`);
            }
          }
          nonces.delete(nonce);
          released = true;
        });
        tail = attempt.then(() => undefined, () => undefined);
        await attempt;
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
  const first = await readLockBytes(path);
  if (first.kind !== "bytes" || first.text !== body) return;
  await beforeUnlink?.();
  const second = await readLockBytes(path);
  if (second.kind !== "bytes" || second.text !== body) return;
  await unlink(path).catch(ignoreMissing);
}

async function reclaimStaleLock(lockPath: string, seenText: string): Promise<ReclaimOutcome> {
  return withReclaimGuard(lockPath, seenText, () => removeStaleLockIfUnchanged(lockPath, seenText));
}

async function rejectUnownedEntry(
  lockPath: string,
  seen: LockStat,
  seenText: string | undefined,
): Promise<ExistingLockAction> {
  const outcome = await withReclaimGuard(lockPath, seenText ?? "", () =>
    confirmUnownedUnchanged(lockPath, seen, seenText));
  switch (outcome) {
    case "removed":
      return "retry";
    case "changed":
    case "busy":
      return "wait";
    default: {
      const exhaustive: never = outcome;
      throw new Error(`unhandled install directory lock reclaim outcome: ${String(exhaustive)}`);
    }
  }
}

async function withReclaimGuard(
  lockPath: string,
  seenText: string,
  remove: () => Promise<ReclaimOutcome>,
): Promise<ReclaimOutcome> {
  await reclaimHook?.({ phase: "stale-observed", lockDir: lockPath, seenText });
  const guard = await acquireReclaimGuard(lockPath);
  if (guard === "busy") {
    await reclaimHook?.({ phase: "reclaim-settled", lockDir: lockPath, seenText });
    return "busy";
  }
  let outcome: ReclaimOutcome;
  try {
    outcome = await remove();
  } finally {
    await guard.release();
  }
  await reclaimHook?.({ phase: "reclaim-settled", lockDir: lockPath, seenText });
  return outcome;
}

async function confirmUnownedUnchanged(
  lockPath: string,
  seen: LockStat,
  seenText: string | undefined,
): Promise<ReclaimOutcome> {
  for (let check = 0; check < 2; check += 1) {
    const current = await lstatIfPresent(lockPath);
    if (current === undefined) return "removed";
    if (!sameIdentity(current, seen) || !sameEntryKind(current, seen)) return "changed";
    if (current.isSymbolicLink() || !current.isFile()) continue;
    const read = await readLockBytes(lockPath);
    switch (read.kind) {
      case "absent":
        return "removed";
      case "unreadable":
        if (seenText !== undefined) return "changed";
        break;
      case "bytes":
        if (read.text !== seenText) return "changed";
        break;
      default: {
        const exhaustive: never = read;
        throw new Error(`unhandled lock read: ${String(exhaustive)}`);
      }
    }
  }
  throw new PluginInstallDirectoryLockCorruptError(lockPath);
}

async function removeStaleLockIfUnchanged(lockPath: string, seenText: string): Promise<ReclaimOutcome> {
  let identity: EntryIdentity | undefined;
  for (let check = 0; check < 2; check += 1) {
    const read = await readLockBytes(lockPath);
    if (read.kind === "absent") return "removed";
    if (read.kind === "unreadable" || read.text !== seenText) return "changed";
    if (identity !== undefined && !sameFile(read, identity)) return "changed";
    if (holderIsLive(parseOwner(read.text), heldDirectoryLockNonces)) return "changed";
    identity = read;
  }
  if (identity === undefined) return "changed";
  return await claimAndRemove(lockPath, { ...identity, text: seenText }) ? "removed" : "changed";
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
        if (!await removeStaleGuard(lockPath, guardPath)) return "busy";
        continue;
      case "busy":
        return "busy";
      case "corrupt":
        throw new PluginInstallDirectoryLockCorruptError(guardPath);
      default: {
        const exhaustive: never = state;
        throw new Error(`unhandled install directory lock guard state: ${String(exhaustive)}`);
      }
    }
  }
  return "busy";
}

async function inspectGuard(guardPath: string): Promise<GuardInspection> {
  const info = await lstatIfPresent(guardPath);
  if (info === undefined) return "absent";
  if (info.isSymbolicLink() || !info.isFile()) return "corrupt";
  const read = await readLockBytes(guardPath);
  switch (read.kind) {
    case "absent":
      return "absent";
    case "unreadable":
      return "corrupt";
    case "bytes":
      return guardRecordState(read.text, read.mtimeMs);
    default: {
      const exhaustive: never = read;
      throw new Error(`unhandled lock read: ${String(exhaustive)}`);
    }
  }
}

async function removeStaleGuard(lockPath: string, guardPath: string): Promise<boolean> {
  const seen = await readDeadEntry(guardPath);
  return seen !== undefined && removeDeadEntry(lockPath, guardPath, seen, 0);
}

/**
 * Removes `path` only while holding `<lock>.reclaim-<nonce of the dead entry>`.
 * A dead holder of that key file is removed the same way, one level deeper.
 */
async function removeDeadEntry(lockPath: string, path: string, seen: DeadEntry, depth: number): Promise<boolean> {
  const keyPath = `${lockPath}${RECLAIM_GUARD_SUFFIX}-${seen.nonce}`;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const key = await holdLinkedFile(keyPath, heldGuardNonces);
    if (key !== undefined) return removeUnderKey(key, path, seen);
    const blocker = await inspectGuard(keyPath);
    if (blocker === "corrupt") throw new PluginInstallDirectoryLockCorruptError(keyPath);
    if (blocker === "busy") return false;
    if (blocker === "reclaimable" && !await removeDeadKey(lockPath, keyPath, depth)) return false;
  }
  return false;
}

async function removeDeadKey(lockPath: string, keyPath: string, depth: number): Promise<boolean> {
  if (depth >= MAX_RECLAIM_KEY_DEPTH) return false;
  const seen = await readDeadEntry(keyPath);
  return seen !== undefined && removeDeadEntry(lockPath, keyPath, seen, depth + 1);
}

async function removeUnderKey(key: PluginInstallDirectoryLock, path: string, seen: DeadEntry): Promise<boolean> {
  let removed = false;
  try {
    const read = await readLockBytes(path);
    if (read.kind === "absent") removed = true;
    else if (read.kind === "bytes" && read.text === seen.text && sameFile(read, seen)) {
      removed = await claimAndRemove(path, seen);
    }
  } finally {
    await key.release();
  }
  return removed;
}

/**
 * Renames `path` to a name only this call knows, then compares the claimed
 * file's bytes and dev/ino, read through one fd, with `expected`. A match is
 * unlinked. Anything else is linked back and reported as not removed.
 */
async function claimAndRemove(
  path: string,
  expected: { readonly text: string } & Pick<EntryIdentity, "dev" | "ino">,
): Promise<boolean> {
  const claim = `${path}.claim-${randomUUID()}`;
  try {
    await rename(path, claim);
  } catch (error) {
    if (codeOf(error) === "ENOENT") return true;
    throw error;
  }
  const got = await readLockBytes(claim);
  if (got.kind === "bytes" && got.text === expected.text && sameFile(got, expected)) {
    await unlink(claim).catch(ignoreMissing);
    return true;
  }
  try {
    await link(claim, path);
  } catch (error) {
    if (codeOf(error) === "EEXIST") throw new PluginInstallDirectoryLockCorruptError(claim);
    throw error;
  }
  await unlink(claim).catch(ignoreMissing);
  return false;
}

async function readDeadEntry(path: string): Promise<DeadEntry | undefined> {
  const read = await readLockBytes(path);
  if (read.kind !== "bytes" || guardRecordState(read.text, read.mtimeMs) !== "reclaimable") return undefined;
  const nonce = parseOwner(read.text)?.nonce;
  return nonce === undefined ? undefined : { ...read, nonce };
}

function sameFile(left: Pick<EntryIdentity, "dev" | "ino">, right: Pick<EntryIdentity, "dev" | "ino">): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

function guardRecordState(text: string, mtimeMs: number): GuardInspection {
  const parsed = parseOwner(text);
  if (parsed === undefined) return "corrupt";
  if (holderIsLive(parsed, heldGuardNonces)) return "busy";
  // A dead guard's nonce names the key file that guards its removal.
  if (parsed.nonce === undefined || !LOWERCASE_UUID.test(parsed.nonce)) return "corrupt";
  const recordedAt = parsed.acquiredAtMs ?? mtimeMs;
  return Date.now() - recordedAt >= reclaimGuardStaleMs ? "reclaimable" : "busy";
}

async function emitPublish(event: PluginInstallDirectoryLockPublishEvent): Promise<void> {
  await publishHook?.(event);
}

function epochMs(mtimeMs: number | bigint): number {
  return typeof mtimeMs === "bigint" ? Number(mtimeMs) : mtimeMs;
}

function ownerText(pid: number, nonce: string, acquiredAtMs: number): string {
  return `${JSON.stringify({ pid, nonce, acquiredAtMs })}\n`;
}

function tempPath(target: string, nonce: string): string {
  return `${target}.tmp-${process.pid}-${nonce}`;
}

async function readLockBytes(path: string): Promise<LockRead> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    // win32 has no O_NOFOLLOW. O_NONBLOCK is absent there too; where it exists
    // it keeps a FIFO swapped in after lstat from blocking in open.
    const follow = constants.O_NOFOLLOW ?? 0;
    const nonblock = constants.O_NONBLOCK ?? 0;
    handle = await open(path, constants.O_RDONLY | follow | nonblock);
    const info = await handle.stat();
    if (!info.isFile() || info.size > MAX_LOCK_BYTES) return { kind: "unreadable" };
    const buffer = Buffer.alloc(info.size);
    await handle.read(buffer, 0, info.size, 0);
    return {
      kind: "bytes",
      text: buffer.toString("utf8"),
      dev: info.dev,
      ino: info.ino,
      mtimeMs: epochMs(info.mtimeMs),
    };
  } catch (error) {
    const code = codeOf(error);
    if (code === "ENOENT") return { kind: "absent" };
    if (
      code === "ELOOP"
      || code === "EISDIR"
      || code === "ENOTDIR"
      || code === "ENXIO"
      || code === "EAGAIN"
    ) return { kind: "unreadable" };
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

function sameIdentity(current: LockStat, seen: LockStat): boolean {
  return current.dev === seen.dev && current.ino === seen.ino;
}

function sameEntryKind(current: LockStat, seen: LockStat): boolean {
  return current.isSymbolicLink() === seen.isSymbolicLink()
    && current.isFile() === seen.isFile()
    && current.isDirectory() === seen.isDirectory()
    && current.isFIFO() === seen.isFIFO()
    && current.isSocket() === seen.isSocket()
    && current.isBlockDevice() === seen.isBlockDevice()
    && current.isCharacterDevice() === seen.isCharacterDevice();
}

async function lstatIfPresent(path: string): Promise<LockStat | undefined> {
  try {
    return await lstat(path);
  } catch (error) {
    if (codeOf(error) === "ENOENT") return undefined;
    throw error;
  }
}

async function pluginInstallDirectoryLockKey(destination: string): Promise<string> {
  const resolved = resolve(destination);
  const parent = await nearestExistingRealpath(dirname(resolved)) ?? dirname(resolved);
  return join(parent, basename(resolved).normalize("NFC").toLowerCase());
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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
