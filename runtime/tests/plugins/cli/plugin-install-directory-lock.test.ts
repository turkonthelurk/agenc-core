import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { lstat, mkdir, mkdtemp, readdir, readFile, rm, symlink, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { describe, expect, it, vi } from "vitest";

const unlinkInterleave = vi.hoisted(() => ({
  lockPath: undefined as string | undefined,
  run: undefined as undefined | (() => Promise<void>),
}));

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    unlink: async (path: Parameters<typeof actual.unlink>[0]) => {
      const run = unlinkInterleave.run;
      if (
        run !== undefined
        && unlinkInterleave.lockPath !== undefined
        && String(path) === unlinkInterleave.lockPath
      ) {
        unlinkInterleave.run = undefined;
        await run();
      }
      return actual.unlink(path);
    },
  };
});

import {
  pluginInstallDirectoryLockDirectory,
  setPluginInstallDirectoryLockGuardStaleMs,
  setPluginInstallDirectoryLockPublishHook,
  setPluginInstallDirectoryLockReclaimHook,
  setPluginInstallDirectoryLockWaitHook,
  tryPluginInstallDirectoryLock,
  withPluginInstallDirectoryLock,
  type PluginInstallDirectoryLockPublishEvent,
  type PluginInstallDirectoryLockReclaimEvent,
} from "../../../src/plugins/cli/plugin-install-directory-lock.js";

function exitedPid(): number {
  const child = spawnSync(process.execPath, ["-e", "process.exit(0)"], { stdio: "ignore" });
  if (typeof child.pid !== "number" || child.pid <= 1) {
    throw new Error("could not obtain an exited pid");
  }
  try {
    process.kill(child.pid, 0);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return child.pid;
    throw error;
  }
  throw new Error(`pid ${child.pid} is still live`);
}

async function plantDestination(): Promise<{
  readonly root: string;
  readonly destination: string;
  readonly lockPath: string;
}> {
  const root = await mkdtemp(join(tmpdir(), "install-dir-lock-"));
  const destination = join(root, "demo");
  await mkdir(destination, { recursive: true });
  const lockPath = await pluginInstallDirectoryLockDirectory(destination);
  return { root, destination, lockPath };
}

async function plantStaleLock(): Promise<{
  readonly root: string;
  readonly destination: string;
  readonly lockPath: string;
  readonly text: string;
}> {
  const world = await plantDestination();
  await mkdir(dirname(world.lockPath), { recursive: true, mode: 0o700 });
  const text = `${JSON.stringify({
    pid: exitedPid(),
    nonce: randomUUID(),
    acquiredAtMs: Date.now() - 10_000,
  })}\n`;
  await writeFile(world.lockPath, text, { mode: 0o600 });
  return { ...world, text };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => {
    setTimeout(resolveDelay, ms);
  });
}

describe("plugin install directory lock reclaim", () => {
  it("keeps a single holder when a stale read resumes after another reclaim enters", async () => {
    const world = await plantStaleLock();
    let inside = 0;
    let maxInside = 0;
    let releaseB: () => void = () => {};
    const gate = new Promise<void>((resolveGate) => {
      releaseB = resolveGate;
    });
    let bEntered: () => void = () => {};
    const bEnteredGate = new Promise<void>((resolveEntered) => {
      bEntered = resolveEntered;
    });
    let bPromise: Promise<void> = Promise.resolve();
    let dEntered = false;
    let dPromise: Promise<void> = Promise.resolve();
    let started = false;
    let bReleaseOnce = false;
    const enter = (): void => {
      inside += 1;
      maxInside = Math.max(maxInside, inside);
    };
    const leave = (): void => {
      inside -= 1;
    };
    try {
      setPluginInstallDirectoryLockReclaimHook(async (event: PluginInstallDirectoryLockReclaimEvent) => {
        if (event.phase === "stale-observed") {
          if (started) return;
          started = true;
          bPromise = withPluginInstallDirectoryLock(world.destination, async () => {
            enter();
            bEntered();
            await gate;
            leave();
          });
          await bEnteredGate;
          dPromise = withPluginInstallDirectoryLock(world.destination, async () => {
            enter();
            dEntered = true;
            leave();
          });
          return;
        }
        if (event.phase === "reclaim-settled") {
          if (inside !== 1 || bReleaseOnce) return;
          bReleaseOnce = true;
          releaseB();
          await bPromise;
        }
      });
      const aPromise = withPluginInstallDirectoryLock(world.destination, async () => {
        enter();
        leave();
      });
      await aPromise;
      await bPromise;
      await dPromise;
      expect(dEntered).toBe(true);
      expect(maxInside).toBe(1);
      expect(inside).toBe(0);
    } finally {
      releaseB();
      setPluginInstallDirectoryLockReclaimHook(undefined);
      setPluginInstallDirectoryLockGuardStaleMs(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("does not take over a dead reclaim guard younger than the bound", async () => {
    const world = await plantStaleLock();
    try {
      setPluginInstallDirectoryLockGuardStaleMs(60_000);
      const guardPath = `${world.lockPath}.reclaim`;
      await writeFile(
        guardPath,
        `${JSON.stringify({ pid: exitedPid(), nonce: randomUUID(), acquiredAtMs: Date.now() })}\n`,
      );
      expect(await tryPluginInstallDirectoryLock(world.destination)).toBeUndefined();
      expect(await readFile(world.lockPath, "utf8")).toBe(world.text);
    } finally {
      setPluginInstallDirectoryLockGuardStaleMs(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("reclaims a dead guard once it is older than the bound and then the stale lock", async () => {
    const world = await plantStaleLock();
    try {
      setPluginInstallDirectoryLockGuardStaleMs(1);
      const guardPath = `${world.lockPath}.reclaim`;
      await writeFile(
        guardPath,
        `${JSON.stringify({
          pid: exitedPid(),
          nonce: randomUUID(),
          acquiredAtMs: Date.now() - 10_000,
        })}\n`,
      );
      let entered = false;
      await withPluginInstallDirectoryLock(world.destination, async () => {
        entered = true;
        expect(await readFile(world.lockPath, "utf8")).not.toBe(world.text);
      });
      expect(entered).toBe(true);
      await expect(lstat(guardPath)).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      setPluginInstallDirectoryLockGuardStaleMs(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("leaves a partial lock file in place", async () => {
    const world = await plantDestination();
    try {
      await mkdir(dirname(world.lockPath), { recursive: true, mode: 0o700 });
      await writeFile(world.lockPath, "partial\n");
      expect(await tryPluginInstallDirectoryLock(world.destination)).toBeUndefined();
      expect(await readFile(world.lockPath, "utf8")).toBe("partial\n");
    } finally {
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("does not replace an empty or partial lock planted before publish", async () => {
    const world = await plantDestination();
    let planted = false;
    try {
      setPluginInstallDirectoryLockPublishHook(async (event: PluginInstallDirectoryLockPublishEvent) => {
        if (event.phase !== "staging-ready" || planted) return;
        planted = true;
        await mkdir(event.lockDir, { mode: 0o700 }).catch((error: unknown) => {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        });
      });
      const held = await tryPluginInstallDirectoryLock(world.destination);
      expect(held).toBeUndefined();
      expect(planted).toBe(true);
      expect(await readdir(world.lockPath)).toEqual([]);
    } finally {
      setPluginInstallDirectoryLockPublishHook(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("keeps a single holder when a second acquire runs during publish", async () => {
    const world = await plantDestination();
    let inside = 0;
    let maxInside = 0;
    let releaseSecond: () => void = () => {};
    const secondGate = new Promise<void>((resolveGate) => {
      releaseSecond = resolveGate;
    });
    let markSecondIn: () => void = () => {};
    const secondIn = new Promise<void>((resolveIn) => {
      markSecondIn = resolveIn;
    });
    let markBlocked: () => void = () => {};
    const blocked = new Promise<void>((resolveBlocked) => {
      markBlocked = resolveBlocked;
    });
    let secondPromise: Promise<void> = Promise.resolve();
    let started = false;
    let publisherEntered = false;
    let secondEntered = false;
    const enter = (): void => {
      inside += 1;
      maxInside = Math.max(maxInside, inside);
    };
    const leave = (): void => {
      inside -= 1;
    };
    try {
      setPluginInstallDirectoryLockWaitHook(() => {
        markBlocked();
      });
      setPluginInstallDirectoryLockPublishHook(async (event: PluginInstallDirectoryLockPublishEvent) => {
        if (event.phase !== "staging-ready" || started) return;
        started = true;
        secondPromise = withPluginInstallDirectoryLock(world.destination, async () => {
          enter();
          secondEntered = true;
          markSecondIn();
          await secondGate;
          leave();
        });
        const status = await Promise.race([
          secondIn.then(() => "entered" as const),
          blocked.then(() => "blocked" as const),
          delay(2_000).then(() => "timeout" as const),
        ]);
        if (status === "entered") {
          setTimeout(() => {
            if (!publisherEntered) releaseSecond();
          }, 1_000);
        }
      });
      const publisher = withPluginInstallDirectoryLock(world.destination, async () => {
        publisherEntered = true;
        enter();
        await delay(50);
        leave();
        releaseSecond();
      });
      await publisher;
      await secondPromise;
      expect(started).toBe(true);
      expect(publisherEntered).toBe(true);
      expect(secondEntered).toBe(true);
      expect(maxInside).toBe(1);
      expect(inside).toBe(0);
    } finally {
      releaseSecond();
      setPluginInstallDirectoryLockPublishHook(undefined);
      setPluginInstallDirectoryLockWaitHook(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("does not drop the nonce before its own lock is gone", async () => {
    const world = await plantDestination();
    let inside = 0;
    let maxInside = 0;
    let releaseWaiter: () => void = () => {};
    const waiterGate = new Promise<void>((resolveGate) => {
      releaseWaiter = resolveGate;
    });
    let markWaiterIn: () => void = () => {};
    const waiterIn = new Promise<void>((resolveIn) => {
      markWaiterIn = resolveIn;
    });
    let markBlocked: () => void = () => {};
    const blocked = new Promise<void>((resolveBlocked) => {
      markBlocked = resolveBlocked;
    });
    let waiterPromise: Promise<void> = Promise.resolve();
    let thirdPromise: Promise<void> = Promise.resolve();
    let started = false;
    let thirdEntered = false;
    let thirdStarted = false;
    let trackWaiter = false;
    const enter = (): void => {
      inside += 1;
      maxInside = Math.max(maxInside, inside);
    };
    const leave = (): void => {
      inside -= 1;
    };
    try {
      setPluginInstallDirectoryLockWaitHook(() => {
        if (trackWaiter) markBlocked();
      });
      setPluginInstallDirectoryLockPublishHook(async (event: PluginInstallDirectoryLockPublishEvent) => {
        if (event.phase !== "before-release-remove" || started) return;
        started = true;
        trackWaiter = true;
        waiterPromise = withPluginInstallDirectoryLock(world.destination, async () => {
          enter();
          markWaiterIn();
          await waiterGate;
          leave();
        });
        const status = await Promise.race([
          waiterIn.then(() => "entered" as const),
          blocked.then(() => "blocked" as const),
          delay(2_000).then(() => "timeout" as const),
        ]);
        trackWaiter = false;
        if (status === "entered") {
          thirdStarted = true;
          thirdPromise = withPluginInstallDirectoryLock(world.destination, async () => {
            enter();
            thirdEntered = true;
            leave();
          });
          await delay(50);
        }
      });
      await withPluginInstallDirectoryLock(world.destination, async () => {
        enter();
        leave();
      });
      if (!thirdStarted) {
        thirdStarted = true;
        thirdPromise = withPluginInstallDirectoryLock(world.destination, async () => {
          enter();
          thirdEntered = true;
          leave();
        });
      }
      await Promise.race([
        waiterIn.then(() => delay(100)),
        delay(400),
      ]);
      expect(started).toBe(true);
      expect(maxInside).toBe(1);
      releaseWaiter();
      await Promise.all([waiterPromise, thirdPromise]);
      expect(thirdEntered).toBe(true);
      expect(maxInside).toBe(1);
      expect(inside).toBe(0);
    } finally {
      releaseWaiter();
      setPluginInstallDirectoryLockPublishHook(undefined);
      setPluginInstallDirectoryLockWaitHook(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("keeps a single holder when a symlink is replaced before unlink", async () => {
    const world = await plantDestination();
    await mkdir(dirname(world.lockPath), { recursive: true, mode: 0o700 });
    const target = join(world.root, "elsewhere");
    await writeFile(target, "not-a-lock\n");
    await symlink(target, world.lockPath);
    let inside = 0;
    let maxInside = 0;
    let releaseSecond: () => void = () => {};
    const secondGate = new Promise<void>((resolveGate) => {
      releaseSecond = resolveGate;
    });
    let markSecondIn: () => void = () => {};
    const secondIn = new Promise<void>((resolveIn) => {
      markSecondIn = resolveIn;
    });
    let markBlocked: () => void = () => {};
    const blocked = new Promise<void>((resolveBlocked) => {
      markBlocked = resolveBlocked;
    });
    let secondPromise: Promise<void> = Promise.resolve();
    let started = false;
    let publisherEntered = false;
    let secondEntered = false;
    const enter = (): void => {
      inside += 1;
      maxInside = Math.max(maxInside, inside);
    };
    const leave = (): void => {
      inside -= 1;
    };
    const replaceAndHold = async (): Promise<void> => {
      if (started) return;
      started = true;
      unlinkInterleave.run = undefined;
      await rm(world.lockPath);
      secondPromise = withPluginInstallDirectoryLock(world.destination, async () => {
        enter();
        secondEntered = true;
        markSecondIn();
        await secondGate;
        leave();
      });
      const status = await Promise.race([
        secondIn.then(() => "entered" as const),
        blocked.then(() => "blocked" as const),
        delay(2_000).then(() => "timeout" as const),
      ]);
      if (status === "entered") {
        setTimeout(() => {
          if (!publisherEntered) releaseSecond();
        }, 1_000);
      }
    };
    try {
      setPluginInstallDirectoryLockWaitHook(() => {
        markBlocked();
      });
      setPluginInstallDirectoryLockReclaimHook(async (event: PluginInstallDirectoryLockReclaimEvent) => {
        if (event.phase !== "stale-observed") return;
        await replaceAndHold();
      });
      unlinkInterleave.lockPath = world.lockPath;
      unlinkInterleave.run = replaceAndHold;
      const publisher = withPluginInstallDirectoryLock(world.destination, async () => {
        publisherEntered = true;
        enter();
        await delay(50);
        leave();
        releaseSecond();
      });
      await publisher;
      await secondPromise;
      expect(started).toBe(true);
      expect(publisherEntered).toBe(true);
      expect(secondEntered).toBe(true);
      expect(maxInside).toBe(1);
      expect(inside).toBe(0);
    } finally {
      releaseSecond();
      unlinkInterleave.run = undefined;
      unlinkInterleave.lockPath = undefined;
      setPluginInstallDirectoryLockReclaimHook(undefined);
      setPluginInstallDirectoryLockWaitHook(undefined);
      await rm(world.root, { recursive: true, force: true });
    }
  });

  it("reclaims an old partial file and an old empty directory at the lock path", async () => {
    const partial = await plantDestination();
    const directory = await plantDestination();
    const past = new Date(Date.now() - 120_000);
    try {
      await mkdir(dirname(partial.lockPath), { recursive: true, mode: 0o700 });
      await writeFile(partial.lockPath, "partial\n");
      await utimes(partial.lockPath, past, past);
      let partialEntered = false;
      await withPluginInstallDirectoryLock(partial.destination, async () => {
        partialEntered = true;
        expect(await readFile(partial.lockPath, "utf8")).not.toBe("partial\n");
      });
      expect(partialEntered).toBe(true);

      await mkdir(dirname(directory.lockPath), { recursive: true, mode: 0o700 });
      await mkdir(directory.lockPath);
      await utimes(directory.lockPath, past, past);
      let directoryEntered = false;
      await withPluginInstallDirectoryLock(directory.destination, async () => {
        directoryEntered = true;
        expect((await lstat(directory.lockPath)).isFile()).toBe(true);
      });
      expect(directoryEntered).toBe(true);
    } finally {
      await rm(partial.root, { recursive: true, force: true });
      await rm(directory.root, { recursive: true, force: true });
    }
  });
});
