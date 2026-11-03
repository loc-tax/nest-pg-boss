import type { PgBoss } from "pg-boss";
import { registerWorkers } from "./workers";

type RegisteredHandler = (jobs: unknown[]) => Promise<unknown>;

const fakeBoss = () => {
  const handlers: RegisteredHandler[] = [];
  const mock = {
    work: vi.fn(
      async (_name: string, _options: object, handler: RegisteredHandler) => {
        handlers.push(handler);
        return `worker-${handlers.length}`;
      },
    ),
    notifyWorker: vi.fn(),
    getWipData: vi.fn(() => [
      { id: "worker-1", workId: "worker-1" },
      { id: "sibling", workId: "worker-1" },
      { id: "elsewhere", workId: "other" },
    ]),
  };
  return { boss: mock as unknown as PgBoss, mock, handlers };
};

const outcome = async (handler: RegisteredHandler | undefined) => {
  await handler?.([]).catch(() => {});
};

describe("registerWorkers", () => {
  it("leaves workers alone without drain", async () => {
    const { boss, mock, handlers } = fakeBoss();
    const handler = vi.fn(async () => {});

    await registerWorkers(boss, "queue", { localConcurrency: 3 }, handler);
    await outcome(handlers[0]);

    expect(mock.work).toHaveBeenCalledTimes(1);
    expect(mock.work).toHaveBeenCalledWith(
      "queue",
      { localConcurrency: 3 },
      handler,
    );
    expect(mock.notifyWorker).not.toHaveBeenCalled();
  });

  it("registers separate workers, and a success wakes only its own", async () => {
    const { boss, mock, handlers } = fakeBoss();

    const ids = await registerWorkers(
      boss,
      "queue",
      { drain: true, localConcurrency: 3 },
      async () => "output",
    );

    expect(ids).toEqual(["worker-1", "worker-2", "worker-3"]);
    expect(mock.work.mock.calls[0]?.[1]).toEqual({ localConcurrency: 1 });

    await expect(handlers[1]?.([])).resolves.toBe("output");

    expect(mock.notifyWorker.mock.calls).toEqual([["worker-2"]]);
  });

  it("wakes nothing when a handler throws", async () => {
    const { boss, mock, handlers } = fakeBoss();

    await registerWorkers(
      boss,
      "queue",
      { drain: true, localConcurrency: 2 },
      async () => {
        throw new Error("failed");
      },
    );

    await expect(handlers[0]?.([])).rejects.toThrow("failed");
    expect(mock.notifyWorker).not.toHaveBeenCalled();
  });

  it("keeps a localGroupConcurrency registration together, and wakes only that group", async () => {
    const { boss, mock, handlers } = fakeBoss();

    const ids = await registerWorkers(
      boss,
      "queue",
      { drain: true, localConcurrency: 4, localGroupConcurrency: 2 },
      async () => {},
    );

    expect(ids).toEqual(["worker-1"]);
    expect(mock.work.mock.calls[0]?.[1]).toEqual({
      localConcurrency: 4,
      localGroupConcurrency: 2,
    });

    await handlers[0]?.([]);

    expect(mock.notifyWorker.mock.calls).toEqual([["worker-1"], ["sibling"]]);
  });

  it("forwards extra arguments such as a transaction", async () => {
    const { boss, handlers } = fakeBoss();
    const handler = vi.fn(async () => {});

    await registerWorkers(boss, "queue", { drain: true }, handler);
    await (handlers[0] as (...args: unknown[]) => Promise<unknown>)(
      ["job"],
      "tx",
    );

    expect(handler).toHaveBeenCalledWith(["job"], "tx");
  });

  it("lets pg-boss reject an invalid count", async () => {
    const { boss, mock } = fakeBoss();

    await registerWorkers(
      boss,
      "queue",
      { drain: true, localConcurrency: 0 },
      async () => {},
    );

    expect(mock.work).toHaveBeenCalledTimes(1);
    expect(mock.work.mock.calls[0]?.[1]).toEqual({ localConcurrency: 0 });
  });
});
