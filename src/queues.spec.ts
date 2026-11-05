import type { PgBoss } from "pg-boss";
import {
  ensureQueue,
  ensureQueueOnce,
  forgetEnsuredQueue,
  readAllPages,
} from "./queues";

const fakeBoss = (stored: Record<string, unknown> | null = null) => {
  const calls: string[] = [];
  const boss = {
    createQueue: vi.fn(async (name: string) => {
      calls.push(name);
    }),
    getQueue: vi.fn(async () => stored),
    updateQueue: vi.fn(async () => {}),
  };
  return { boss: boss as unknown as PgBoss, mock: boss, calls };
};

const withDeadLetter = (retryLimit: number) => ({
  queueName: "main",
  queueOptions: {
    deadLetter: { queueName: "dead", queueOptions: { retryLimit } },
  },
});

const equalDeclaration = () => ({
  queueName: "once",
  queueOptions: {
    retryLimit: 1,
    deadLetter: { queueName: "dead", queueOptions: {} },
  },
});

describe("ensureQueueOnce", () => {
  it("sets a queue up once, also when asked at the same time", async () => {
    const { boss, mock } = fakeBoss();
    const declaration = { queueName: "once" };

    await Promise.all([
      ensureQueueOnce(boss, declaration),
      ensureQueueOnce(boss, declaration),
    ]);
    await ensureQueueOnce(boss, declaration);

    expect(mock.createQueue).toHaveBeenCalledTimes(1);
  });

  it("tracks each pg-boss instance separately", async () => {
    const first = fakeBoss();
    const second = fakeBoss();

    await ensureQueueOnce(first.boss, { queueName: "once" });
    await ensureQueueOnce(second.boss, { queueName: "once" });

    expect(second.mock.createQueue).toHaveBeenCalledTimes(1);
  });

  it("throws on a different declaration of the same queue", async () => {
    const { boss, mock } = fakeBoss();
    await ensureQueueOnce(boss, {
      queueName: "once",
      queueOptions: { retryLimit: 1 },
    });

    await expect(
      ensureQueueOnce(boss, {
        queueName: "once",
        queueOptions: { retryLimit: 2 },
      }),
    ).rejects.toThrow(/declared twice/);
    await expect(
      ensureQueueOnce(boss, {
        queueName: "once",
        queueOptions: { retryLimit: 1 },
        subscribeTo: [{ eventName: "changed" }],
      }),
    ).rejects.toThrow(/declared twice/);
    expect(mock.createQueue).toHaveBeenCalledTimes(1);
  });

  it("accepts an equal declaration made separately", async () => {
    const { boss, mock } = fakeBoss();
    await ensureQueueOnce(boss, equalDeclaration());
    await ensureQueueOnce(boss, equalDeclaration());

    expect(mock.createQueue).toHaveBeenCalledTimes(2);
  });

  it("treats undefined options and an empty subscribeTo as not declared", async () => {
    const { boss, mock } = fakeBoss();

    await ensureQueueOnce(boss, {
      queueName: "once",
      queueOptions: { retryLimit: 1 },
    });
    await ensureQueueOnce(boss, {
      queueName: "once",
      queueOptions: { retryLimit: 1, expireInSeconds: undefined },
      subscribeTo: [],
    });

    expect(mock.createQueue).toHaveBeenCalledTimes(1);
  });

  it("lets an undefined option fall back to its default", async () => {
    const { boss, mock } = fakeBoss();

    await ensureQueue(
      boss,
      { queueName: "defaulted", queueOptions: { retryLimit: undefined } },
      { defaultQueueOptions: { retryLimit: 5 } },
    );

    expect(mock.createQueue).toHaveBeenCalledWith("defaulted", {
      retryLimit: 5,
    });
  });

  it("applies a dead letter declaration that follows a bare reference to it", async () => {
    const { boss, mock } = fakeBoss();

    await ensureQueueOnce(boss, {
      queueName: "main",
      queueOptions: { deadLetter: { queueName: "dead" } },
    });
    await ensureQueueOnce(boss, {
      queueName: "main",
      queueOptions: {
        deadLetter: { queueName: "dead", queueOptions: { retryLimit: 1 } },
      },
    });

    expect(mock.createQueue).toHaveBeenCalledWith("dead", { retryLimit: 1 });
  });

  it("throws when a matching job names a conflicting dead letter queue", async () => {
    const { boss } = fakeBoss();
    await ensureQueueOnce(boss, withDeadLetter(1));

    await expect(ensureQueueOnce(boss, withDeadLetter(2))).rejects.toThrow(
      /"dead" is declared twice/,
    );
  });

  it("still tells leaving out schedules from an empty list", async () => {
    const { boss } = fakeBoss();

    await ensureQueueOnce(boss, { queueName: "once", queueOptions: {} });

    await expect(
      ensureQueueOnce(boss, {
        queueName: "once",
        queueOptions: {},
        schedules: [],
      }),
    ).rejects.toThrow(/declared twice/);
  });

  it("applies a full declaration after a bare reference, and not the reverse", async () => {
    const { boss, mock } = fakeBoss();
    const full = { queueName: "dead", queueOptions: { retryLimit: 1 } };

    await ensureQueueOnce(boss, { queueName: "dead" });
    await ensureQueueOnce(boss, full);
    await ensureQueueOnce(boss, { queueName: "dead" });

    expect(mock.createQueue).toHaveBeenCalledTimes(2);
    expect(mock.createQueue).toHaveBeenLastCalledWith("dead", {
      retryLimit: 1,
    });
  });

  it("sets up a dead letter queue once when it is also a job", async () => {
    const { boss, mock, calls } = fakeBoss();
    const dead = { queueName: "dead", queueOptions: {} };

    await ensureQueueOnce(boss, dead);
    await ensureQueueOnce(boss, {
      queueName: "main",
      queueOptions: { deadLetter: dead },
    });

    expect(calls).toEqual(["dead", "main"]);
    expect(mock.createQueue).toHaveBeenCalledTimes(2);
  });

  it("does not hang on a queue that is its own dead letter queue", async () => {
    const { boss } = fakeBoss();

    await expect(
      ensureQueueOnce(boss, {
        queueName: "self",
        queueOptions: { deadLetter: { queueName: "self" } },
      }),
    ).resolves.toBeUndefined();
  });

  it("tries again after a failure", async () => {
    const { boss, mock } = fakeBoss();
    mock.createQueue.mockRejectedValueOnce(new Error("database gone"));

    await expect(ensureQueueOnce(boss, { queueName: "once" })).rejects.toThrow(
      "database gone",
    );
    await ensureQueueOnce(boss, { queueName: "once" });

    expect(mock.createQueue).toHaveBeenCalledTimes(2);
  });

  it("sets a queue up again once it has been forgotten", async () => {
    const { boss, mock } = fakeBoss();

    await ensureQueueOnce(boss, { queueName: "once" });
    forgetEnsuredQueue(boss, "once");
    await ensureQueueOnce(boss, { queueName: "once" });

    expect(mock.createQueue).toHaveBeenCalledTimes(2);
  });
});

describe("ensureQueue", () => {
  it("creates a queue with no declared options without reading it back", async () => {
    const { boss, mock } = fakeBoss();

    await ensureQueue(boss, { queueName: "plain" });

    expect(mock.createQueue).toHaveBeenCalledWith("plain", {});
    expect(mock.getQueue).not.toHaveBeenCalled();
  });

  it("creates the dead letter queue first, with its own options", async () => {
    const { boss, mock, calls } = fakeBoss();

    await ensureQueue(boss, {
      queueName: "main",
      queueOptions: {
        retryLimit: 1,
        deadLetter: { queueName: "dead", queueOptions: { retryLimit: 0 } },
      },
    });

    expect(calls).toEqual(["dead", "main"]);
    expect(mock.createQueue).toHaveBeenCalledWith("dead", { retryLimit: 0 });
    expect(mock.createQueue).toHaveBeenCalledWith("main", {
      retryLimit: 1,
      deadLetter: "dead",
    });
  });

  it("updates only the declared options that differ", async () => {
    const { boss, mock } = fakeBoss({
      policy: "short",
      retryLimit: 2,
      retryDelay: 9,
      deleteAfterSeconds: 60,
    });

    await ensureQueue(boss, {
      queueName: "drifted",
      queueOptions: {
        policy: "short",
        retryLimit: 5,
        deleteAfterSeconds: 60,
        expireInSeconds: undefined,
      },
    });

    expect(mock.updateQueue).toHaveBeenCalledWith("drifted", { retryLimit: 5 });
  });

  it("leaves undefined options out of what pg-boss receives", async () => {
    const { boss, mock } = fakeBoss({ retryLimit: 1 });

    await ensureQueue(boss, {
      queueName: "sparse",
      queueOptions: { retryLimit: 1, expireInSeconds: undefined },
    });

    expect(mock.createQueue).toHaveBeenCalledWith("sparse", { retryLimit: 1 });
  });

  it("sends retryBackoff along with a changed retryDelayMax", async () => {
    const { boss, mock } = fakeBoss({ retryBackoff: true, retryDelayMax: 30 });

    await ensureQueue(boss, {
      queueName: "backoff",
      queueOptions: { retryBackoff: true, retryDelayMax: 60 },
    });

    expect(mock.updateQueue).toHaveBeenCalledWith("backoff", {
      retryDelayMax: 60,
      retryBackoff: true,
    });
  });

  it("skips the queue but keeps schedules and subscriptions with createQueues: false", async () => {
    const { boss, mock } = fakeBoss();
    const extras = {
      getSchedules: vi.fn(async () => []),
      schedule: vi.fn(async () => {}),
      subscribe: vi.fn(async () => {}),
    };
    Object.assign(boss, extras);

    await ensureQueue(
      boss,
      {
        queueName: "external",
        schedules: [{ cron: "0 0 * * *", data: {} }],
        subscribeTo: [{ eventName: "happened" }],
      },
      { createQueues: false },
    );

    expect(mock.createQueue).not.toHaveBeenCalled();
    expect(extras.schedule).toHaveBeenCalled();
    expect(extras.subscribe).toHaveBeenCalledWith("happened", "external");
  });

  it("does not update a queue that already matches", async () => {
    const { boss, mock } = fakeBoss({ retryLimit: 5 });

    await ensureQueue(boss, {
      queueName: "same",
      queueOptions: { retryLimit: 5 },
    });

    expect(mock.updateQueue).not.toHaveBeenCalled();
  });

  it.each([
    ["policy", { policy: "short" }, { policy: "standard" }],
    ["partition", { partition: true }, { partition: false }],
  ])(
    "refuses a %s that differs from the stored queue",
    async (option, queueOptions, stored) => {
      const { boss, mock } = fakeBoss(stored);

      await expect(
        ensureQueue(boss, { queueName: "fixed", queueOptions }),
      ).rejects.toThrow(`pg-boss cannot change ${option}`);
      expect(mock.updateQueue).not.toHaveBeenCalled();
    },
  );
});

describe("readAllPages", () => {
  it("follows the cursor past a full page", async () => {
    const all = Array.from({ length: 2_500 }, (_unused, index) => index);
    const readPage = vi.fn(
      async ({ limit, after }: { limit: number; after?: number }) =>
        all.slice(after === undefined ? 0 : after + 1).slice(0, limit),
    );

    await expect(readAllPages(readPage, (row: number) => row)).resolves.toEqual(
      all,
    );
    expect(readPage).toHaveBeenCalledTimes(3);
  });
});
