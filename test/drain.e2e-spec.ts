import { setTimeout as sleep } from "node:timers/promises";
import { Injectable } from "@nestjs/common";
import { PgBoss, type Job as PgBossJob } from "pg-boss";
import { createJob, JobService, type Job } from "../src";
import { items, serviceOf, useApps, waitFor, type Item } from "./support";

/** Long enough that a test only passes if draining, not polling, did the work. */
const SLOW_POLL = { pollingIntervalSeconds: 60 } as const;

describe("Handle({ drain: true }) (e2e)", () => {
  const { build, close } = useApps();

  const queueBacklog = async (job: Job<Item>, count: number) => {
    const sender = await build({
      jobs: [job],
      options: { disableWorkers: true },
    });
    const service = serviceOf(sender, job);
    await service.insert(items(count));
    return service;
  };

  it("works through a backlog in order without waiting for the next poll", async () => {
    const DrainJob = createJob<Item>("drain-order", { retryLimit: 0 });

    @Injectable()
    class Handler {
      readonly seen: number[] = [];

      @DrainJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(job: PgBossJob<Item>) {
        this.seen.push(job.data.index);
      }
    }

    const sender = await build({
      jobs: [DrainJob],
      options: { disableWorkers: true },
    });
    for (const { data } of items(20)) {
      // oxlint-disable-next-line no-await-in-loop -- jobs inserted together have no defined order.
      await serviceOf(sender, DrainJob).send(data);
    }

    const app = await build({ jobs: [DrainJob], providers: [Handler] });
    const handler = app.get(Handler);

    await waitFor(() => handler.seen.length === 20, 10_000);

    expect(handler.seen).toEqual(items(20).map(({ data }) => data.index));
  });

  it("does not drain without the option", async () => {
    const PlainJob = createJob<Item>("drain-off", { retryLimit: 0 });

    @Injectable()
    class Handler {
      seen = 0;

      @PlainJob.Handle(SLOW_POLL)
      async handle(_job: PgBossJob<Item>) {
        this.seen++;
      }
    }

    await queueBacklog(PlainJob, 3);
    const app = await build({ jobs: [PlainJob], providers: [Handler] });

    await sleep(1_500);

    expect(app.get(Handler).seen).toBe(1);
  });

  it("claims one job at a time and saves each result as it finishes", async () => {
    const SavedJob = createJob<Item>("drain-saved", { retryLimit: 0 });

    @Injectable()
    class Handler {
      readonly snapshots: { active: number; completed: number }[] = [];

      constructor(
        @SavedJob.Inject() private readonly service: JobService<Item>,
      ) {}

      @SavedJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(_job: PgBossJob<Item>) {
        const jobs = await this.service.findJobs();
        this.snapshots.push({
          active: jobs.filter(({ state }) => state === "active").length,
          completed: jobs.filter(({ state }) => state === "completed").length,
        });
      }
    }

    await queueBacklog(SavedJob, 5);
    const app = await build({ jobs: [SavedJob], providers: [Handler] });

    await waitFor(() => app.get(Handler).snapshots.length === 5, 10_000);

    // A crash at any of these points would rerun only the one active job.
    expect(app.get(Handler).snapshots).toEqual(
      [0, 1, 2, 3, 4].map((completed) => ({ active: 1, completed })),
    );
  });

  it("falls back to polling after a handler throws, leaving other jobs untouched", async () => {
    const ThrowingJob = createJob<Item>("drain-throws", { retryLimit: 0 });

    @Injectable()
    class Handler {
      attempts = 0;

      @ThrowingJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(_job: PgBossJob<Item>) {
        this.attempts++;
        throw new Error("dependency is down");
      }
    }

    const service = await queueBacklog(ThrowingJob, 5);
    const app = await build({ jobs: [ThrowingJob], providers: [Handler] });

    await waitFor(() => app.get(Handler).attempts === 1);
    await sleep(1_000);

    // It did not burn through the backlog, and the rest keep every retry.
    expect(app.get(Handler).attempts).toBe(1);
    const jobs = await service.findJobs();
    expect(jobs.filter(({ state }) => state === "failed")).toHaveLength(1);
    const untouched = jobs.filter(({ state }) => state === "created");
    expect(untouched).toHaveLength(4);
    expect(untouched.every(({ retryCount }) => retryCount === 0)).toBe(true);
  });

  it("leaves unstarted jobs alone when a handler stops the worker with offWork", async () => {
    const OffWorkJob = createJob<Item>("drain-off-work", { retryLimit: 1 });

    @Injectable()
    class Handler {
      started = 0;

      constructor(
        @OffWorkJob.Inject() private readonly service: JobService<Item>,
      ) {}

      @OffWorkJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(_job: PgBossJob<Item>) {
        this.started++;
        if (this.started === 2) {
          // Waiting here would wait for this very handler.
          await this.service.offWork({ wait: false });
        }
      }
    }

    const service = await queueBacklog(OffWorkJob, 6);
    const app = await build({ jobs: [OffWorkJob], providers: [Handler] });

    await waitFor(() => app.get(Handler).started === 2);
    await waitFor(async () => {
      const jobs = await service.findJobs();
      return jobs.filter(({ state }) => state === "completed").length === 2;
    });
    await sleep(500);

    expect(app.get(Handler).started).toBe(2);
    const jobs = await service.findJobs();
    const untouched = jobs.filter(({ state }) => state === "created");
    expect(untouched).toHaveLength(4);
    expect(untouched.every(({ retryCount }) => retryCount === 0)).toBe(true);
  });

  it("finishes the job in flight when the app stops, and the rest wait for the next instance", async () => {
    const StoppingJob = createJob<Item>("drain-stop", { retryLimit: 1 });

    @Injectable()
    class Handler {
      started = 0;
      finished = 0;

      @StoppingJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(_job: PgBossJob<Item>) {
        this.started++;
        await sleep(300);
        this.finished++;
      }
    }

    const service = await queueBacklog(StoppingJob, 8);
    const first = await build({ jobs: [StoppingJob], providers: [Handler] });
    const handler = first.get(Handler);

    await waitFor(() => handler.started === 2);
    await close(first);

    expect(handler.finished).toBe(handler.started);
    const afterStop = await service.findJobs();
    expect(afterStop.filter(({ state }) => state === "active")).toHaveLength(0);
    expect(afterStop.filter(({ state }) => state === "failed")).toHaveLength(0);
    const waiting = afterStop.filter(({ state }) => state === "created");
    expect(waiting).toHaveLength(8 - handler.finished);
    expect(waiting.every(({ retryCount }) => retryCount === 0)).toBe(true);

    const second = await build({ jobs: [StoppingJob], providers: [Handler] });
    await waitFor(() => second.get(Handler).finished === waiting.length);
    const completed = await service.findJobs();
    expect(completed.every(({ state }) => state === "completed")).toBe(true);
    expect(completed.every(({ retryCount }) => retryCount === 0)).toBe(true);
  });

  it("gives each job its own expiry, so a long backlog cannot expire", async () => {
    const ExpiringJob = createJob<Item>("drain-expiry", {
      retryLimit: 0,
      expireInSeconds: 1,
    });

    @Injectable()
    class Handler {
      finished = 0;

      @ExpiringJob.Handle({ ...SLOW_POLL, drain: true })
      async handle(_job: PgBossJob<Item>) {
        await sleep(400);
        this.finished++;
      }
    }

    // Six jobs at 400ms take 2.4s against a 1s expiry per job.
    const service = await queueBacklog(ExpiringJob, 6);
    const app = await build({ jobs: [ExpiringJob], providers: [Handler] });

    await waitFor(async () => {
      await service.supervise();
      return app.get(Handler).finished === 6;
    });
    await waitFor(async () =>
      (await service.findJobs()).every(({ state }) => state === "completed"),
    );
  });

  it("keeps every worker of a localConcurrency registration draining", async () => {
    const ParallelJob = createJob<Item>("drain-parallel", { retryLimit: 0 });

    @Injectable()
    class Handler {
      running = 0;
      peak = 0;
      peakAfterFirstWave = 0;
      finished = 0;

      @ParallelJob.Handle({ ...SLOW_POLL, drain: true, localConcurrency: 4 })
      async handle(_job: PgBossJob<Item>) {
        this.running++;
        this.peak = Math.max(this.peak, this.running);
        if (this.finished >= 8) {
          this.peakAfterFirstWave = Math.max(
            this.peakAfterFirstWave,
            this.running,
          );
        }
        await sleep(100);
        this.running--;
        this.finished++;
      }
    }

    await queueBacklog(ParallelJob, 40);
    const app = await build({ jobs: [ParallelJob], providers: [Handler] });

    await waitFor(() => app.get(Handler).finished === 40, 10_000);

    expect(app.get(Handler).peak).toBeLessThanOrEqual(4);
    // Well past the first fetches, so this only holds if all workers carried on.
    expect(app.get(Handler).peakAfterFirstWave).toBe(4);
  });

  it.each([
    ["number", 2],
    ["tiers", { default: 1, tiers: { gold: 3 } }],
  ])("works with localGroupConcurrency as %s", async (label, limit) => {
    const GroupJob = createJob<Item>(`drain-local-group-${label}`, {
      retryLimit: 0,
    });

    @Injectable()
    class Handler {
      running = 0;
      peak = 0;
      finished = 0;

      @GroupJob.Handle({
        ...SLOW_POLL,
        drain: true,
        localConcurrency: 4,
        localGroupConcurrency: limit,
      })
      async handle(_job: PgBossJob<Item>) {
        this.running++;
        this.peak = Math.max(this.peak, this.running);
        await sleep(50);
        this.running--;
        this.finished++;
      }
    }

    const sender = await build({
      jobs: [GroupJob],
      options: { disableWorkers: true },
    });
    await serviceOf(sender, GroupJob).insert(
      items(12).map(({ data }) => ({ data, group: { id: "tenant" } })),
    );

    const app = await build({ jobs: [GroupJob], providers: [Handler] });
    const handler = app.get(Handler);
    await waitFor(() => handler.finished === 12, 15_000);

    expect(handler.peak).toBeLessThanOrEqual(
      typeof limit === "number" ? limit : limit.default,
    );
  });

  it("stops every worker of a draining JobService.work by its id", async () => {
    const StoppableJob = createJob<Item>("drain-stop-by-id", { retryLimit: 0 });
    const app = await build({ jobs: [StoppableJob] });
    const service = serviceOf(app, StoppableJob);
    const boss = app.get(PgBoss);
    const workersOnQueue = () =>
      boss.getWipData().filter(({ name }) => name === "drain-stop-by-id");

    const id = await service.work(
      { ...SLOW_POLL, drain: true, localConcurrency: 3 },
      async () => {},
    );
    expect(workersOnQueue()).toHaveLength(3);

    await service.offWork({ id });

    expect(workersOnQueue()).toHaveLength(0);
  });

  it("also drains batch handlers, including a final partial batch", async () => {
    const BatchJob = createJob<Item>("drain-batch", { retryLimit: 0 });

    @Injectable()
    class Handler {
      readonly sizes: number[] = [];

      @BatchJob.Handle({ ...SLOW_POLL, drain: true, batchSize: 4 })
      async handle(jobs: PgBossJob<Item>[]) {
        this.sizes.push(jobs.length);
      }
    }

    await queueBacklog(BatchJob, 10);
    const app = await build({ jobs: [BatchJob], providers: [Handler] });

    await waitFor(() => app.get(Handler).sizes.length === 3, 10_000);

    expect(app.get(Handler).sizes).toEqual([4, 4, 2]);
  });

  it("applies through defaultWorkOptions and JobService.work", async () => {
    const DefaultedJob = createJob<Item>("drain-default", { retryLimit: 0 });

    @Injectable()
    class Handler {
      seen = 0;

      @DefaultedJob.Handle(SLOW_POLL)
      async handle(_job: PgBossJob<Item>) {
        this.seen++;
      }
    }

    await queueBacklog(DefaultedJob, 5);
    const app = await build({
      jobs: [DefaultedJob],
      providers: [Handler],
      options: { defaultWorkOptions: { drain: true } },
    });
    await waitFor(() => app.get(Handler).seen === 5, 10_000);

    const RuntimeJob = createJob<Item>("drain-runtime", { retryLimit: 0 });
    await queueBacklog(RuntimeJob, 5);
    const runtime = await build({ jobs: [RuntimeJob] });
    let seen = 0;
    await serviceOf(runtime, RuntimeJob).work(
      { ...SLOW_POLL, drain: true },
      async (jobs) => {
        seen += jobs.length;
      },
    );
    await waitFor(() => seen === 5, 10_000);
  });
});
