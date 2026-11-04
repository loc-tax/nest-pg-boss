import { setTimeout as sleep } from "node:timers/promises";
import { Injectable, Logger, type INestApplication } from "@nestjs/common";
import {
  PgBoss,
  type Job as PgBossJob,
  type JobWithMetadata,
  type Warning,
} from "pg-boss";
import {
  createEvent,
  createJob,
  HandlerScannerService,
  JobService,
  OnPgBossEvent,
  PubSubService,
} from "../src";
import { items, POLL, serviceOf, useApps, waitFor, type Item } from "./support";

const storedSchedulesOn = async (app: INestApplication) =>
  (await app.get(PgBoss).getSchedules("schedules-sender")).map(
    ({ key, cron }) => [key, cron],
  );

const withSchedules = (
  schedules: {
    cron: string;
    data: Item;
    options?: { key?: string; tz?: string; missed?: "skip" | "once" };
  }[],
) => createJob<Item>("schedules-synced", { schedules });

class WrongListener {
  // @ts-expect-error `warning` delivers a Warning, not a number
  @OnPgBossEvent("warning")
  onWarning(_count: number) {}
}

describe("pg-boss features through this package (e2e)", () => {
  const { build, close } = useApps();

  describe("JobService", () => {
    it("settles fetched jobs by attempt, which a later attempt is safe from", async () => {
      const ManualJob = createJob<Item>("api-manual", { retryLimit: 1 });
      const app = await build({ jobs: [ManualJob] });
      const service = serviceOf(app, ManualJob);

      await service.send({ index: 1 });
      const [firstAttempt] = await service.fetch();
      if (!firstAttempt) {
        throw new Error("Expected a job");
      }
      await service.fail(firstAttempt);

      const [secondAttempt] = await service.fetch();
      expect(secondAttempt?.id).toBe(firstAttempt.id);
      expect(secondAttempt?.retryCount).toBe(1);

      // The first attempt's claim lapsed, so completing it changes nothing.
      await service.complete(firstAttempt, { from: "stale" });
      const [stillActive] = await service.findJobs({ id: firstAttempt.id });
      expect(stillActive?.state).toBe("active");

      await service.touch(secondAttempt ?? firstAttempt.id);
      await service.complete(secondAttempt ?? firstAttempt.id, { from: "new" });
      const [done] = await service.findJobs({ id: firstAttempt.id });
      expect(done).toMatchObject({
        state: "completed",
        output: { from: "new" },
      });
    });

    it("fetches with metadata", async () => {
      const MetadataJob = createJob<Item>("api-fetch-metadata", {
        retryLimit: 3,
      });
      const app = await build({ jobs: [MetadataJob] });
      const service = serviceOf(app, MetadataJob);

      await service.send({ index: 1 });
      const [job] = await service.fetch({ includeMetadata: true });

      expect(job?.retryLimit).toBe(3);
    });

    it("covers cancel, resume, retry, delete and the delete helpers", async () => {
      const ManageJob = createJob<Item>("api-manage", { retryLimit: 0 });
      const app = await build({ jobs: [ManageJob] });
      const service = serviceOf(app, ManageJob);
      const stateOf = async (id: string) =>
        (await service.findJobs({ id }))[0]?.state;

      const cancelledId = (await service.send({ index: 1 })) ?? "";
      await service.cancel(cancelledId);
      await expect(stateOf(cancelledId)).resolves.toBe("cancelled");
      await service.resume(cancelledId);
      await expect(stateOf(cancelledId)).resolves.toBe("created");

      const [fetched] = await service.fetch();
      await service.fail(fetched?.id ?? "");
      await expect(stateOf(cancelledId)).resolves.toBe("failed");
      await service.retry(cancelledId);
      await expect(stateOf(cancelledId)).resolves.toBe("retry");

      await service.deleteJob(cancelledId);
      await expect(stateOf(cancelledId)).resolves.toBeUndefined();

      await service.insert(items(3));
      await expect(service.deleteQueuedJobs()).resolves.toBe(3);
      await service.insert(items(2));
      await expect(service.deleteAllJobs()).resolves.toBe(2);
      await expect(service.deleteStoredJobs()).resolves.toBe(0);
    });

    it("returns ids from insert only with returnId", async () => {
      const InsertJob = createJob<Item>("api-insert");
      const app = await build({ jobs: [InsertJob] });
      const service = serviceOf(app, InsertJob);

      await expect(service.insert(items(2))).resolves.toBeNull();

      const ids = await service.insert(items(2), { returnId: true });
      expect(ids).toHaveLength(2);
    });

    it("updates and upserts a waiting job by key", async () => {
      const KeyedJob = createJob<Item>("api-update");
      const app = await build({ jobs: [KeyedJob] });
      const service = serviceOf(app, KeyedJob);

      const inserted = await service.upsert(
        { index: 1 },
        { singletonKey: "same" },
      );
      expect(inserted.jobs).toHaveLength(1);

      await service.update({ index: 2 }, { singletonKey: "same" });
      const replaced = await service.upsert(
        { index: 3 },
        { singletonKey: "same" },
      );
      expect(replaced.updated).toBe(1);

      const jobs = await service.findJobs();
      expect(jobs).toHaveLength(1);
      expect(jobs[0]?.data).toEqual({ index: 3 });
    });

    it("sends deferred, throttled and debounced jobs", async () => {
      const TimedJob = createJob<Item>("api-timed");
      const app = await build({ jobs: [TimedJob] });
      const service = serviceOf(app, TimedJob);

      const deferred = await service.sendAfter({ index: 1 }, null, 3_600);
      const [job] = await service.findJobs({ id: deferred ?? "" });
      expect(job?.startAfter.getTime()).toBeGreaterThan(Date.now() + 3_000_000);

      await expect(
        service.sendThrottled({ index: 2 }, null, 60, "throttle"),
      ).resolves.toEqual(expect.any(String));
      await expect(
        service.sendThrottled({ index: 3 }, null, 60, "throttle"),
      ).resolves.toBeNull();

      await expect(
        service.sendDebounced({ index: 4 }, null, 60, "debounce"),
      ).resolves.toEqual(expect.any(String));
    });

    it("reads, updates and deletes its queue", async () => {
      const QueueJob = createJob<Item>("api-queue", { retryLimit: 1 });
      const app = await build({ jobs: [QueueJob] });
      const service = serviceOf(app, QueueJob);

      await service.updateQueue({ retryLimit: 4 });
      await expect(service.getQueue()).resolves.toMatchObject({
        retryLimit: 4,
      });
      await service.supervise();
      await expect(service.getQueueStats()).resolves.toEqual(expect.any(Array));

      await service.deleteQueue();
      await expect(service.getQueue()).resolves.toBeNull();
    });

    it("exposes pg-boss spies", async () => {
      const SpiedJob = createJob<Item>("api-spy");

      @Injectable()
      class Handler {
        @SpiedJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {}
      }

      const app = await build({
        jobs: [SpiedJob],
        providers: [Handler],
        options: { __test__enableSpies: true },
      });
      const service = serviceOf(app, SpiedJob);
      const spy = service.getSpy();

      const id = await service.send({ index: 7 });

      await expect(
        spy.waitForJobWithId(id ?? "", "completed"),
      ).resolves.toMatchObject({ data: { index: 7 } });
    });
  });

  describe("module defaults", () => {
    it("fills in what a job leaves out and updates existing queues", async () => {
      const DefaultedJob = createJob<Item>("defaults-applied", {
        retentionSeconds: 7_200,
      });
      const OwnJob = createJob<Item>("defaults-overridden", { retryLimit: 5 });
      const defaultQueueOptions = {
        retryLimit: 0,
        retentionSeconds: 3_600,
        deleteAfterSeconds: 60,
      };

      const first = await build({
        jobs: [DefaultedJob, OwnJob],
        options: { defaultQueueOptions },
      });
      const boss = first.get(PgBoss);
      await expect(boss.getQueue("defaults-applied")).resolves.toMatchObject({
        retryLimit: 0,
        retentionSeconds: 7_200,
        deleteAfterSeconds: 60,
      });
      await expect(boss.getQueue("defaults-overridden")).resolves.toMatchObject(
        {
          retryLimit: 5,
        },
      );
      await close(first);

      const second = await build({
        jobs: [DefaultedJob],
        options: {
          defaultQueueOptions: { ...defaultQueueOptions, retryLimit: 1 },
        },
      });
      await expect(
        second.get(PgBoss).getQueue("defaults-applied"),
      ).resolves.toMatchObject({ retryLimit: 1 });
    });

    it("leaves queues alone on an instance without defaults", async () => {
      const SharedJob = createJob<Item>("defaults-shared");

      const worker = await build({
        jobs: [SharedJob],
        options: { defaultQueueOptions: { retryLimit: 0 } },
      });
      const sender = await build({
        jobs: [SharedJob],
        options: { disableWorkers: true },
      });

      await expect(
        sender.get(PgBoss).getQueue("defaults-shared"),
      ).resolves.toMatchObject({ retryLimit: 0 });
      await close(worker);
    });

    it("shows effective work options, with the handler winning", async () => {
      const ScannedJob = createJob<Item>("defaults-scanned");

      @Injectable()
      class Handler {
        @ScannedJob.Handle({ localConcurrency: 2 })
        async handle(_job: PgBossJob<Item>) {}
      }

      const app = await build({
        providers: [Handler],
        options: {
          defaultWorkOptions: { localConcurrency: 9, ...POLL },
        },
      });

      const scanned = app
        .get(HandlerScannerService)
        .getJobHandlers()
        .find(({ metadata }) => metadata.jobName === "defaults-scanned");

      expect(scanned?.metadata.workOptions).toEqual({
        localConcurrency: 2,
        ...POLL,
      });
    });

    it("reports the real capacity of a draining handler, including from defaults", async () => {
      const OwnJob = createJob<Item>("capacity-own", { retryLimit: 0 });
      const DefaultedJob = createJob<Item>("capacity-defaulted", {
        retryLimit: 0,
      });

      @Injectable()
      class Handler {
        @OwnJob.Handle({ ...POLL, drain: true, localConcurrency: 4 })
        async own(_job: PgBossJob<Item>) {}

        @DefaultedJob.Handle(POLL)
        async defaulted(_job: PgBossJob<Item>) {}
      }

      const app = await build({
        providers: [Handler],
        options: { defaultWorkOptions: { drain: true, localConcurrency: 3 } },
      });
      const running = app.get(PgBoss).getWipData();

      for (const { metadata } of app
        .get(HandlerScannerService)
        .getJobHandlers()) {
        const workers = running.filter(({ name }) => name === metadata.jobName);
        expect(workers).toHaveLength(
          metadata.workOptions.localConcurrency ?? 1,
        );
      }
    });

    it("fails startup on stopOptions.destroy and on per-job options", async () => {
      await expect(
        build({ options: { stopOptions: { destroy: true } as object } }),
      ).rejects.toThrow("destroy (use close)");
      await expect(
        build({ options: { defaultWorkOptions: { batchSize: 2 } as object } }),
      ).rejects.toThrow("defaultWorkOptions cannot set batchSize");
    });
  });

  describe("options that reach pg-boss", () => {
    it("heartbeats while a handler runs, and aborts job.signal when the claim is lost", async () => {
      const BeatingJob = createJob<Item>("options-heartbeat", {
        retryLimit: 0,
        heartbeatSeconds: 10,
      });

      @Injectable()
      class Handler {
        heartbeats: (Date | null)[] = [];
        heartbeatSeconds: number | null = null;
        aborted = false;

        constructor(
          @BeatingJob.Inject() private readonly service: JobService<Item>,
        ) {}

        @BeatingJob.Handle({ ...POLL, heartbeatRefreshSeconds: 1 })
        async handle(job: PgBossJob<Item>) {
          this.heartbeatSeconds = job.heartbeatSeconds;
          const heartbeatOn = async () =>
            (await this.service.findJobs({ id: job.id }))[0]?.heartbeatOn ??
            null;

          this.heartbeats.push(await heartbeatOn());
          await sleep(1_500);
          this.heartbeats.push(await heartbeatOn());

          await this.service.cancel(job.id);
          await waitFor(() => job.signal.aborted, 5_000);
          this.aborted = job.signal.aborted;
        }
      }

      const app = await build({ jobs: [BeatingJob], providers: [Handler] });
      await serviceOf(app, BeatingJob).send({ index: 1 });
      const handler = app.get(Handler);

      await waitFor(() => handler.aborted);

      expect(handler.heartbeatSeconds).toBe(10);
      const [before, after] = handler.heartbeats;
      expect(after?.getTime()).toBeGreaterThan(before?.getTime() ?? Infinity);
    });

    it("still fails a job at expireInSeconds while its heartbeat is healthy", async () => {
      const OutlivingJob = createJob<Item>("options-heartbeat-expiry", {
        retryLimit: 0,
        expireInSeconds: 1,
        heartbeatSeconds: 10,
      });

      @Injectable()
      class Handler {
        @OutlivingJob.Handle({ ...POLL, heartbeatRefreshSeconds: 0.2 })
        async handle(_job: PgBossJob<Item>) {
          await sleep(2_500);
        }
      }

      const app = await build({ jobs: [OutlivingJob], providers: [Handler] });
      const service = serviceOf(app, OutlivingJob);
      await service.send({ index: 1 });

      await waitFor(
        async () => (await service.findJobs())[0]?.state === "failed",
        10_000,
      );
    });

    it("applies declared schedules and subscriptions with createQueues: false", async () => {
      const Event = createEvent<Item>("external-event");
      const ExternalJob = createJob<Item>("options-external", {
        schedules: [{ cron: "0 0 * * *", data: { index: 1 } }],
        subscribeTo: [Event],
      });

      const first = await build({ options: { disableWorkers: true } });
      await first.get(PgBoss).createQueue("options-external");
      await close(first);

      const app = await build({
        jobs: [ExternalJob],
        options: { createQueues: false },
      });
      const service = serviceOf(app, ExternalJob);

      await expect(service.getSchedules()).resolves.toHaveLength(1);
      await app.get(PubSubService).publish(Event, { index: 2 });
      await expect(service.findJobs()).resolves.toHaveLength(1);
    });

    it("holds back a group with an active job through groupConcurrency", async () => {
      const GroupJob = createJob<Item>("options-group", { retryLimit: 0 });

      @Injectable()
      class Handler {
        readonly seen: number[] = [];

        @GroupJob.Handle({ ...POLL, groupConcurrency: 1 })
        async handle(job: PgBossJob<Item>) {
          this.seen.push(job.data.index);
        }
      }

      const sender = await build({
        jobs: [GroupJob],
        options: { disableWorkers: true },
      });
      const service = serviceOf(sender, GroupJob);
      await service.send({ index: 0 }, { group: { id: "busy" } });
      const [busyJob] = await service.fetch();
      await service.send({ index: 1 }, { group: { id: "busy" } });
      await service.send({ index: 2 }, { group: { id: "free" } });

      const app = await build({ jobs: [GroupJob], providers: [Handler] });
      const handler = app.get(Handler);

      await waitFor(() => handler.seen.includes(2));
      await sleep(1_000);
      expect(handler.seen).toEqual([2]);

      await service.complete(busyJob?.id ?? "");
      await waitFor(() => handler.seen.includes(1));
    });

    it("limits a group locally with localGroupConcurrency", async () => {
      const LocalGroupJob = createJob<Item>("options-local-group", {
        retryLimit: 0,
      });

      @Injectable()
      class Handler {
        running = 0;
        peak = 0;
        finished = 0;

        @LocalGroupJob.Handle({
          ...POLL,
          localConcurrency: 3,
          localGroupConcurrency: 1,
        })
        async handle(_job: PgBossJob<Item>) {
          this.running++;
          this.peak = Math.max(this.peak, this.running);
          await sleep(100);
          this.running--;
          this.finished++;
        }
      }

      const app = await build({ jobs: [LocalGroupJob], providers: [Handler] });
      await serviceOf(app, LocalGroupJob).insert(
        items(3).map(({ data }) => ({ data, group: { id: "tenant" } })),
      );

      await waitFor(() => app.get(Handler).finished === 3);

      expect(app.get(Handler).peak).toBe(1);
    });

    it("wakes a worker through notify and useListenNotify", async () => {
      const NotifiedJob = createJob<Item>("options-notify", {
        retryLimit: 0,
        notify: true,
      });

      @Injectable()
      class Handler {
        seen = 0;

        @NotifiedJob.Handle({
          pollingIntervalSeconds: 60,
          notifyPollingIntervalSeconds: 60,
        })
        async handle(_job: PgBossJob<Item>) {
          this.seen++;
        }
      }

      const app = await build({
        jobs: [NotifiedJob],
        providers: [Handler],
        options: { useListenNotify: true },
      });
      // Let the worker's first, empty poll pass, so only NOTIFY can wake it.
      await sleep(1_000);

      await serviceOf(app, NotifiedJob).send({ index: 1 });

      await waitFor(() => app.get(Handler).seen === 1, 10_000);
    });

    it("drains a backlog on a notify queue", async () => {
      const BacklogJob = createJob<Item>("options-notify-backlog", {
        retryLimit: 0,
        notify: true,
      });

      @Injectable()
      class Handler {
        seen = 0;

        @BacklogJob.Handle({
          pollingIntervalSeconds: 60,
          notifyPollingIntervalSeconds: 60,
          drain: true,
        })
        async handle(_job: PgBossJob<Item>) {
          this.seen++;
        }
      }

      const sender = await build({
        jobs: [BacklogJob],
        options: { disableWorkers: true },
      });
      await serviceOf(sender, BacklogJob).insert(items(20));
      const app = await build({
        jobs: [BacklogJob],
        providers: [Handler],
        options: { useListenNotify: true },
      });

      await waitFor(() => app.get(Handler).seen === 20, 10_000);
    });

    it("accepts openTelemetry settings", async () => {
      const TracedJob = createJob<Item>("options-telemetry", { retryLimit: 0 });

      @Injectable()
      class Handler {
        seen = 0;

        @TracedJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.seen++;
        }
      }

      const app = await build({
        jobs: [TracedJob],
        providers: [Handler],
        options: { openTelemetry: { enabled: false } },
      });
      await serviceOf(app, TracedJob).send({ index: 1 });

      await waitFor(() => app.get(Handler).seen === 1);
    });

    it("gives a partitioned queue its own table", async () => {
      const PartitionedJob = createJob<Item>("options-partition", {
        retryLimit: 0,
        partition: true,
        policy: "short",
      });

      @Injectable()
      class Handler {
        seen = 0;

        @PartitionedJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.seen++;
        }
      }

      const app = await build({ jobs: [PartitionedJob], providers: [Handler] });
      const service = serviceOf(app, PartitionedJob);

      await expect(service.getQueue()).resolves.toMatchObject({
        partition: true,
        policy: "short",
      });
      await service.send({ index: 1 });

      await waitFor(() => app.get(Handler).seen === 1);
    });

    it("sends a dead letter back with redrive", async () => {
      const DeadJob = createJob<Item>("options-dead", { retryLimit: 0 });
      const LiveJob = createJob<Item>("options-live", {
        retryLimit: 0,
        deadLetter: DeadJob,
      });

      @Injectable()
      class Handler {
        healthy = false;
        succeeded = 0;

        @LiveJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          if (!this.healthy) {
            throw new Error("not yet");
          }
          this.succeeded++;
        }
      }

      const app = await build({
        jobs: [LiveJob, DeadJob],
        providers: [Handler],
      });
      const dead = serviceOf(app, DeadJob);
      await serviceOf(app, LiveJob).send({ index: 1 });

      await waitFor(async () => (await dead.findJobs()).length === 1);

      await expect(dead.previewRedrive()).resolves.toMatchObject({ total: 1 });
      app.get(Handler).healthy = true;
      await expect(dead.redrive()).resolves.toBe(1);

      await waitFor(() => app.get(Handler).succeeded === 1);
    });

    it("reports what holds up a key_strict_fifo queue", async () => {
      const FifoJob = createJob<Item>("options-fifo", {
        policy: "key_strict_fifo",
        retryLimit: 0,
      });
      const app = await build({ jobs: [FifoJob] });
      const service = serviceOf(app, FifoJob);

      await service.send({ index: 1 }, { singletonKey: "stuck" });
      await service.send({ index: 2 }, { singletonKey: "stuck" });
      await service.send({ index: 3 }, { singletonKey: "free" });

      const [head] = await service.fetch();
      await service.fail(head?.id ?? "");

      await expect(service.getBlockedKeys()).resolves.toEqual(["stuck"]);
      await expect(service.fetch()).resolves.toMatchObject([
        { data: { index: 3 } },
      ]);
    });

    it("treats jobs without a key as duplicates on an exclusive queue", async () => {
      const ExclusiveJob = createJob<Item>("options-exclusive", {
        policy: "exclusive",
      });
      const app = await build({ jobs: [ExclusiveJob] });
      const service = serviceOf(app, ExclusiveJob);

      await expect(service.send({ index: 1 })).resolves.toEqual(
        expect.any(String),
      );
      await expect(service.send({ index: 2 })).resolves.toBeNull();
    });
  });

  describe("declared schedules", () => {
    it("only removes schedules on instances that run them", async () => {
      const newer = createJob<Item>("schedules-sender", {
        schedules: [
          { cron: "0 0 * * *", data: { index: 1 }, options: { key: "old" } },
          { cron: "0 1 * * *", data: { index: 2 }, options: { key: "new" } },
        ],
      });
      const older = createJob<Item>("schedules-sender", {
        schedules: [
          { cron: "0 2 * * *", data: { index: 1 }, options: { key: "old" } },
        ],
      });
      await close(await build({ jobs: [newer] }));

      const warn = vi.spyOn(Logger.prototype, "warn");
      const sender = await build({
        jobs: [older],
        options: { disableWorkers: true },
      });
      await expect(storedSchedulesOn(sender)).resolves.toEqual([
        ["new", "0 1 * * *"],
        ["old", "0 2 * * *"],
      ]);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('"new"'));
      warn.mockRestore();
      await close(sender);

      const scheduler = await build({
        jobs: [older],
        options: { disableWorkers: true, schedule: true },
      });
      await expect(storedSchedulesOn(scheduler)).resolves.toEqual([
        ["old", "0 2 * * *"],
      ]);
    });

    it("adds, updates and removes to match the declaration", async () => {
      const first = await build({
        jobs: [
          withSchedules([
            { cron: "0 0 * * *", data: { index: 1 }, options: { key: "keep" } },
            { cron: "0 1 * * *", data: { index: 2 }, options: { key: "drop" } },
          ]),
        ],
      });
      const boss = first.get(PgBoss);
      const keysOf = async () =>
        (await boss.getSchedules("schedules-synced")).map(({ key }) => key);
      await expect(keysOf()).resolves.toEqual(["drop", "keep"]);
      await close(first);

      const second = await build({
        jobs: [
          withSchedules([
            { cron: "0 5 * * *", data: { index: 1 }, options: { key: "keep" } },
          ]),
        ],
      });
      const secondBoss = second.get(PgBoss);
      await expect(
        secondBoss.getSchedules("schedules-synced"),
      ).resolves.toMatchObject([{ key: "keep", cron: "0 5 * * *" }]);
      await close(second);

      // Leaving `schedules` out means this job does not manage them.
      const third = await build({
        jobs: [createJob<Item>("schedules-synced")],
      });
      await expect(
        third.get(PgBoss).getSchedules("schedules-synced"),
      ).resolves.toHaveLength(1);
      await close(third);

      const fourth = await build({ jobs: [withSchedules([])] });
      await expect(
        fourth.get(PgBoss).getSchedules("schedules-synced"),
      ).resolves.toHaveLength(0);
    });

    it("does not rewrite an unchanged schedule", async () => {
      const declared = withSchedules([
        {
          cron: "0 0 * * *",
          data: { index: 1 },
          options: { key: "same", tz: "Europe/Amsterdam", missed: "once" },
        },
      ]);
      const first = await build({ jobs: [declared] });
      const before = await first
        .get(PgBoss)
        .getSchedule("schedules-synced", "same");
      await close(first);

      const second = await build({ jobs: [declared] });
      const after = await second
        .get(PgBoss)
        .getSchedule("schedules-synced", "same");

      expect(after?.updatedOn).toEqual(before?.updatedOn);
      expect(after?.timezone).toBe("Europe/Amsterdam");
    });

    it("converges when several instances start together", async () => {
      const declared = createJob<Item>("schedules-together", {
        schedules: [
          { cron: "0 0 * * *", data: { index: 1 }, options: { key: "one" } },
          { cron: "0 1 * * *", data: { index: 2 }, options: { key: "two" } },
        ],
      });

      const [first] = await Promise.all([
        build({ jobs: [declared] }),
        build({ jobs: [declared] }),
        build({ jobs: [declared] }),
      ]);

      await expect(
        first.get(PgBoss).getSchedules("schedules-together"),
      ).resolves.toHaveLength(2);
    });

    it("stores RRULE schedules and missed-run policies", async () => {
      const RuleJob = createJob<Item>("schedules-rrule", {
        schedules: [
          {
            cron: "FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17",
            data: { index: 1 },
            options: { tz: "Europe/Amsterdam", missed: "once" },
          },
        ],
      });

      const app = await build({ jobs: [RuleJob] });
      const schedule = await serviceOf(app, RuleJob).getSchedule();

      expect(schedule).toMatchObject({
        kind: "rrule",
        timezone: "Europe/Amsterdam",
        options: { missed: "once" },
      });
    });

    it("rejects two schedules with the same key", async () => {
      const Duplicate = createJob<Item>("schedules-duplicate", {
        schedules: [
          { cron: "0 0 * * *", data: { index: 1 } },
          { cron: "0 1 * * *", data: { index: 2 } },
        ],
      });

      await expect(build({ jobs: [Duplicate] })).rejects.toThrow(
        'two schedules with the key ""',
      );
    });

    it("actually sends the job when it comes due", async () => {
      const EveryMinuteJob = createJob<Item>("schedules-firing", {
        retryLimit: 0,
        schedules: [{ cron: "* * * * *", data: { index: 42 } }],
      });

      @Injectable()
      class Handler {
        readonly seen: Item[] = [];

        @EveryMinuteJob.Handle(POLL)
        async handle(job: PgBossJob<Item>) {
          this.seen.push(job.data);
        }
      }

      const app = await build({
        jobs: [EveryMinuteJob],
        providers: [Handler],
        options: {
          cronMonitorIntervalSeconds: 1,
          cronWorkerIntervalSeconds: 1,
        },
      });

      await waitFor(() => app.get(Handler).seen.length > 0, 85_000);

      expect(app.get(Handler).seen[0]).toEqual({ index: 42 });
    }, 90_000);
  });

  describe("pub/sub", () => {
    interface OrderPlaced {
      orderId: string;
    }
    const OrderPlacedEvent = createEvent<OrderPlaced>("order-placed");

    it("delivers a published event to every subscribed job", async () => {
      const EmailJob = createJob<OrderPlaced>("pubsub-email", {
        retryLimit: 0,
        subscribeTo: [OrderPlacedEvent],
      });
      const AuditJob = createJob<OrderPlaced>("pubsub-audit", {
        retryLimit: 0,
        subscribeTo: [OrderPlacedEvent],
      });

      @Injectable()
      class Handlers {
        readonly seen: string[] = [];

        @EmailJob.Handle(POLL)
        async email(job: PgBossJob<OrderPlaced>) {
          this.seen.push(`email:${job.data.orderId}`);
        }

        @AuditJob.Handle(POLL)
        async audit(job: PgBossJob<OrderPlaced>) {
          this.seen.push(`audit:${job.data.orderId}`);
        }
      }

      const app = await build({ providers: [Handlers] });
      const pubSub = app.get(PubSubService);

      await pubSub.publish(OrderPlacedEvent, { orderId: "a1" });

      await waitFor(() => app.get(Handlers).seen.length === 2);
      expect(app.get(Handlers).seen).toEqual(
        expect.arrayContaining(["email:a1", "audit:a1"]),
      );

      await pubSub.unsubscribe(OrderPlacedEvent, AuditJob);
      await pubSub.publish(OrderPlacedEvent, { orderId: "a2" });
      await waitFor(() => app.get(Handlers).seen.includes("email:a2"));
      await sleep(1_000);
      expect(app.get(Handlers).seen).not.toContain("audit:a2");
    });

    it("subscribes at runtime", async () => {
      const LateEvent = createEvent<OrderPlaced>("order-late");
      const LateJob = createJob<OrderPlaced>("pubsub-late");
      const app = await build({ jobs: [LateJob] });
      const pubSub = app.get(PubSubService);

      await pubSub.subscribe(LateEvent, LateJob);
      await pubSub.publish(LateEvent, { orderId: "b1" });

      await expect(serviceOf(app, LateJob).findJobs()).resolves.toMatchObject([
        { data: { orderId: "b1" } },
      ]);
    });

    it("checks payloads at compile time", async () => {
      const app = await build({});
      const pubSub = app.get(PubSubService);
      const typeChecks = () => {
        // @ts-expect-error the event's payload needs an orderId
        void pubSub.publish(OrderPlacedEvent, { wrong: true });
        const StatusEvent = createEvent<{ status: "pending" | "done" }>(
          "pubsub-status",
        );
        void pubSub.publish(StatusEvent, { status: "pending" });
        // @ts-expect-error "typo" is not a status
        void pubSub.publish(StatusEvent, { status: "typo" });
        createJob<{ other: number }>("pubsub-wrong", {
          // @ts-expect-error this job's data does not match the event's payload
          subscribeTo: [OrderPlacedEvent],
        });
      };
      expect(typeChecks).toBeTypeOf("function");
    });
  });

  describe("@OnPgBossEvent", () => {
    it("calls provider methods on lifecycle events, surviving a listener that throws", async () => {
      const BacklogJob = createJob<Item>("events-backlog", {
        warningQueueSize: 1,
      });

      @Injectable()
      class Listener {
        readonly warnings: Warning[] = [];
        stopped = 0;

        @OnPgBossEvent("warning")
        onWarning(warning: Warning) {
          this.warnings.push(warning);
        }

        @OnPgBossEvent("warning")
        async alwaysFails() {
          throw new Error("listener failure");
        }

        @OnPgBossEvent("warning")
        async rejectsWithNothing() {
          return Promise.reject();
        }

        @OnPgBossEvent("stopped")
        onStopped() {
          this.stopped++;
        }
      }

      const app = await build({
        jobs: [BacklogJob],
        providers: [Listener],
        options: { disableWorkers: true },
      });
      const listener = app.get(Listener);
      const service = serviceOf(app, BacklogJob);
      await service.insert(items(3));

      await waitFor(async () => {
        await service.supervise();
        return listener.warnings.some(({ data }) =>
          JSON.stringify(data).includes("events-backlog"),
        );
      });

      await close(app);
      expect(listener.stopped).toBe(1);
    });

    it("checks the listener's arguments at compile time", () => {
      expect(WrongListener).toBeTypeOf("function");
    });
  });

  it("delivers transactional work with the metadata shape it declares", async () => {
    const CombinedJob = createJob<Item>("combined-transactional", {
      retryLimit: 3,
    });

    @Injectable()
    class Handler {
      retryLimit: number | undefined;

      @CombinedJob.Handle({
        ...POLL,
        includeMetadata: true,
        transactional: true,
      })
      async handle(job: JobWithMetadata<Item>) {
        this.retryLimit = job.retryLimit;
      }
    }

    const app = await build({ jobs: [CombinedJob], providers: [Handler] });
    await serviceOf(app, CombinedJob).send({ index: 1 });

    await waitFor(() => app.get(Handler).retryLimit === 3);
  });
});
