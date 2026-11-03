import { setTimeout as sleep } from "node:timers/promises";
import {
  Injectable,
  type INestApplication,
  type OnModuleInit,
  type Provider,
} from "@nestjs/common";
import { Test } from "@nestjs/testing";
import {
  PgBoss,
  type Db,
  type Job as PgBossJob,
  type JobResult,
  type JobWithMetadata,
  type Warning,
} from "pg-boss";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import {
  HandlerScannerService,
  PGBossModule,
  createJob,
  type Job,
  type JobService,
} from "../src";
import type { PGBossModuleOptions } from "../src/interfaces/pg-boss-options.interface";

interface FoobarJobData {
  foo: string;
  bar: boolean;
}

const FoobarJob = createJob<FoobarJobData>("foobar");

@Injectable()
class FoobarService {
  constructor(
    @FoobarJob.Inject()
    private readonly foobarJobService: JobService<FoobarJobData>,
  ) {}

  public readonly datastore: FoobarJobData[] = [];

  async sendJob() {
    await this.foobarJobService.send({ foo: "oof", bar: true }, {});
  }

  @FoobarJob.Handle({ pollingIntervalSeconds: 0.5 })
  async handleJob(job: PgBossJob<FoobarJobData>) {
    this.datastore.push(job.data);
  }
}

interface Item {
  index: number;
}

const items = (count: number) =>
  Array.from({ length: count }, (_unused, index) => ({ data: { index } }));

const POLL = { pollingIntervalSeconds: 0.5 } as const;

const serviceOf = <JobData extends object>(
  app: INestApplication,
  job: Job<JobData>,
) => app.get<JobService<JobData>>(job.ServiceProvider.provide as string);

const waitFor = async (condition: () => boolean | Promise<boolean>) => {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    // oxlint-disable-next-line no-await-in-loop -- sequential poll; running the checks in parallel would defeat the wait.
    if (await condition()) {
      return;
    }
    // oxlint-disable-next-line no-await-in-loop -- see above.
    await sleep(100);
  }
  throw new Error("Timed out waiting for the condition");
};

const jobStates = async (boss: PgBoss, queue: string) =>
  (await boss.findJobs(queue)).map(({ state }) => state);

describe("PGBossModule (e2e)", () => {
  let postgres: StartedPostgreSqlContainer;
  const openApps: INestApplication[] = [];

  beforeAll(async () => {
    postgres = await new PostgreSqlContainer("postgres:16-alpine").start();
  });

  afterEach(async () => {
    await Promise.all(openApps.splice(0).map((app) => app.close()));
  });

  afterAll(async () => {
    await postgres?.stop();
  });

  const buildApp = async ({
    jobs = [],
    providers = [],
    options = {},
  }: {
    jobs?: Job[];
    providers?: Provider[];
    options?: Partial<PGBossModuleOptions>;
  }): Promise<INestApplication> => {
    const moduleFixture = await Test.createTestingModule({
      imports: [
        PGBossModule.forRoot({
          host: postgres.getHost(),
          port: postgres.getPort(),
          database: postgres.getDatabase(),
          user: postgres.getUsername(),
          password: postgres.getPassword(),
          onError: (error) => {
            console.error(error);
          },
          retryAttempts: 1,
          ...options,
        }),
        PGBossModule.forJobs(jobs),
      ],
      providers,
    }).compile();

    const app = moduleFixture.createNestApplication();
    await app.init();
    openApps.push(app);
    return app;
  };

  describe("with workers enabled", () => {
    it("creates the queue and handles a job", async () => {
      const app = await buildApp({
        jobs: [FoobarJob],
        providers: [FoobarService],
      });
      const foobarService = app.get(FoobarService);

      await expect(app.get(PgBoss).getQueue("foobar")).resolves.toMatchObject({
        name: "foobar",
        policy: "standard",
      });

      await foobarService.sendJob();
      await waitFor(() => foobarService.datastore.length === 1);

      expect(foobarService.datastore[0]).toEqual({ foo: "oof", bar: true });
    });

    it("exposes the same pg-boss instance through getBossInstance", async () => {
      const app = await buildApp({});
      expect(app.get(PGBossModule).getBossInstance()).toBe(app.get(PgBoss));
    });
  });

  describe("handler shapes", () => {
    it("gives a handler without batchSize one job at a time", async () => {
      const SingleJob = createJob<Item>("handler-single");

      @Injectable()
      class SingleHandler {
        readonly received: unknown[] = [];

        @SingleJob.Handle(POLL)
        async handle(job: PgBossJob<Item>) {
          this.received.push(job);
          return { doubled: job.data.index * 2 };
        }
      }

      const app = await buildApp({
        jobs: [SingleJob],
        providers: [SingleHandler],
      });
      const handler = app.get(SingleHandler);
      await serviceOf(app, SingleJob).insert(items(3));

      await waitFor(() => handler.received.length === 3);

      for (const job of handler.received) {
        expect(Array.isArray(job)).toBe(false);
        expect(job).toMatchObject({ name: "handler-single" });
      }

      // The handler's return value still becomes the job's output.
      const boss = app.get(PgBoss);
      await waitFor(async () =>
        (await jobStates(boss, "handler-single")).every(
          (state) => state === "completed",
        ),
      );
      const outputs = (await boss.findJobs("handler-single")).map(
        ({ output }) => output,
      );
      expect(outputs).toContainEqual({ doubled: 4 });
    });

    it("gives a handler with batchSize an array of up to that many jobs", async () => {
      const BatchJob = createJob<Item>("handler-batch");

      @Injectable()
      class BatchHandler {
        readonly batches: PgBossJob<Item>[][] = [];

        @BatchJob.Handle({ ...POLL, batchSize: 5 })
        async handle(jobs: PgBossJob<Item>[]) {
          this.batches.push(jobs);
        }
      }

      const app = await buildApp({
        jobs: [BatchJob],
        providers: [BatchHandler],
      });
      const handler = app.get(BatchHandler);
      await serviceOf(app, BatchJob).insert(items(7));

      await waitFor(() => handler.batches.flat().length === 7);

      expect(handler.batches.map((batch) => batch.length)).toEqual([5, 2]);
    });

    it("gives a handler with batchSize: 1 an array", async () => {
      const BatchOfOneJob = createJob<Item>("handler-batch-of-one");

      @Injectable()
      class BatchOfOneHandler {
        readonly received: unknown[] = [];

        @BatchOfOneJob.Handle({ ...POLL, batchSize: 1 })
        async handle(jobs: PgBossJob<Item>[]) {
          this.received.push(jobs);
        }
      }

      const app = await buildApp({
        jobs: [BatchOfOneJob],
        providers: [BatchOfOneHandler],
      });
      const handler = app.get(BatchOfOneHandler);
      await serviceOf(app, BatchOfOneJob).send({ index: 1 }, {});

      await waitFor(() => handler.received.length === 1);

      expect(handler.received[0]).toHaveLength(1);
    });

    it("includes metadata on a single job", async () => {
      const MetadataJob = createJob<Item>("handler-metadata", {
        retryLimit: 4,
      });

      @Injectable()
      class MetadataHandler {
        readonly received: JobWithMetadata<Item>[] = [];

        @MetadataJob.Handle({ ...POLL, includeMetadata: true })
        async handle(job: JobWithMetadata<Item>) {
          this.received.push(job);
        }
      }

      const app = await buildApp({
        jobs: [MetadataJob],
        providers: [MetadataHandler],
      });
      const handler = app.get(MetadataHandler);
      await serviceOf(app, MetadataJob).send({ index: 1 }, {});

      await waitFor(() => handler.received.length === 1);

      expect(handler.received[0]).toMatchObject({
        state: "active",
        retryLimit: 4,
      });
    });

    it("passes the transaction after the single job of a transactional worker", async () => {
      const TransactionalJob = createJob<Item>("handler-transactional");

      @Injectable()
      class TransactionalHandler {
        readonly received: [PgBossJob<Item>, Db][] = [];

        @TransactionalJob.Handle({ ...POLL, transactional: true })
        async handle(job: PgBossJob<Item>, tx: Db) {
          this.received.push([job, tx]);
        }
      }

      const app = await buildApp({
        jobs: [TransactionalJob],
        providers: [TransactionalHandler],
      });
      const handler = app.get(TransactionalHandler);
      await serviceOf(app, TransactionalJob).send({ index: 1 }, {});

      await waitFor(() => handler.received.length === 1);

      const [job, tx] = handler.received[0] ?? [];
      expect(job).toMatchObject({ data: { index: 1 } });
      expect(typeof tx?.executeSql).toBe("function");
    });

    it("keeps the array for perJobResults without batchSize", async () => {
      const PerJobJob = createJob<Item>("handler-per-job");

      @Injectable()
      class PerJobHandler {
        readonly received: unknown[] = [];

        @PerJobJob.Handle({ ...POLL, perJobResults: true })
        async handle(jobs: PgBossJob<Item>[]): Promise<JobResult[]> {
          this.received.push(jobs);
          return jobs.map(({ id }) => ({ id, status: "completed" }));
        }
      }

      const app = await buildApp({
        jobs: [PerJobJob],
        providers: [PerJobHandler],
      });
      const handler = app.get(PerJobHandler);
      await serviceOf(app, PerJobJob).send({ index: 1 }, {});
      const boss = app.get(PgBoss);

      await waitFor(
        async () =>
          (await jobStates(boss, "handler-per-job"))[0] === "completed",
      );

      expect(handler.received[0]).toHaveLength(1);
    });

    it("runs up to localConcurrency handlers at once", async () => {
      const ConcurrentJob = createJob<Item>("handler-concurrent");

      @Injectable()
      class ConcurrentHandler {
        running = 0;
        peak = 0;
        done = 0;

        @ConcurrentJob.Handle({ ...POLL, localConcurrency: 3 })
        async handle(_job: PgBossJob<Item>) {
          this.running++;
          this.peak = Math.max(this.peak, this.running);
          await sleep(1_000);
          this.running--;
          this.done++;
        }
      }

      const app = await buildApp({
        jobs: [ConcurrentJob],
        providers: [ConcurrentHandler],
      });
      const handler = app.get(ConcurrentHandler);
      await serviceOf(app, ConcurrentJob).insert(items(6));

      await waitFor(() => handler.done === 6);

      expect(handler.peak).toBeGreaterThan(1);
      expect(handler.peak).toBeLessThanOrEqual(3);
    });
  });

  describe("queue creation", () => {
    it("applies the options declared on createJob, and retries inherit them", async () => {
      const DeclaredJob = createJob<Item>("queue-declared", {
        retryLimit: 7,
        retryDelay: 3,
        expireInSeconds: 120,
        retentionSeconds: 3_600,
        deleteAfterSeconds: 60,
      });

      const app = await buildApp({ jobs: [DeclaredJob] });
      const boss = app.get(PgBoss);

      await expect(boss.getQueue("queue-declared")).resolves.toMatchObject({
        retryLimit: 7,
        retryDelay: 3,
        expireInSeconds: 120,
        retentionSeconds: 3_600,
        deleteAfterSeconds: 60,
      });

      await serviceOf(app, DeclaredJob).send({ index: 1 }, {});
      const [job] = await boss.findJobs("queue-declared");
      expect(job?.retryLimit).toBe(7);
    });

    it("keeps pg-boss's default of two retries when none are declared", async () => {
      const DefaultJob = createJob<Item>("queue-default-retries");

      const app = await buildApp({ jobs: [DefaultJob] });

      await expect(
        app.get(PgBoss).getQueue("queue-default-retries"),
      ).resolves.toMatchObject({ retryLimit: 2 });
    });

    it("does not retry a failed job with retryLimit: 0", async () => {
      const NoRetryJob = createJob<Item>("queue-no-retry", { retryLimit: 0 });

      @Injectable()
      class FailingHandler {
        attempts = 0;

        @NoRetryJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.attempts++;
          throw new Error("always fails");
        }
      }

      const app = await buildApp({
        jobs: [NoRetryJob],
        providers: [FailingHandler],
      });
      const boss = app.get(PgBoss);
      await serviceOf(app, NoRetryJob).send({ index: 1 }, {});

      await waitFor(
        async () => (await jobStates(boss, "queue-no-retry"))[0] === "failed",
      );

      expect(app.get(FailingHandler).attempts).toBe(1);
    });

    it("creates queues before any provider can send from onModuleInit", async () => {
      const EarlyJob = createJob<Item>("queue-early-send");

      @Injectable()
      class EarlySender implements OnModuleInit {
        jobId: string | null = null;

        constructor(
          @EarlyJob.Inject()
          private readonly jobService: JobService<Item>,
        ) {}

        async onModuleInit() {
          this.jobId = await this.jobService.send({ index: 1 }, {});
        }
      }

      const app = await buildApp({
        jobs: [EarlyJob],
        providers: [EarlySender],
      });

      expect(app.get(EarlySender).jobId).toEqual(expect.any(String));
    });

    it("creates the queue of a job that only has a handler", async () => {
      const HandlerOnlyJob = createJob<Item>("queue-handler-only", {
        retryLimit: 1,
      });

      @Injectable()
      class HandlerOnly {
        @HandlerOnlyJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {}
      }

      const app = await buildApp({ providers: [HandlerOnly] });

      await expect(
        app.get(PgBoss).getQueue("queue-handler-only"),
      ).resolves.toMatchObject({ retryLimit: 1 });
    });

    it("copies a job that exhausts its retries to the dead letter queue", async () => {
      const DeadJob = createJob<Item>("queue-dead", { retryLimit: 0 });
      const MainJob = createJob<Item>("queue-main", {
        retryLimit: 0,
        deadLetter: DeadJob,
      });

      @Injectable()
      class Handlers {
        readonly dead: Item[] = [];

        @MainJob.Handle(POLL)
        async main(_job: PgBossJob<Item>) {
          throw new Error("always fails");
        }

        @DeadJob.Handle(POLL)
        async deadLetter(job: PgBossJob<Item>) {
          this.dead.push(job.data);
        }
      }

      // Listed so the main queue's factory may run first: the order must not matter.
      const app = await buildApp({
        jobs: [MainJob, DeadJob],
        providers: [Handlers],
      });
      await serviceOf(app, MainJob).send({ index: 9 }, {});

      await waitFor(() => app.get(Handlers).dead.length === 1);

      expect(app.get(Handlers).dead[0]).toEqual({ index: 9 });
    });

    it("updates changed options on restart and leaves undeclared ones alone", async () => {
      const first = createJob<Item>("queue-reconcile", {
        retryLimit: 1,
        retryDelay: 5,
      });
      const firstApp = await buildApp({ jobs: [first] });
      await firstApp.close();
      openApps.splice(openApps.indexOf(firstApp), 1);

      const second = createJob<Item>("queue-reconcile", { retryLimit: 6 });
      const app = await buildApp({ jobs: [second] });

      await expect(
        app.get(PgBoss).getQueue("queue-reconcile"),
      ).resolves.toMatchObject({ retryLimit: 6, retryDelay: 5 });
    });

    it("updates retryDelayMax on an existing queue through real validation", async () => {
      const first = createJob<Item>("queue-backoff", {
        retryBackoff: true,
        retryDelayMax: 30,
        expireInSeconds: undefined,
      });
      const firstApp = await buildApp({ jobs: [first] });
      await firstApp.close();
      openApps.splice(openApps.indexOf(firstApp), 1);

      const second = createJob<Item>("queue-backoff", {
        retryBackoff: true,
        retryDelayMax: 60,
      });
      const app = await buildApp({ jobs: [second] });

      await expect(
        app.get(PgBoss).getQueue("queue-backoff"),
      ).resolves.toMatchObject({ retryBackoff: true, retryDelayMax: 60 });
    });

    it("accepts retryDelay as the connection retry delay", async () => {
      const app = await buildApp({ options: { retryDelay: 100 } });
      expect(app.get(PgBoss)).toBeInstanceOf(PgBoss);
    });

    it("fails startup and stops pg-boss when the declared policy differs from the existing queue", async () => {
      const standard = createJob<Item>("queue-policy-conflict");
      const firstApp = await buildApp({ jobs: [standard] });
      await firstApp.close();
      openApps.splice(openApps.indexOf(firstApp), 1);

      const short = createJob<Item>("queue-policy-conflict", {
        policy: "short",
      });
      // A second queue being set up at the same time must not cause a stray failure.
      const neighbor = createJob<Item>("queue-policy-neighbor", {
        retryLimit: 1,
      });
      const stop = vi.spyOn(PgBoss.prototype, "stop");

      try {
        await expect(buildApp({ jobs: [short, neighbor] })).rejects.toThrow(
          "pg-boss cannot change policy",
        );
        expect(stop).toHaveBeenCalled();
      } finally {
        stop.mockRestore();
      }
    });

    it("stops pg-boss when a handler-only queue fails during bootstrap", async () => {
      const standard = createJob<Item>("queue-handler-conflict");
      const firstApp = await buildApp({ jobs: [standard] });
      await firstApp.close();
      openApps.splice(openApps.indexOf(firstApp), 1);

      const short = createJob<Item>("queue-handler-conflict", {
        policy: "short",
      });

      @Injectable()
      class ConflictingHandler {
        @short.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {}
      }
      const stop = vi.spyOn(PgBoss.prototype, "stop");

      try {
        await expect(
          buildApp({ providers: [ConflictingHandler] }),
        ).rejects.toThrow("pg-boss cannot change policy");
        expect(stop).toHaveBeenCalled();
      } finally {
        stop.mockRestore();
      }
    });

    it("rejects a sender with createQueues: false until the queue exists", async () => {
      const ExternalJob = createJob<Item>("queue-external");

      const app = await buildApp({
        jobs: [ExternalJob],
        options: { createQueues: false },
      });
      const boss = app.get(PgBoss);

      await expect(boss.getQueue("queue-external")).resolves.toBeNull();
      await expect(
        serviceOf(app, ExternalJob).send({ index: 1 }, {}),
      ).rejects.toThrow("does not exist");

      await boss.createQueue("queue-external");
      await expect(
        serviceOf(app, ExternalJob).send({ index: 1 }, {}),
      ).resolves.toEqual(expect.any(String));
    });
  });

  describe("queue policies", () => {
    it("short: holds one queued job per singleton key", async () => {
      const ShortJob = createJob<Item>("policy-short", { policy: "short" });
      const app = await buildApp({ jobs: [ShortJob] });
      const service = serviceOf(app, ShortJob);

      await expect(
        app.get(PgBoss).getQueue("policy-short"),
      ).resolves.toMatchObject({ policy: "short" });
      await expect(service.send({ index: 1 }, {})).resolves.toEqual(
        expect.any(String),
      );
      await expect(service.send({ index: 2 }, {})).resolves.toBeNull();
      await expect(
        service.send({ index: 3 }, { singletonKey: "other" }),
      ).resolves.toEqual(expect.any(String));
    });

    it("singleton: runs one job at a time across workers", async () => {
      const SingletonJob = createJob<Item>("policy-singleton", {
        policy: "singleton",
      });

      @Injectable()
      class SingletonHandler {
        running = 0;
        peak = 0;
        done = 0;

        @SingletonJob.Handle({ ...POLL, localConcurrency: 3 })
        async handle(_job: PgBossJob<Item>) {
          this.running++;
          this.peak = Math.max(this.peak, this.running);
          await sleep(300);
          this.running--;
          this.done++;
        }
      }

      const app = await buildApp({
        jobs: [SingletonJob],
        providers: [SingletonHandler],
      });
      const handler = app.get(SingletonHandler);
      await serviceOf(app, SingletonJob).insert(items(3));

      await waitFor(() => handler.done === 3);

      expect(handler.peak).toBe(1);
    });

    it("stately: holds one queued and one active job", async () => {
      const StatelyJob = createJob<Item>("policy-stately", {
        policy: "stately",
      });

      @Injectable()
      class StatelyHandler {
        started = 0;
        release: () => void = () => {};
        private readonly gate = new Promise<void>((resolve) => {
          this.release = resolve;
        });

        @StatelyJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.started++;
          await this.gate;
        }
      }

      const app = await buildApp({
        jobs: [StatelyJob],
        providers: [StatelyHandler],
      });
      const handler = app.get(StatelyHandler);
      const service = serviceOf(app, StatelyJob);

      await service.send({ index: 1 }, {});
      await waitFor(() => handler.started === 1);

      // One active, so one more may queue, and no further.
      await expect(service.send({ index: 2 }, {})).resolves.toEqual(
        expect.any(String),
      );
      await expect(service.send({ index: 3 }, {})).resolves.toBeNull();

      handler.release();
    });

    it("exclusive: holds one job, queued or active", async () => {
      const ExclusiveJob = createJob<Item>("policy-exclusive", {
        policy: "exclusive",
      });

      @Injectable()
      class ExclusiveHandler {
        started = 0;
        release: () => void = () => {};
        private readonly gate = new Promise<void>((resolve) => {
          this.release = resolve;
        });

        @ExclusiveJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.started++;
          await this.gate;
        }
      }

      const app = await buildApp({
        jobs: [ExclusiveJob],
        providers: [ExclusiveHandler],
      });
      const handler = app.get(ExclusiveHandler);
      const service = serviceOf(app, ExclusiveJob);

      await service.send({ index: 1 }, {});
      await expect(service.send({ index: 2 }, {})).resolves.toBeNull();

      await waitFor(() => handler.started === 1);
      await expect(service.send({ index: 3 }, {})).resolves.toBeNull();

      handler.release();
    });

    it("key_strict_fifo: requires a key and keeps order within it", async () => {
      const FifoJob = createJob<Item>("policy-fifo", {
        policy: "key_strict_fifo",
      });

      @Injectable()
      class FifoHandler {
        readonly seen: number[] = [];

        @FifoJob.Handle({ ...POLL, localConcurrency: 3 })
        async handle(job: PgBossJob<Item>) {
          await sleep(50);
          this.seen.push(job.data.index);
        }
      }

      const app = await buildApp({
        jobs: [FifoJob],
        providers: [FifoHandler],
      });
      const handler = app.get(FifoHandler);
      const service = serviceOf(app, FifoJob);

      await expect(service.send({ index: 0 }, {})).rejects.toThrow(
        /singletonKey/,
      );

      // One at a time: jobs inserted together share a creation time, which
      // leaves their order undefined.
      for (const { data } of items(5)) {
        // oxlint-disable-next-line no-await-in-loop -- the jobs must be created in order.
        await service.send(data, { singletonKey: "same-key" });
      }
      await waitFor(() => handler.seen.length === 5);

      expect(handler.seen).toEqual([0, 1, 2, 3, 4]);
    });
  });

  describe("with disableWorkers: true", () => {
    it("creates queues and allows send() without running handlers", async () => {
      const SendOnlyJob = createJob<Item>("disabled-send-only", {
        retryLimit: 3,
      });

      @Injectable()
      class NeverRuns {
        calls = 0;

        @SendOnlyJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {
          this.calls++;
        }
      }

      const app = await buildApp({
        jobs: [SendOnlyJob],
        providers: [NeverRuns],
        options: { disableWorkers: true },
      });
      const boss = app.get(PgBoss);

      await expect(boss.getQueue("disabled-send-only")).resolves.toMatchObject({
        retryLimit: 3,
      });

      await serviceOf(app, SendOnlyJob).send({ index: 1 }, {});

      // Give pg-boss enough time that any rogue worker would have picked it up.
      await sleep(2_000);

      expect(app.get(NeverRuns).calls).toBe(0);
      await expect(jobStates(boss, "disabled-send-only")).resolves.toEqual([
        "created",
      ]);
    });

    it("hands a job over to an instance with workers", async () => {
      const HandoverJob = createJob<Item>("disabled-handover", {
        policy: "standard",
      });

      const sender = await buildApp({
        jobs: [HandoverJob],
        options: { disableWorkers: true },
      });

      @Injectable()
      class Consumer {
        readonly received: Item[] = [];

        @HandoverJob.Handle(POLL)
        async handle(job: PgBossJob<Item>) {
          this.received.push(job.data);
        }
      }

      const consumer = await buildApp({
        jobs: [HandoverJob],
        providers: [Consumer],
      });
      await serviceOf(sender, HandoverJob).send({ index: 5 }, {});

      await waitFor(() => consumer.get(Consumer).received.length === 1);

      expect(consumer.get(Consumer).received).toEqual([{ index: 5 }]);
    });

    it("creates the queue of a job that only has a disabled handler", async () => {
      const DisabledHandlerJob = createJob<Item>("disabled-handler-only");

      @Injectable()
      class DisabledHandler {
        @DisabledHandlerJob.Handle(POLL)
        async handle(_job: PgBossJob<Item>) {}
      }

      const app = await buildApp({
        providers: [DisabledHandler],
        options: { disableWorkers: true },
      });

      await expect(
        app.get(PgBoss).getQueue("disabled-handler-only"),
      ).resolves.not.toBeNull();
    });

    it("disables supervision and scheduling by default", async () => {
      const app = await buildApp({
        options: { disableWorkers: true, instanceName: "disabled-defaults" },
      });

      const instances = await app.get(PgBoss).getInstances();
      const instance = instances.find(
        ({ name, live }) => name === "disabled-defaults" && live,
      );

      expect(instance).toMatchObject({ supervise: false, schedule: false });
    });

    it("preserves explicit supervise and schedule overrides", async () => {
      const app = await buildApp({
        options: {
          disableWorkers: true,
          instanceName: "disabled-overrides",
          supervise: true,
          schedule: true,
        },
      });

      const instances = await app.get(PgBoss).getInstances();
      const instance = instances.find(
        ({ name, live }) => name === "disabled-overrides" && live,
      );

      expect(instance).toMatchObject({ supervise: true, schedule: true });
    });

    it("can still manage schedules", async () => {
      const ScheduledJob = createJob<Item>("disabled-scheduled");
      const app = await buildApp({
        jobs: [ScheduledJob],
        options: { disableWorkers: true },
      });
      const boss = app.get(PgBoss);
      const service = serviceOf(app, ScheduledJob);

      await service.schedule("0 0 * * *", { index: 1 }, {});
      await expect(
        boss.getSchedules("disabled-scheduled"),
      ).resolves.toHaveLength(1);

      await service.unschedule();
      await expect(
        boss.getSchedules("disabled-scheduled"),
      ).resolves.toHaveLength(0);
    });
  });

  describe("onWarning", () => {
    it("receives pg-boss warnings such as a queue backlog", async () => {
      const BacklogJob = createJob<Item>("warning-backlog", {
        warningQueueSize: 1,
      });
      const warnings: Warning[] = [];

      const app = await buildApp({
        jobs: [BacklogJob],
        options: {
          disableWorkers: true,
          onWarning: (warning) => warnings.push(warning),
        },
      });
      await serviceOf(app, BacklogJob).insert(items(3));

      const backlogWarning = () =>
        warnings.find(({ data }) =>
          JSON.stringify(data).includes("warning-backlog"),
        );
      await waitFor(async () => {
        await app.get(PgBoss).supervise("warning-backlog");
        return backlogWarning() !== undefined;
      });

      expect(backlogWarning()?.message).toEqual(expect.any(String));
    });
  });

  describe("removed pg-boss options", () => {
    it("fails startup with a noSupervisor option", async () => {
      await expect(
        buildApp({ options: { noSupervisor: true } as object }),
      ).rejects.toThrow("noSupervisor (use supervise: false)");
    });
  });

  describe("HandlerScannerService", () => {
    it("exposes the work and queue options of handlers", async () => {
      const ScannedJob = createJob<Item>("scanned", { retryLimit: 0 });

      @Injectable()
      class Scanned {
        @ScannedJob.Handle({ ...POLL, localConcurrency: 2 })
        async handle(_job: PgBossJob<Item>) {}
      }

      const app = await buildApp({ providers: [Scanned] });

      const handler = app
        .get(HandlerScannerService)
        .getJobHandlers()
        .find(({ metadata }) => metadata.jobName === "scanned");

      expect(handler?.metadata.workOptions.localConcurrency).toBe(2);
      expect(handler?.metadata.queueOptions?.retryLimit).toBe(0);
    });
  });
});
