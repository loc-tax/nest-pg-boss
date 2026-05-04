import { setTimeout as sleep } from "node:timers/promises";
import { Injectable, type INestApplication } from "@nestjs/common";
import { Test, type TestingModule } from "@nestjs/testing";
import PGBoss, { type Job } from "pg-boss";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { PGBossModule, createJob, type JobService } from "../src";
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

  @FoobarJob.Handle()
  async handleJob(job: Job<FoobarJobData>) {
    this.datastore.push(job.data);
  }
}

describe("PGBossModule (e2e)", () => {
  let postgres: StartedPostgreSqlContainer;

  beforeAll(async () => {
    jest.setTimeout(60_000);
    postgres = await new PostgreSqlContainer("postgres:16-alpine").start();
  });

  afterAll(async () => {
    if (postgres) {
      // Even after calling `app.close()` pg-boss does not properly close the
      // job workers, throwing unexpected exceptions once the database is stopped.
      // After 10 seconds the remaining workers have all stopped.
      await sleep(10_000);
      await postgres.stop();
    }
  });

  const buildApp = async (
    overrides: Partial<PGBossModuleOptions> = {},
  ): Promise<INestApplication> => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
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
          disableWorkers: false,
          ...overrides,
        }),
        PGBossModule.forJobs([FoobarJob]),
      ],
      providers: [FoobarService],
    }).compile();

    const app = moduleFixture.createNestApplication();
    await app.init();
    return app;
  };

  describe("with workers enabled", () => {
    let app: INestApplication;
    let foobarService: FoobarService;

    beforeEach(async () => {
      app = await buildApp();
      foobarService = app.get<FoobarService>(FoobarService);
    });

    afterEach(async () => {
      await app.close();
    });

    it("handles a Job", async () => {
      await foobarService.sendJob();

      // Wait for processing
      let lastError: Error | null = null;

      for (let retry = 0; retry < 25; retry++) {
        try {
          expect(foobarService.datastore).toHaveLength(1);

          lastError = null;
          break;
        } catch (err) {
          lastError = err as Error;
          await sleep(2_000);
        }
      }

      expect(foobarService.datastore[0]).toEqual({ foo: "oof", bar: true });
      expect(lastError).toBeNull();
    });
  });

  describe("with disableWorkers: true", () => {
    let app: INestApplication;
    let foobarService: FoobarService;
    let boss: PGBoss;

    beforeEach(async () => {
      app = await buildApp({ disableWorkers: true });
      foobarService = app.get<FoobarService>(FoobarService);
      boss = app.get<PGBoss>(PGBoss);
    });

    afterEach(async () => {
      await app.close();
    });

    it("does not run user job handlers but still allows send()", async () => {
      await foobarService.sendJob();

      // Give pg-boss enough time that any rogue worker would have picked it up.
      await sleep(5_000);

      expect(foobarService.datastore).toHaveLength(0);

      const queueSize = await boss.getQueueSize("foobar");
      expect(queueSize).toBeGreaterThan(0);
    });

    it("disables internal supervisor and scheduling polling by default", () => {
      // pg-boss stores the resolved config on the instance; verifying it here
      // ensures the auto-coupling actually reached PGBoss.
      const config = (boss as unknown as { config: Record<string, unknown> })
        .config;
      expect(config.noSupervisor).toBe(true);
      expect(config.noScheduling).toBe(true);
    });
  });

  describe("with disableWorkers: true and explicit overrides", () => {
    let app: INestApplication;
    let boss: PGBoss;

    beforeEach(async () => {
      app = await buildApp({
        disableWorkers: true,
        noSupervisor: false,
        noScheduling: false,
      });
      boss = app.get<PGBoss>(PGBoss);
    });

    afterEach(async () => {
      await app.close();
    });

    it("preserves explicit noSupervisor/noScheduling: false", () => {
      const config = (boss as unknown as { config: Record<string, unknown> })
        .config;
      expect(config.noSupervisor).toBe(false);
      expect(config.noScheduling).toBe(false);
    });
  });
});
