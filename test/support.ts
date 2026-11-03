import { setTimeout as sleep } from "node:timers/promises";
import type { INestApplication, Provider } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { PGBossModule, type Job, type JobService } from "../src";
import type { PGBossModuleOptions } from "../src/interfaces/pg-boss-options.interface";

export const POLL = { pollingIntervalSeconds: 0.5 } as const;

export interface Item {
  index: number;
}

export const items = (count: number) =>
  Array.from({ length: count }, (_unused, index) => ({ data: { index } }));

export const serviceOf = <JobData extends object>(
  app: INestApplication,
  job: Job<JobData>,
) => app.get<JobService<JobData>>(job.ServiceProvider.provide as string);

export const waitFor = async (
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 30_000,
) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // oxlint-disable-next-line no-await-in-loop -- sequential poll; running the checks in parallel would defeat the wait.
    if (await condition()) {
      return;
    }
    // oxlint-disable-next-line no-await-in-loop -- see above.
    await sleep(50);
  }
  throw new Error("Timed out waiting for the condition");
};

export interface AppOptions {
  jobs?: Job[];
  providers?: Provider[];
  options?: Partial<PGBossModuleOptions>;
}

/** One Postgres container per spec file, and apps that close after each test. */
export const useApps = () => {
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

  const build = async ({
    jobs = [],
    providers = [],
    options = {},
  }: AppOptions): Promise<INestApplication> => {
    const moduleFixture = await Test.createTestingModule({
      imports: [
        PGBossModule.forRoot({
          connectionString: postgres.getConnectionUri(),
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

  const close = async (app: INestApplication) => {
    openApps.splice(openApps.indexOf(app), 1);
    await app.close();
  };

  return { build, close };
};
