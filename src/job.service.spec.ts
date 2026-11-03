import "reflect-metadata";
import type {
  Db,
  Job as PgBossJob,
  JobResult,
  JobWithMetadata,
  PgBoss,
} from "pg-boss";
import { PG_BOSS_JOB_METADATA } from "./pg-boss.constants";
import {
  createJob,
  JobService,
  type JobOf,
  type JobWithMetadataOf,
} from "./job.service";
import type { Jsonified } from "./jsonified";
import type { DefaultWorkOptions, HandleOptions } from "./workers";

interface Payload {
  id: string;
}

const TypedJob = createJob<Payload>("typed");

interface WithDate {
  at: Date;
  nested: { when: Date; label?: string };
  list: Date[];
}

const DatedJob = createJob<WithDate>("dated");

interface OddValues {
  list: (string | undefined)[];
  method: () => void;
}

export const jsonifiedArraysHoldNull: Jsonified<OddValues> = {
  list: ["kept", null],
};

interface MaybeDropped {
  label: string | symbol;
  anything: any;
}

// `label` can disappear, so a handler has to allow for that.
export const readLabel = (value: Jsonified<MaybeDropped>): string =>
  // @ts-expect-error `label` may be missing after the trip through JSON
  value.label.toUpperCase() + String(value.anything.free);

export const withoutLabel: Jsonified<MaybeDropped> = { anything: 1 };

export const runtimeWorkers = (service: JobService<Payload>) => [
  service.work({ transactional: true }, async (_jobs, _tx: Db) => {}),
  service.work({ includeMetadata: true }, async (jobs) => jobs[0]?.retryLimit),
  // @ts-expect-error a perJobResults handler has to answer with results
  service.work({ perJobResults: true }, async () => {}),
];

// One import is enough: these name exactly what the handlers receive.
export class DerivedShapes {
  @DatedJob.Handle()
  async single(job: JobOf<typeof DatedJob>) {
    const at: string = job.data.at;
    return at;
  }

  @DatedJob.Handle({ batchSize: 2 })
  async batch(jobs: JobOf<typeof DatedJob>[]) {
    const first: string | undefined = jobs[0]?.data.nested.when;
    return first;
  }

  @DatedJob.Handle({ includeMetadata: true })
  async metadata(job: JobWithMetadataOf<typeof DatedJob>) {
    const retryLimit: number = job.retryLimit;
    return retryLimit;
  }

  // @ts-expect-error the data is a string by now, so the wrong field type is caught
  @TypedJob.Handle()
  async wrongJob(_job: JobOf<typeof DatedJob>) {}
}

// Handlers see JSON, so a Date arrives as a string, and senders still pass a Date.
export class JsonShapes {
  constructor(private readonly service: JobService<WithDate>) {}

  @DatedJob.Handle()
  async asJson(
    _job: PgBossJob<{
      at: string;
      nested: { when: string; label?: string };
      list: string[];
    }>,
  ) {
    await this.service.send({
      at: new Date(),
      nested: { when: new Date() },
      list: [],
    });
  }

  // @ts-expect-error the Date has become a string by the time a handler runs
  @DatedJob.Handle()
  async asDate(_job: PgBossJob<WithDate>) {}
}

const widenedOptions: HandleOptions = { perJobResults: true };
const flag = Boolean(process.env.PER_JOB_RESULTS);
// Options chosen at runtime stay valid while typed so they cannot change the shape.
const shapeNeutralOptions: DefaultWorkOptions = flag
  ? { drain: true }
  : { localConcurrency: 2 };

// Compiled by `pnpm typecheck` and never called. This pins the handler shape
// each set of `Handle()` options requires.
export class HandlerShapes {
  @TypedJob.Handle()
  async single(_job: PgBossJob<Payload>) {}

  @TypedJob.Handle({ localConcurrency: 2 })
  async concurrent(_job: PgBossJob<Payload>): Promise<string> {
    return "output";
  }

  @TypedJob.Handle({ includeMetadata: true })
  async singleWithMetadata(_job: JobWithMetadata<Payload>) {}

  @TypedJob.Handle({ transactional: true })
  async singleTransactional(_job: PgBossJob<Payload>, _tx: Db) {}

  @TypedJob.Handle({ batchSize: 5 })
  async batch(_jobs: PgBossJob<Payload>[]) {}

  @TypedJob.Handle({ batchSize: 1 })
  async batchOfOne(_jobs: PgBossJob<Payload>[]) {}

  @TypedJob.Handle({ batchSize: 5, includeMetadata: true })
  async batchWithMetadata(_jobs: JobWithMetadata<Payload>[]) {}

  @TypedJob.Handle({ perJobResults: true })
  async perJob(_jobs: PgBossJob<Payload>[]): Promise<JobResult[]> {
    return [];
  }

  // @ts-expect-error without batchSize the handler gets one job, not an array
  @TypedJob.Handle()
  async wrongSingle(_jobs: PgBossJob<Payload>[]) {}

  @TypedJob.Handle({ drain: true, localConcurrency: 2 })
  async draining(_job: PgBossJob<Payload>) {}

  @TypedJob.Handle()
  async ignoresTheJob() {}

  @TypedJob.Handle(shapeNeutralOptions)
  async runtimeChosen(_job: PgBossJob<Payload>) {}

  // @ts-expect-error a typo next to a valid option is still rejected
  @TypedJob.Handle({ localConcurrency: 2, localConcurency: 3 })
  async typo(_job: PgBossJob<Payload>) {}

  // @ts-expect-error pg-boss removed teamSize
  @TypedJob.Handle({ teamSize: 10 })
  async removedAlone(_job: PgBossJob<Payload>) {}

  // @ts-expect-error pg-boss removed teamSize, also rejected next to a valid option
  @TypedJob.Handle({ batchSize: 5, teamSize: 10 })
  async removedWithBatch(_jobs: PgBossJob<Payload>[]) {}

  // @ts-expect-error pg-boss ignores priority since 12.30 and will reject it
  @TypedJob.Handle({ priority: false })
  async deprecated(_job: PgBossJob<Payload>) {}

  // @ts-expect-error a widened type hides whether the handler gets an array
  @TypedJob.Handle(widenedOptions)
  async widened(_job: PgBossJob<Payload>) {}

  // @ts-expect-error a widened flag hides whether the handler gets an array
  @TypedJob.Handle({ perJobResults: flag })
  async widenedFlag(_job: PgBossJob<Payload>) {}

  @TypedJob.Handle({ perJobResults: false })
  async notPerJob(_job: PgBossJob<Payload>) {}

  // @ts-expect-error with batchSize the handler gets an array, not one job
  @TypedJob.Handle({ batchSize: 5 })
  async wrongBatch(_job: PgBossJob<Payload>) {}
}

describe("JobService", () => {
  it("exposes the pg-boss queue name via queueName", () => {
    const service = new JobService<{ id: string }>(
      "StartLibraryFileAnalysis",
      {} as PgBoss,
    );
    expect(service.queueName).toBe("StartLibraryFileAnalysis");
  });

  it("passes the queue name first to insert", async () => {
    const insert = vi.fn(async () => null);
    const service = new JobService<Payload>("queue", {
      insert,
    } as unknown as PgBoss);
    const jobs = [{ data: { id: "one" } }];

    await service.insert(jobs);

    expect(insert).toHaveBeenCalledWith("queue", jobs, undefined);
  });
});

const fakeBoss = () =>
  ({
    createQueue: vi.fn(async () => {}),
    getQueue: vi.fn(async () => null),
    updateQueue: vi.fn(async () => {}),
  }) as unknown as PgBoss;

describe("createJob", () => {
  it("creates the queue before handing out the service", async () => {
    const job = createJob<Payload>("declared", { retryLimit: 0 });
    const boss = fakeBoss();

    const service = await job.ServiceProvider.useFactory(boss, {});

    expect(boss.createQueue).toHaveBeenCalledWith("declared", {
      retryLimit: 0,
    });
    expect(service).toBeInstanceOf(JobService);
  });

  it("does not touch queues with createQueues: false", async () => {
    const boss = fakeBoss();

    await TypedJob.ServiceProvider.useFactory(boss, { createQueues: false });

    expect(boss.createQueue).not.toHaveBeenCalled();
  });

  it("applies the module's default queue options, with the job winning", async () => {
    const job = createJob<Payload>("defaulted", { retryLimit: 3 });
    const boss = fakeBoss();

    await job.ServiceProvider.useFactory(boss, {
      defaultQueueOptions: { retryLimit: 0, deleteAfterSeconds: 60 },
    });

    expect(boss.createQueue).toHaveBeenCalledWith("defaulted", {
      retryLimit: 3,
      deleteAfterSeconds: 60,
    });
  });

  it("stops pg-boss when the queue cannot be set up", async () => {
    const boss = {
      createQueue: vi.fn(async () => {
        throw new Error("no permission");
      }),
      stop: vi.fn(async () => {}),
    } as unknown as PgBoss;

    await expect(TypedJob.ServiceProvider.useFactory(boss, {})).rejects.toThrow(
      "no permission",
    );
    expect(boss.stop).toHaveBeenCalledWith({ graceful: false });
  });

  it("records the queue options on the handler metadata", () => {
    const job = createJob<Payload>("declared", { policy: "short" });
    class Handler {
      @job.Handle()
      async handle(_job: PgBossJob<Payload>) {}
    }

    expect(
      Reflect.getMetadata(PG_BOSS_JOB_METADATA, Handler.prototype.handle),
    ).toMatchObject({ jobName: "declared", queueOptions: { policy: "short" } });
  });
});
