import {
  FactoryProvider,
  Inject,
  Injectable,
  SetMetadata,
} from "@nestjs/common";
import {
  PgBoss,
  type CommandResponse,
  type CompleteOptions,
  type ConnectionOptions,
  type Db,
  type DependencyRef,
  type FetchOptions,
  type FindJobsOptions,
  type InsertOptions,
  type Job as PgBossJob,
  type JobAttempt,
  type JobInsert,
  type JobSpyInterface,
  type JobWithMetadata,
  type ListOptions,
  type OffWorkOptions,
  type QueueResult,
  type QueueStats,
  type QueueStatsOptions,
  type RedriveOptions,
  type RedrivePreview,
  type RedrivePreviewOptions,
  type Schedule,
  type ScheduleOptions,
  type SendOptions,
  type SuperviseOptions,
  type UpdateOptions,
  type UpdateQueueOptions,
  type UpdateResponse,
  type UpsertResponse,
  type WorkHandler as PgBossWorkHandler,
  type WorkHandlerFor,
  type WorkOptions,
} from "pg-boss";
import type { PgBossEvent } from "./events";
import { HandlerMetadata } from "./interfaces/handler-metadata.interface";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";
import type { Jsonified } from "./jsonified";
import { MODULE_OPTIONS_TOKEN } from "./pg-boss.module-definition";
import { PG_BOSS_JOB_METADATA } from "./pg-boss.constants";
import { ensureQueue, type JobQueueOptions, type JobSchedule } from "./queues";
import { getJobToken, stopAfterFailedStartup } from "./utils";
import { registerWorkers, type HandleOptions } from "./workers";

/** A job to act on: its id, or the attempt `fetch()` or a handler received. */
export type JobReference = string | string[] | JobAttempt | JobAttempt[];

/**
 * pg-boss's job and queue operations, bound to one queue and typed to the job's
 * data. Data going in is `JobData`. Data coming back is `Jsonified<JobData>`,
 * because it has been through the database as JSON.
 */
@Injectable()
export class JobService<JobData extends object> {
  /** A draining `work()` is several pg-boss workers, known by the first one's id. */
  private readonly workerGroups = new Map<string, string[]>();

  constructor(
    private readonly name: string,
    private readonly pgBoss: PgBoss,
  ) {}

  /**
   * The pg-boss queue name this service sends to. Exposed so callers that need the queue
   * name (e.g. to look it up in another query) can read it off the injected job service
   * instead of importing the name constant separately.
   */
  get queueName(): string {
    return this.name;
  }

  // Creating jobs

  async send(data: JobData, options?: SendOptions): Promise<string | null> {
    return this.pgBoss.send(this.name, data, options);
  }

  async sendAfter(
    data: JobData,
    options: SendOptions | null,
    date: Date | string | number,
  ): Promise<string | null> {
    // sendAfter has three overloads for all date variants we accept
    return this.pgBoss.sendAfter(this.name, data, options, date as any);
  }

  async sendThrottled(
    data: JobData,
    options: SendOptions | null,
    seconds: number,
    key?: string,
  ): Promise<string | null> {
    return this.pgBoss.sendThrottled(this.name, data, options, seconds, key);
  }

  async sendDebounced(
    data: JobData,
    options: SendOptions | null,
    seconds: number,
    key?: string,
  ): Promise<string | null> {
    return this.pgBoss.sendDebounced(this.name, data, options, seconds, key);
  }

  /** Resolves to the job ids only with `{ returnId: true }`, otherwise to `null`. */
  async insert(
    jobs: JobInsert<JobData>[],
    options?: InsertOptions,
  ): Promise<string[] | null> {
    return this.pgBoss.insert(this.name, jobs, options);
  }

  /**
   * Changes a job that has not started yet, found by `id` or `singletonKey`. New
   * data replaces the old rather than merging with it. Pass `undefined` to change
   * only the options.
   */
  async update(
    data: JobData | null | undefined,
    options?: UpdateOptions,
  ): Promise<UpdateResponse> {
    return this.pgBoss.update(this.name, data, options);
  }

  /** Like `update`, but creates the job when nothing matches. */
  async upsert(
    data: JobData | null | undefined,
    options?: UpdateOptions,
  ): Promise<UpsertResponse> {
    return this.pgBoss.upsert(this.name, data, options);
  }

  // Working jobs by hand. `@Job.Handle()` is the usual way.

  /**
   * Registers a worker at runtime. Unlike `@Job.Handle()`, the handler always
   * receives an array, as with pg-boss's `work()`. Stop it with `offWork`, using
   * the id this resolves to.
   */
  async work<const Options extends HandleOptions>(
    options: Options,
    handler: WorkHandlerFor<Options & WorkOptions, Jsonified<JobData>>,
  ): Promise<string> {
    const workerIds = await registerWorkers(
      this.pgBoss,
      this.name,
      options,
      handler as PgBossWorkHandler<any>,
    );
    const [firstId] = workerIds as [string];
    this.workerGroups.set(firstId, workerIds);
    return firstId;
  }

  /**
   * Stops this queue's workers, or with `id`, the ones `work()` registered under
   * it. Pass `{ wait: false }` from inside a handler, because waiting there would
   * wait for that handler itself.
   */
  async offWork(options?: OffWorkOptions): Promise<void> {
    // pg-boss only defaults `wait` when it receives no options at all.
    const stopOptions = { wait: true, ...options };
    const { id: groupId } = stopOptions;
    const group = groupId ? this.workerGroups.get(groupId) : undefined;
    if (!groupId || !group) {
      await this.pgBoss.offWork(this.name, stopOptions);
      // Without `wait`, the groups stay so a later waiting call can still find them.
      if (!groupId && stopOptions.wait) {
        this.workerGroups.clear();
      }
      return;
    }

    await Promise.all(
      group.map((id) => this.pgBoss.offWork(this.name, { ...stopOptions, id })),
    );
    if (stopOptions.wait) {
      this.workerGroups.delete(groupId);
    }
  }

  async fetch(
    options: FetchOptions & { includeMetadata: true },
  ): Promise<JobWithMetadata<Jsonified<JobData>>[]>;
  async fetch(options?: FetchOptions): Promise<PgBossJob<Jsonified<JobData>>[]>;
  async fetch(options?: FetchOptions): Promise<any[]> {
    return this.pgBoss.fetch(this.name, options);
  }

  async complete(
    job: JobReference,
    output?: object | null,
    options?: CompleteOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.complete(this.name, job, output, options);
  }

  async fail(
    job: JobReference,
    output?: object | null,
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.fail(this.name, job, output, options);
  }

  /** Refreshes a job's heartbeat. */
  async touch(
    job: JobReference,
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.touch(this.name, job, options);
  }

  // Managing jobs

  async findJobs(
    options?: FindJobsOptions,
  ): Promise<JobWithMetadata<Jsonified<JobData>>[]> {
    return this.pgBoss.findJobs(this.name, options);
  }

  async cancel(
    job: JobReference,
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.cancel(this.name, job, options);
  }

  /** Puts cancelled jobs back in the queue. */
  async resume(
    id: string | string[],
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.resume(this.name, id, options);
  }

  /** Gives failed jobs another try. */
  async retry(
    id: string | string[],
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.retry(this.name, id, options);
  }

  async deleteJob(
    job: JobReference,
    options?: ConnectionOptions,
  ): Promise<CommandResponse> {
    return this.pgBoss.deleteJob(this.name, job, options);
  }

  /** Call on a dead letter queue to send its jobs back where they came from. */
  async redrive(options?: RedriveOptions): Promise<number> {
    return this.pgBoss.redrive(this.name, options);
  }

  async previewRedrive(
    options?: RedrivePreviewOptions,
  ): Promise<RedrivePreview> {
    return this.pgBoss.previewRedrive(this.name, options);
  }

  async getDependencies(
    id: string,
    options?: ConnectionOptions & ListOptions<DependencyRef>,
  ): Promise<DependencyRef[]> {
    return this.pgBoss.getDependencies(this.name, id, options);
  }

  async getDependents(
    id: string,
    options?: ConnectionOptions & ListOptions<DependencyRef>,
  ): Promise<DependencyRef[]> {
    return this.pgBoss.getDependents(this.name, id, options);
  }

  // Schedules

  /** Add or replace a schedule at runtime. `schedules` on `createJob` declares them. */
  async schedule(cron: string, data: JobData, options?: ScheduleOptions) {
    return this.pgBoss.schedule(this.name, cron, data, options);
  }

  async unschedule(key?: string, options?: ConnectionOptions) {
    await this.pgBoss.unschedule(this.name, key, options);
  }

  async getSchedule(key?: string): Promise<Schedule | null> {
    return this.pgBoss.getSchedule(this.name, key);
  }

  async getSchedules(
    key?: string,
    options?: ListOptions<Pick<Schedule, "name" | "key">>,
  ): Promise<Schedule[]> {
    return this.pgBoss.getSchedules(this.name, key, options);
  }

  // The queue

  async getQueue(): Promise<QueueResult | null> {
    return this.pgBoss.getQueue(this.name);
  }

  async getQueueStats(options?: QueueStatsOptions): Promise<QueueStats[]> {
    return this.pgBoss.getQueueStats(this.name, options);
  }

  /** For runtime changes. Options on `createJob` are applied at every startup. */
  async updateQueue(options: UpdateQueueOptions): Promise<void> {
    return this.pgBoss.updateQueue(this.name, options);
  }

  async deleteQueue(): Promise<void> {
    return this.pgBoss.deleteQueue(this.name);
  }

  async deleteQueuedJobs(): Promise<number> {
    return this.pgBoss.deleteQueuedJobs(this.name);
  }

  async deleteStoredJobs(): Promise<number> {
    return this.pgBoss.deleteStoredJobs(this.name);
  }

  async deleteAllJobs(): Promise<number | null> {
    return this.pgBoss.deleteAllJobs(this.name);
  }

  /** The keys holding up a `key_strict_fifo` queue. */
  async getBlockedKeys(options?: ListOptions): Promise<string[]> {
    return this.pgBoss.getBlockedKeys(this.name, options);
  }

  async supervise(options?: SuperviseOptions): Promise<void> {
    return this.pgBoss.supervise(this.name, options);
  }

  /** For tests. Requires `__test__enableSpies: true` on the module. */
  getSpy(): JobSpyInterface<Jsonified<JobData>> {
    return this.pgBoss.getSpy<Jsonified<JobData>>(this.name);
  }
}

export interface WorkHandler<ReqData> {
  (job: PgBossJob<ReqData>): Promise<any>;
}

export interface WorkHandlerBatch<ReqData> {
  (jobs: PgBossJob<ReqData>[]): Promise<any>;
}

/**
 * The job a handler receives when it is not batching, shaped by the `Handle()`
 * options. pg-boss always delivers an array, and the module unwraps it.
 */
type SingleJobHandler<JobData, Options> = (
  job: Options extends { includeMetadata: true }
    ? JobWithMetadata<JobData>
    : PgBossJob<JobData>,
  ...transaction: Options extends { transactional: true } ? [tx: Db] : []
) => Promise<any>;

/**
 * True for options typed so widely, such as a variable of type `WorkOptions`,
 * that only the runtime value decides between one job and an array.
 */
type MayReceiveArray<Options> = "batchSize" extends keyof Options
  ? true
  : "perJobResults" extends keyof Options
    ? Options["perJobResults" & keyof Options] extends false | undefined
      ? false
      : true
    : false;

/** Shown in the compiler error when the options hide the handler's job shape. */
type PassShapeOptionsLiterally =
  "Pass batchSize and perJobResults to Handle() as literal options, so the handler's job shape is known at compile time";

/**
 * `batchSize`, even `batchSize: 1`, opts in to the array. So does
 * `perJobResults`, because the handler must answer with one result per job.
 */
type HandlerFor<JobData, Options> = Options extends
  { batchSize: number } | { perJobResults: true }
  ? WorkHandlerFor<Options & WorkOptions, JobData>
  : MayReceiveArray<Options> extends true
    ? PassShapeOptionsLiterally
    : SingleJobHandler<JobData, Options>;

type AllKeys<Options> = Options extends unknown ? keyof Options : never;

/**
 * Resolves to `unknown` for valid options, which leaves them unchanged in the
 * intersection on `Handle()`. Otherwise it maps a key that is not a `Handle()`
 * option to `never`, so that a typo or a removed option fails even next to valid
 * options. A generic parameter on its own gets no excess property check.
 */
type RejectUnknown<Options> = [AllKeys<Options>] extends [keyof HandleOptions]
  ? [Options] extends [HandleOptions]
    ? unknown
    : InvalidOptions<Options>
  : InvalidOptions<Options>;

type InvalidOptions<Options> = {
  [Key in keyof Options]: Key extends keyof HandleOptions
    ? HandleOptions[Key]
    : never;
};

interface HandleDecorator<JobData extends object> {
  // No `extends HandleOptions`: failing a constraint would widen `Options` to that
  // type, which would add a misleading error about the handler's job shape.
  <const Options extends object = {}>(
    options?: Options & RejectUnknown<Options>,
  ): <Method extends HandlerFor<Jsonified<JobData>, Options>>(
    target: object,
    propertyKey: string | symbol,
    descriptor: TypedPropertyDescriptor<Method>,
  ) => void;
}

/** What `createJob` was given as the data type of a job. */
export type JobDataOf<Declared extends Job> =
  Declared extends Job<infer JobData> ? JobData : never;

/**
 * What a handler of this job receives: `@SomeJob.Handle() handle(job: JobOf<typeof
 * SomeJob>)`. It cannot drift from the job's data type, and the data is already
 * `Jsonified`. For batches, use `JobOf<typeof SomeJob>[]`.
 */
export type JobOf<Declared extends Job> = PgBossJob<
  Jsonified<JobDataOf<Declared>>
>;

/** Like `JobOf`, for a handler with `includeMetadata: true`. */
export type JobWithMetadataOf<Declared extends Job> = JobWithMetadata<
  Jsonified<JobDataOf<Declared>>
>;

export type CreateJobOptions<JobData extends object = any> = JobQueueOptions & {
  /**
   * The recurring runs of this job, kept in sync at every startup. When present,
   * this list owns the queue's schedules: a stored schedule whose key is not in
   * it is removed, so `schedules: []` removes them all. Leave it out to manage
   * schedules yourself with `JobService.schedule()`.
   */
  schedules?: JobSchedule<JobData>[];
  /** Pub/sub events that should send this job when published. */
  subscribeTo?: PgBossEvent<JobData>[];
};

export interface Job<JobData extends object = any> {
  queueName: string;
  queueOptions: JobQueueOptions;
  schedules?: JobSchedule<JobData>[];
  subscribeTo?: PgBossEvent<JobData>[];
  ServiceProvider: FactoryProvider<JobService<JobData>>;
  Inject: () => ParameterDecorator;
  Handle: HandleDecorator<JobData>;
}

export const createJob = <JobData extends object>(
  name: string,
  { schedules, subscribeTo, ...queueOptions }: CreateJobOptions<JobData> = {},
): Job<JobData> => {
  const token = getJobToken(name);
  const declaration = { queueName: name, queueOptions, schedules, subscribeTo };

  return {
    ...declaration,
    ServiceProvider: {
      provide: token,
      // Creating the queue here rather than in a lifecycle hook means it exists
      // before anything can inject this service and send, including constructors
      // and `onModuleInit` hooks, and on instances running with `disableWorkers`.
      useFactory: async (pgBoss: PgBoss, options?: PGBossModuleOptions) => {
        try {
          await ensureQueue(pgBoss, declaration, options);
        } catch (error) {
          await stopAfterFailedStartup(pgBoss);
          throw error;
        }
        return new JobService<JobData>(name, pgBoss);
      },
      inject: [PgBoss, { token: MODULE_OPTIONS_TOKEN, optional: true }],
    },
    Inject: () => Inject(token),
    Handle: ((options: HandleOptions = {}) =>
      SetMetadata<string, HandlerMetadata>(PG_BOSS_JOB_METADATA, {
        token,
        jobName: name,
        workOptions: options,
        queueOptions,
        schedules,
        subscribeTo,
      })) as unknown as HandleDecorator<JobData>,
  };
};
