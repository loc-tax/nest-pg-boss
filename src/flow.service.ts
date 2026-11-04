import { Inject, Injectable, Optional } from "@nestjs/common";
import {
  PgBoss,
  type ConnectionOptions,
  type DependencyRef,
  type FlowJob,
  type ListOptions,
} from "pg-boss";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";
import type { Job } from "./job.service";
import { MODULE_OPTIONS_TOKEN } from "./pg-boss.module-definition";
import { ensureQueueOnce, readAllPages } from "./queues";

export interface FlowStep<Ref extends string = string> {
  job: Job;
  data: object;
  dependsOn?: readonly Ref[];
  options?: FlowJob["options"];
}

/** One job in a flow. `data` is checked against the job's payload type. */
export function step<JobData extends object, const Ref extends string = never>(
  job: Job<JobData>,
  data: NoInfer<JobData>,
  config: { dependsOn?: readonly Ref[]; options?: FlowJob["options"] } = {},
): FlowStep<Ref> {
  return { job, data, ...config };
}

/**
 * Runs jobs that depend on each other. A child stays blocked until every parent
 * has completed. Handlers need nothing special, because a flow's jobs reach them
 * like any other job.
 */
@Injectable()
export class FlowService {
  constructor(
    private readonly boss: PgBoss,
    @Optional()
    @Inject(MODULE_OPTIONS_TOKEN)
    private readonly options?: PGBossModuleOptions,
  ) {}

  /**
   * Inserts a whole flow atomically. The keys of `steps` are the refs, so
   * `dependsOn` and the returned ids are typed. Pass `{ db }` to take part in a
   * caller's transaction, so the flow is only there if that transaction commits.
   */
  async run<const Steps extends Record<string, FlowStep<any>>>(
    steps: Steps & { [Key in keyof Steps]: FlowStep<keyof Steps & string> },
    options?: ConnectionOptions,
  ): Promise<{ [Key in keyof Steps]: string }> {
    const entries = Object.entries(steps) as [string, FlowStep][];

    await this.ensureQueues(entries.map(([, { job }]) => job));

    const ids = await this.boss.flow(
      entries.map(([ref, { job, data, dependsOn, options: jobOptions }]) => {
        const flowJob: FlowJob = { ref, name: job.queueName, data };
        if (dependsOn) {
          flowJob.dependsOn = [...dependsOn];
        }
        if (jobOptions) {
          flowJob.options = jobOptions;
        }
        return flowJob;
      }),
      options,
    );
    return ids as { [Key in keyof Steps]: string };
  }

  /** Unblocks children whose parents have completed, without waiting for the timer. */
  async resolve(): Promise<void> {
    await this.boss.resolveFlow();
  }

  /** The parents a job is waiting on. */
  async getDependencies(
    job: Job,
    id: string,
    options?: ConnectionOptions & ListOptions<DependencyRef>,
  ): Promise<DependencyRef[]> {
    return this.boss.getDependencies(job.queueName, id, options);
  }

  /** The children waiting on a job. */
  async getDependents(
    job: Job,
    id: string,
    options?: ConnectionOptions & ListOptions<DependencyRef>,
  ): Promise<DependencyRef[]> {
    return this.boss.getDependents(job.queueName, id, options);
  }

  /**
   * Gives up on what depends on a job. pg-boss leaves the children of a failed
   * parent blocked, so that retrying the parent can still release them. When the
   * parent will not be retried, this cancels its children and their children in
   * turn, and returns how many it cancelled.
   */
  async cancelDependents(job: Job, id: string): Promise<number> {
    let cancelled = 0;
    // A descendant can be reached along several paths, and should be visited once.
    const visited = new Set<string>();
    const pending: DependencyRef[] = [{ name: job.queueName, id }];

    for (let parent = pending.pop(); parent; parent = pending.pop()) {
      const { name: parentQueue, id: parentId } = parent;
      // oxlint-disable-next-line no-await-in-loop -- one parent at a time keeps a large tree from exhausting the pool.
      const children = await readAllPages<DependencyRef, DependencyRef>(
        (options) => this.boss.getDependents(parentQueue, parentId, options),
        ({ name, id: childId }) => ({ name, id: childId }),
      );
      const unseen = children.filter((child) => {
        const key = `${child.name}/${child.id}`;
        return !visited.has(key) && visited.add(key);
      });
      pending.push(...unseen);

      for (const queueName of new Set(unseen.map(({ name }) => name))) {
        const ids = unseen
          .filter(({ name }) => name === queueName)
          .map((child) => child.id);
        // oxlint-disable-next-line no-await-in-loop -- see above.
        const response = await this.boss.cancel(queueName, ids);
        // pg-boss's type for this response declares no fields, but it reports `affected`.
        cancelled += (response as { affected?: number }).affected ?? 0;
      }
    }

    return cancelled;
  }

  /**
   * A flow can name jobs this module has not seen through `forJobs` or a handler.
   * Queues it set up at startup are not checked again, since a flow may run per
   * request.
   */
  private async ensureQueues(jobs: Job[]): Promise<void> {
    await Promise.all(
      jobs.map((job) => ensureQueueOnce(this.boss, job, this.options)),
    );
  }
}
