import type { StopOptions, ConstructorOptions, Warning } from "pg-boss";
import type { DefaultQueueOptions } from "../queues";
import type { DefaultWorkOptions } from "../workers";

// TODO: Figure at "name", "application_name", "instanceName"

export type PGBossModuleOptions = {
  /**
   * Number of times to retry connecting
   * Default: 10
   */
  retryAttempts?: number;
  /**
   * Delay between connection retry attempts (ms)
   * Default: 3000
   */
  retryDelay?: number;
  /**
   * Function that determines whether the module should
   * attempt to connect upon failure.
   *
   * @param err error that was thrown
   * @returns whether to retry connection or not
   */
  toRetry?: (err: any) => boolean;
  /**
   * If `true`, will show verbose error messages on each connection retry.
   */
  verboseRetryLog?: boolean;
  /**
   * This exposes the `on('error', ...` from pg-boss.
   * @see https://pgboss.io/api/events
   */
  onError: (err: Error) => void;
  /**
   * This exposes the `on('warning', ...` from pg-boss, which reports problems
   * that do not stop it, such as a growing queue backlog, slow queries or a
   * handler that outruns its timeout. Without this option they go unheard.
   * @see https://pgboss.io/api/events
   */
  onWarning?: (warning: Warning) => void;
  /**
   * If `true`, registered `@Job.Handle()` job handlers will not be wired up
   * on `onApplicationBootstrap`. The PgBoss instance is still created and
   * `start()` is still called so this process can `send()` jobs and run
   * migrations. Queues are still created, see `createQueues`.
   *
   * Note: enabling this will also default pg-boss's `supervise` and `schedule`
   * to `false`, so the instance does not run maintenance or cron. To opt back in
   * to either, pass `supervise: true` or `schedule: true` explicitly, which is
   * useful when you want a non-consumer process to still act as the
   * maintenance or cron leader.
   */
  disableWorkers?: boolean;
  /**
   * Create the queue of every job given to `forJobs` or used with
   * `@Job.Handle()` during startup, and apply the options declared on
   * `createJob`. This also applies to instances with `disableWorkers`, because
   * pg-boss rejects sending to a queue that does not exist.
   *
   * Set to `false` if queues are managed outside the application, for example
   * when this instance's database role may not change the queue table. Schedules
   * and subscriptions declared on a job are still applied.
   * Default: true
   */
  createQueues?: boolean;
  /**
   * Queue options for every job, with options on `createJob` winning. Like those,
   * they are applied at each startup, so an existing queue is updated to match.
   * Give instances that share queues the same defaults, or the last one to start
   * wins. `policy`, `partition` and `deadLetter` can only be set per job.
   */
  defaultQueueOptions?: DefaultQueueOptions;
  /**
   * `@Job.Handle()` options for every handler on this instance, with options on
   * `Handle()` winning. Options that change the handler's arguments can only be
   * set per handler: `batchSize`, `perJobResults`, `includeMetadata`,
   * `transactional` and `transactionTimeoutSeconds`.
   */
  defaultWorkOptions?: DefaultWorkOptions;
  /**
   * pg-boss stop options to run.
   * @see https://pgboss.io/api/ops#stop-options
   */
  stopOptions?: StopOptions;
} & ConstructorOptions;
