import { AssertionError } from "node:assert";
import { Logger } from "@nestjs/common";
import { delay, retryWhen, scan, type Observable } from "rxjs";
import type { PgBoss, WorkHandler, WorkOptions } from "pg-boss";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";

const logger = new Logger("PGBossModule");

export function getJobToken(jobName: string): string {
  return `JobService(${jobName})`;
}

/**
 * When `disableWorkers` is on, default `supervise` and `schedule` to `false` so
 * the PgBoss instance does not run maintenance or cron. Explicit values
 * (including `true`) are preserved so callers can opt back in to being the
 * supervisor or scheduling leader.
 */
export function applyDisableWorkersDefaults(
  options: PGBossModuleOptions,
): PGBossModuleOptions {
  if (!options.disableWorkers) {
    return options;
  }

  return {
    ...options,
    supervise: Object.hasOwn(options, "supervise") ? options.supervise : false,
    schedule: Object.hasOwn(options, "schedule") ? options.schedule : false,
  };
}

/**
 * pg-boss always hands `work()` handlers an array. A `@Job.Handle()` handler
 * with no `batchSize` receives the single job instead. Any further arguments,
 * such as the `tx` of a transactional worker, are passed along. `perJobResults`
 * handlers keep the array, because they must answer with one result per job.
 */
export function toWorkHandler(
  handler: (...args: any[]) => Promise<any>,
  options: Pick<WorkOptions, "batchSize" | "perJobResults">,
): WorkHandler<unknown> {
  if (options.batchSize != null || options.perJobResults) {
    return handler;
  }

  return ([job]: unknown[], ...args: unknown[]) => handler(job, ...args);
}

const USE_QUEUE_OPTIONS = "declare it per queue on createJob";

/** Constructor options from older pg-boss versions, with what replaced them. */
export const REMOVED_MODULE_OPTIONS: Record<string, string> = {
  noSupervisor: "use supervise: false",
  noScheduling: "use schedule: false",
  // `retryDelay` is absent on purpose: it is this module's connection retry delay.
  retryLimit: USE_QUEUE_OPTIONS,
  retryBackoff: USE_QUEUE_OPTIONS,
  expireInSeconds: USE_QUEUE_OPTIONS,
  expireInMinutes: USE_QUEUE_OPTIONS,
  expireInHours: USE_QUEUE_OPTIONS,
  retentionSeconds: USE_QUEUE_OPTIONS,
  retentionMinutes: USE_QUEUE_OPTIONS,
  retentionHours: USE_QUEUE_OPTIONS,
  retentionDays: USE_QUEUE_OPTIONS,
  deleteAfterSeconds: USE_QUEUE_OPTIONS,
  deleteAfterMinutes: USE_QUEUE_OPTIONS,
  deleteAfterHours: USE_QUEUE_OPTIONS,
  deleteAfterDays: USE_QUEUE_OPTIONS,
  archiveCompletedAfterSeconds: USE_QUEUE_OPTIONS,
  archiveFailedAfterSeconds: USE_QUEUE_OPTIONS,
  maintenanceIntervalMinutes: "use maintenanceIntervalSeconds",
  clockMonitorIntervalMinutes: "use clockMonitorIntervalSeconds",
  monitorStateIntervalSeconds: "use getQueues()",
  monitorStateIntervalMinutes: "use getQueues()",
  newJobCheckInterval: "use pollingIntervalSeconds on Handle()",
  newJobCheckIntervalSeconds: "use pollingIntervalSeconds on Handle()",
  uuid: "there is no replacement",
};

/**
 * pg-boss ignores options it no longer has, so a config written for an older
 * version would keep running with different retries, retention or concurrency
 * and no error. Fail at startup instead.
 */
export const REMOVED_STOP_OPTIONS: Record<string, string> = {
  destroy: "use close",
};

/** Options that describe one queue or handler, so cannot be defaulted. */
const PER_JOB_ONLY = {
  defaultQueueOptions: ["policy", "partition", "deadLetter"],
  defaultWorkOptions: [
    "batchSize",
    "perJobResults",
    "includeMetadata",
    "transactional",
    "transactionTimeoutSeconds",
  ],
} as const;

/**
 * `forRootAsync` factories get no excess property checks, so what could otherwise
 * only be caught by the compiler is checked again when the module starts.
 */
export function assertValidModuleOptions(options: PGBossModuleOptions): void {
  assertNoRemovedOptions(
    options,
    REMOVED_MODULE_OPTIONS,
    "PGBossModule.forRoot()",
  );
  assertNoRemovedOptions(
    options.stopOptions ?? {},
    REMOVED_STOP_OPTIONS,
    "PGBossModule.forRoot() stopOptions",
  );

  for (const [option, perJobOnly] of Object.entries(PER_JOB_ONLY)) {
    const given = Object.keys(
      options[option as keyof typeof PER_JOB_ONLY] ?? {},
    );
    const invalid = given.filter((key) =>
      (perJobOnly as readonly string[]).includes(key),
    );
    if (invalid.length > 0) {
      throw new Error(
        `PGBossModule.forRoot() ${option} cannot set ${invalid.join(", ")}, ` +
          `which can only be set per job.`,
      );
    }
  }
}

export function assertNoRemovedOptions(
  options: object,
  removed: Readonly<Record<string, string>>,
  context: string,
): void {
  const problems = Object.keys(options)
    .filter((option) => Object.hasOwn(removed, option))
    .map((option) => `${option} (${removed[option]})`);

  if (problems.length > 0) {
    throw new Error(
      `${context} uses options pg-boss no longer supports: ${problems.join(", ")}`,
    );
  }
}

const bootstrappedInstances = new WeakSet<PgBoss>();

/** Called by the module once an instance's app has finished starting. */
export function markBootstrapped(boss: PgBoss): void {
  bootstrappedInstances.add(boss);
}

export function hasBootstrapped(boss: PgBoss): boolean {
  return bootstrappedInstances.has(boss);
}

/**
 * Nest runs no destroy hooks when startup fails, so a pg-boss instance that has
 * already started would keep its pool and timers open.
 */
export async function stopAfterFailedStartup(boss: PgBoss): Promise<void> {
  try {
    await boss.stop({ graceful: false });
  } catch {
    // The startup error is the one worth reporting.
  }
}

export function handleRetry(
  retryAttempts = 9,
  retryDelay = 3000,
  verboseRetryLog = false,
  toRetry?: (err: any) => boolean,
): <T>(source: Observable<T>) => Observable<T> {
  return <T>(source: Observable<T>) =>
    source.pipe(
      retryWhen((e) =>
        e.pipe(
          scan((errorCount, error: Error) => {
            // pg-boss throws an AssertionError for what fails the same way on
            // every try, such as a schema too old to migrate from.
            if (
              error instanceof AssertionError ||
              (toRetry && !toRetry(error))
            ) {
              throw error;
            }
            const verboseMessage = verboseRetryLog
              ? ` Message: ${error.message}.`
              : "";

            logger.error(
              `Unable to connect to the database.${verboseMessage} Retrying (${
                errorCount + 1
              })...`,
              error.stack,
            );
            if (errorCount + 1 >= retryAttempts) {
              throw error;
            }
            return errorCount + 1;
          }, 0),
          delay(retryDelay),
        ),
      ),
    );
}
