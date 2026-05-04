import { Logger } from "@nestjs/common";
import { delay, retryWhen, scan, type Observable } from "rxjs";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";

const logger = new Logger("PGBossModule");

export function getJobToken(jobName: string): string {
  return `JobService(${jobName})`;
}

/**
 * When `disableWorkers` is on, default `noSupervisor` and `noScheduling` to
 * `true` so the PGBoss instance does no internal polling. Explicit values
 * (including `false`) are preserved so callers can opt back in to being the
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
    noSupervisor: Object.hasOwn(options, "noSupervisor")
      ? options.noSupervisor
      : true,
    noScheduling: Object.hasOwn(options, "noScheduling")
      ? options.noScheduling
      : true,
  };
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
            if (toRetry && !toRetry(error)) {
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
