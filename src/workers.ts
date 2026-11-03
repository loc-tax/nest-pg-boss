import type { PgBoss, WorkHandler, WorkOptions } from "pg-boss";

/** The options of `@Job.Handle()`: pg-boss's work options plus `drain`. */
export type HandleOptions = Omit<
  WorkOptions,
  "priority" | "orderByCreatedOn"
> & {
  /**
   * Fetch the next job as soon as a handler returns, rather than waiting out the
   * polling interval, so a backlog is worked through quickly. Nothing wakes the
   * worker after an empty fetch or after its handler throws, so a failing dependency
   * does not burn through a backlog's retries by itself. It is not a guaranteed
   * pause: pg-boss's burst options, a NOTIFY or, with `localGroupConcurrency`, another
   * worker's success can still start the next fetch.
   *
   * Each job is still claimed, expired, heartbeated, retried and completed on its
   * own, so unlike `batchSize` this holds nothing back from other workers, and a
   * crash only reruns the job that was in flight.
   */
  drain?: boolean;
};

/** `Handle()` options that can be defaulted, as they leave its arguments alone. */
export type DefaultWorkOptions = Omit<
  HandleOptions,
  | "batchSize"
  | "perJobResults"
  | "includeMetadata"
  | "transactional"
  | "transactionTimeoutSeconds"
>;

/**
 * Registers a queue's workers and resolves to their ids. pg-boss wakes a single
 * worker through `notifyWorker`, and a worker notified while its handler runs
 * skips its delay and fetches again right after.
 *
 * A handler cannot tell which of a `localConcurrency` group it runs on, so a
 * draining group is registered as separate single workers. That way a success
 * wakes only the worker it ran on, and one whose handler threw keeps its delay.
 *
 * `localGroupConcurrency` is the exception, because pg-boss validates it against
 * the `localConcurrency` of one registration. Those workers stay together, and a
 * success wakes them all.
 */
export async function registerWorkers(
  boss: PgBoss,
  queueName: string,
  { drain, ...workOptions }: HandleOptions,
  handler: WorkHandler<any>,
): Promise<string[]> {
  const { localConcurrency = 1, localGroupConcurrency } = workOptions;

  // An invalid count goes to pg-boss, which rejects it with its own message.
  if (!drain || !Number.isInteger(localConcurrency) || localConcurrency < 1) {
    return [await boss.work(queueName, workOptions, handler)];
  }

  const runThenWake =
    (wake: () => void): WorkHandler<any> =>
    async (...args: unknown[]) => {
      const result = await (handler as (...all: unknown[]) => Promise<unknown>)(
        ...args,
      );
      wake();
      return result;
    };

  if (localGroupConcurrency != null) {
    let workId: string | undefined;
    const wakeGroup = () => {
      for (const worker of boss.getWipData()) {
        if (worker.workId === workId) {
          boss.notifyWorker(worker.id);
        }
      }
    };
    workId = await boss.work(queueName, workOptions, runThenWake(wakeGroup));
    return [workId];
  }

  const registerDrainingWorker = async () => {
    let workerId: string | undefined;
    const wakeSelf = () => {
      if (workerId) {
        boss.notifyWorker(workerId);
      }
    };
    workerId = await boss.work(
      queueName,
      { ...workOptions, localConcurrency: 1 },
      runThenWake(wakeSelf),
    );
    return workerId;
  };

  return Promise.all(
    Array.from({ length: localConcurrency }, registerDrainingWorker),
  );
}
