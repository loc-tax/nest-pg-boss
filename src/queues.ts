import { isDeepStrictEqual } from "node:util";
import { Logger } from "@nestjs/common";
import type {
  PgBoss,
  Queue,
  Schedule,
  ScheduleOptions,
  UpdateQueueOptions,
} from "pg-boss";

const logger = new Logger("PGBossModule");

/**
 * Queue settings declared on `createJob`. These are the options of pg-boss's
 * `createQueue()`, except that `deadLetter` takes the job whose queue receives
 * failed jobs rather than a queue name. pg-boss rejects a dead letter queue
 * that does not exist yet, and holding the job lets that queue be created
 * first, with its own options.
 */
export type JobQueueOptions = Omit<Queue, "name" | "deadLetter"> & {
  deadLetter?: QueueDeclaration;
};

/**
 * Queue settings that can be defaulted across a whole module. `policy`,
 * `partition` and `deadLetter` describe one queue's purpose rather than a house
 * rule, and a default dead letter queue would also apply to itself.
 */
export type DefaultQueueOptions = Omit<
  JobQueueOptions,
  "policy" | "partition" | "deadLetter"
>;

/** One recurring job. `options` holds `key`, `tz`, `missed` and send options. */
export interface JobSchedule<JobData extends object = any> {
  /** A cron expression or an RFC 5545 recurrence rule. */
  cron: string;
  data: JobData;
  options?: ScheduleOptions;
}

export interface QueueDeclaration {
  queueName: string;
  queueOptions?: JobQueueOptions;
  /** Present means this list owns the queue's schedules, even when empty. */
  schedules?: JobSchedule[];
  subscribeTo?: { eventName: string }[];
}

/** The parts of the module options that decide how a queue is set up. */
export type QueueSetupOptions = {
  createQueues?: boolean;
  defaultQueueOptions?: DefaultQueueOptions;
};

/** pg-boss cannot change these once a queue exists. */
const IMMUTABLE_OPTIONS = ["policy", "partition"] as const;

/**
 * Applies a job's declaration: its queue, schedules and event subscriptions.
 *
 * `createQueue()` does nothing for a queue that already exists, so on its own a
 * changed declaration would never take effect. Options that are declared or
 * defaulted are therefore compared with the stored queue and updated when they
 * differ. Options that are neither are not touched.
 *
 * `createQueues: false` leaves the queue itself to whoever manages it. Schedules
 * and subscriptions are still applied, because they were explicitly declared.
 */
export async function ensureQueue(
  boss: PgBoss,
  { queueName, queueOptions = {}, schedules, subscribeTo }: QueueDeclaration,
  setup: QueueSetupOptions = {},
): Promise<void> {
  const { deadLetter, ...options } = queueOptions;
  if (deadLetter) {
    await ensureQueue(boss, deadLetter, setup);
  }

  if (setup.createQueues !== false) {
    // pg-boss's validators reject a key that is present but undefined.
    const declared = Object.fromEntries(
      Object.entries({
        ...setup.defaultQueueOptions,
        ...options,
        ...(deadLetter && { deadLetter: deadLetter.queueName }),
      }).filter(([, value]) => value !== undefined),
    ) as Omit<Queue, "name">;

    await boss.createQueue(queueName, declared);
    await applyDeclaredOptions(boss, queueName, declared);
  }

  if (schedules) {
    await syncSchedules(boss, queueName, schedules);
  }
  for (const { eventName } of subscribeTo ?? []) {
    // oxlint-disable-next-line no-await-in-loop -- a job subscribes to few events, and order keeps errors readable.
    await boss.subscribe(eventName, queueName);
  }
}

/**
 * Reads every page of a pg-boss list. Its `limit` caps one page, and the last row
 * of a page is the `after` that continues the list.
 */
export async function readAllPages<Row, After>(
  readPage: (options: { limit: number; after?: After }) => Promise<Row[]>,
  toAfter: (row: Row) => After,
): Promise<Row[]> {
  const limit = 1_000;
  const rows: Row[] = [];
  let after: After | undefined;

  for (;;) {
    // oxlint-disable-next-line no-await-in-loop -- each page continues from the previous one.
    const page = await readPage(
      after === undefined ? { limit } : { limit, after },
    );
    rows.push(...page);
    const last = page.at(-1);
    if (page.length < limit || last === undefined) {
      return rows;
    }
    after = toAfter(last);
  }
}

async function applyDeclaredOptions(
  boss: PgBoss,
  queueName: string,
  declared: Omit<Queue, "name">,
): Promise<void> {
  const declaredEntries = Object.entries(declared);
  if (declaredEntries.length === 0) {
    return;
  }

  const existing = (await boss.getQueue(queueName)) as Record<
    string,
    unknown
  > | null;
  if (!existing) {
    return;
  }

  const changedEntries = declaredEntries.filter(
    ([key, value]) => existing[key] !== value,
  );

  for (const option of IMMUTABLE_OPTIONS) {
    if (changedEntries.some(([key]) => key === option)) {
      throw new Error(
        `Queue "${queueName}" already exists with ${option} ${JSON.stringify(existing[option])}, ` +
          `but its job declares ${JSON.stringify(declared[option])}. ` +
          `pg-boss cannot change ${option} on an existing queue, so use a new queue name ` +
          `or move the jobs and delete the queue.`,
      );
    }
  }

  if (changedEntries.length > 0) {
    const changes: Record<string, unknown> = Object.fromEntries(changedEntries);
    // pg-boss only accepts retryDelayMax alongside retryBackoff in the same call,
    // and it has validated that a declared retryDelayMax has one by this point.
    if ("retryDelayMax" in changes) {
      changes.retryBackoff = declared.retryBackoff;
    }
    await boss.updateQueue(queueName, changes as UpdateQueueOptions);
    logger.log(
      `Updated queue "${queueName}": ${changedEntries.map(([key]) => key).join(", ")}`,
    );
  }
}

const asJson = (value: unknown): unknown =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));

/**
 * Makes the stored schedules of a queue match its declaration: adds and updates
 * what differs and removes keys no longer declared. Instances can do this at the
 * same time, because each step is an idempotent write of the same end state.
 */
async function syncSchedules(
  boss: PgBoss,
  queueName: string,
  schedules: JobSchedule[],
): Promise<void> {
  const keys = schedules.map(({ options }) => options?.key ?? "");
  const duplicate = keys.find((key, index) => keys.indexOf(key) !== index);
  if (duplicate !== undefined) {
    throw new Error(
      `Job "${queueName}" declares two schedules with the key "${duplicate}". ` +
        `Give each schedule of a job its own options.key.`,
    );
  }

  const stored = await readAllPages<Schedule, Pick<Schedule, "name" | "key">>(
    (options) => boss.getSchedules(queueName, undefined, options),
    ({ name, key }) => ({ name, key }),
  );

  for (const { cron, data, options = {} } of schedules) {
    const key = options.key ?? "";
    const current = stored.find((schedule) => schedule.key === key);
    const unchanged =
      current?.cron === cron &&
      current.timezone === (options.tz || "UTC") &&
      isDeepStrictEqual(current.data ?? null, asJson(data) ?? null) &&
      isDeepStrictEqual(current.options ?? {}, asJson(options));

    if (!unchanged) {
      // oxlint-disable-next-line no-await-in-loop -- startup work on a short list, and order keeps errors readable.
      await boss.schedule(queueName, cron, data, options);
    }
  }

  for (const { key } of stored) {
    if (!keys.includes(key)) {
      // oxlint-disable-next-line no-await-in-loop -- see above.
      await boss.unschedule(queueName, key);
      logger.log(
        `Removed schedule "${key}" from queue "${queueName}", no longer declared`,
      );
    }
  }
}
