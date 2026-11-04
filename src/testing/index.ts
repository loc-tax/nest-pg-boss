import { randomUUID } from "node:crypto";
import type { Job, JobDataOf, JobWithMetadataOf } from "../job.service";

export {
  CLOCK_OVERRIDE_SETTING,
  disableClockOverride,
  enableClockOverride,
  systemClock,
  TestClock,
} from "pg-boss";
export type { JobSpyInterface, JobSpyState, SpyJob } from "pg-boss";

/**
 * Builds a job as a handler would receive it, to unit test a handler without a
 * database. `data` goes through JSON like the real thing, so a `Date` arrives as a
 * string. The result has every metadata field, so it also fits `includeMetadata`
 * handlers, and `overrides` can set any of them.
 */
export function createTestJob<Declared extends Job>(
  job: Declared,
  data: JobDataOf<Declared>,
  overrides: Partial<JobWithMetadataOf<Declared>> = {},
): JobWithMetadataOf<Declared> {
  const now = new Date();

  return {
    id: randomUUID(),
    name: job.queueName,
    data: JSON.parse(JSON.stringify(data)),
    expireInSeconds: 900,
    heartbeatSeconds: null,
    retryCount: 0,
    signal: new AbortController().signal,
    priority: 0,
    state: "active",
    retryLimit: 2,
    retryDelay: 0,
    retryBackoff: false,
    startAfter: now,
    startedOn: now,
    singletonKey: null,
    singletonOn: null,
    deleteAfterSeconds: 604_800,
    createdOn: now,
    completedOn: null,
    keepUntil: new Date(now.getTime() + 604_800_000),
    policy: "standard",
    heartbeatOn: null,
    blocked: false,
    blocking: false,
    pendingDependencies: 0,
    deadLetter: "",
    output: {},
    sourceName: null,
    sourceId: null,
    sourceCreatedOn: null,
    sourceRetryCount: null,
    sourceOutput: null,
    sourceRootId: null,
    ...overrides,
  } as JobWithMetadataOf<Declared>;
}
