/**
 * Keeps this package complete. Every public `PgBoss` method and every runtime
 * export of pg-boss has to be classified here, so a pg-boss upgrade that adds one
 * fails `pnpm typecheck` until someone decides where it belongs.
 */
import { readFileSync } from "node:fs";
import type EventEmitter from "node:events";
import { resolve } from "node:path";
import * as pgBossExports from "pg-boss";
import type { FlowService } from "./flow.service";
import type { PubSubService } from "./events";
import type { JobService } from "./job.service";
import * as publicApi from "./index";

type PublicMethod = Exclude<keyof pgBossExports.PgBoss, keyof EventEmitter>;

/** Where each pg-boss method is offered. */
type Home =
  /** Bound to one queue and typed to the job's data. */
  | "JobService"
  | "FlowService"
  | "PubSubService"
  /** The module does this for you, at startup or shutdown. */
  | "module"
  /** Not tied to a queue. Inject `PgBoss`, which reads the same as a wrapper would. */
  | "raw";

const METHODS = {
  // On JobService
  cancel: "JobService",
  complete: "JobService",
  deleteAllJobs: "JobService",
  deleteJob: "JobService",
  deleteQueue: "JobService",
  deleteQueuedJobs: "JobService",
  deleteStoredJobs: "JobService",
  fail: "JobService",
  fetch: "JobService",
  findJobs: "JobService",
  getBlockedKeys: "JobService",
  getDependencies: "JobService",
  getDependents: "JobService",
  getQueue: "JobService",
  getQueueStats: "JobService",
  getSchedule: "JobService",
  getSchedules: "JobService",
  getSpy: "JobService",
  insert: "JobService",
  offWork: "JobService",
  previewRedrive: "JobService",
  redrive: "JobService",
  resume: "JobService",
  retry: "JobService",
  schedule: "JobService",
  send: "JobService",
  sendAfter: "JobService",
  sendDebounced: "JobService",
  sendThrottled: "JobService",
  supervise: "JobService",
  touch: "JobService",
  unschedule: "JobService",
  update: "JobService",
  updateQueue: "JobService",
  upsert: "JobService",
  work: "JobService",

  // On their own services
  flow: "FlowService",
  resolveFlow: "FlowService",
  publish: "PubSubService",
  subscribe: "PubSubService",
  unsubscribe: "PubSubService",

  // Handled by the module
  createQueue: "module",
  start: "module",
  stop: "module",

  // Inject PgBoss
  clearSpies: "raw",
  detectSchemaDrift: "raw",
  getBamEntries: "raw",
  getBamStatus: "raw",
  getDb: "raw",
  getInstances: "raw",
  /** Deprecated by pg-boss in favor of `findJobs`. */
  getJobById: "raw",
  getQueues: "raw",
  getReindexCommands: "raw",
  getWipData: "raw",
  isBamWorking: "raw",
  isCheckingSkew: "raw",
  isInstalled: "raw",
  isMaintaining: "raw",
  isResolvingFlow: "raw",
  isTimekeeping: "raw",
  notifyWorker: "raw",
  previewSchedule: "raw",
  schemaVersion: "raw",
} as const satisfies Record<PublicMethod, Home>;

type MethodsIn<Target extends Home> = {
  [Method in keyof typeof METHODS]: (typeof METHODS)[Method] extends Target
    ? Method
    : never;
}[keyof typeof METHODS];

// A classification must be true: each of these fails to compile if the service
// does not have the method it is credited with. FlowService and PubSubService
// name some of theirs differently, which the mapping below records.
type JobServiceMethods = keyof JobService<object>;
const jobServiceHasAll: MethodsIn<"JobService"> extends JobServiceMethods
  ? true
  : never = true;

const FLOW_NAMES = {
  flow: "run",
  resolveFlow: "resolve",
} as const satisfies Record<MethodsIn<"FlowService">, keyof FlowService>;

const PUB_SUB_NAMES = {
  publish: "publish",
  subscribe: "subscribe",
  unsubscribe: "unsubscribe",
} as const satisfies Record<MethodsIn<"PubSubService">, keyof PubSubService>;

// Fails to compile when pg-boss adds a runtime export this package does not re-export.
const exportsCovered: Exclude<
  keyof typeof pgBossExports,
  keyof typeof publicApi
> extends never
  ? true
  : never = true;

/** The source names of what a file re-exports as types. */
const typeExportsOf = (source: string, fromPattern: string): string[] =>
  [
    ...source.matchAll(
      new RegExp(`export type \\{([^}]*)\\} from ['"]${fromPattern}['"]`, "g"),
    ),
  ].flatMap(([, names = ""]) =>
    names
      .split(",")
      .map((name) => name.trim())
      .filter(Boolean),
  );

const nameAfterAs = (name: string) => name.split(" as ").at(-1) ?? name;
const nameBeforeAs = (name: string) => name.split(" as ").at(0) ?? name;

describe("pg-boss API coverage", () => {
  // Types do not exist at runtime or in `keyof`, so these are compared as text.
  it("re-exports every type pg-boss exports", () => {
    const pgBossTypes = typeExportsOf(
      readFileSync(
        resolve(__dirname, "../node_modules/pg-boss/dist/index.d.ts"),
        "utf8",
      ),
      "[^'\"]+",
    ).map(nameAfterAs);
    const ownTypes = new Set(
      typeExportsOf(
        readFileSync(resolve(__dirname, "index.ts"), "utf8"),
        "pg-boss",
      ).map(nameBeforeAs),
    );

    expect(pgBossTypes.length).toBeGreaterThan(50);
    expect(pgBossTypes.filter((name) => !ownTypes.has(name))).toEqual([]);
  });

  it("classifies every method on the PgBoss prototype, including any the types omit", () => {
    const eventEmitterMethods = new Set<string>();
    for (
      let prototype = Object.getPrototypeOf(pgBossExports.PgBoss.prototype);
      prototype && prototype !== Object.prototype;
      prototype = Object.getPrototypeOf(prototype)
    ) {
      for (const name of Object.getOwnPropertyNames(prototype)) {
        eventEmitterMethods.add(name);
      }
    }

    const runtimeMethods = Object.getOwnPropertyNames(
      pgBossExports.PgBoss.prototype,
    ).filter((name) => name !== "constructor");

    // oxlint-disable-next-line unicorn/no-array-sort -- both arrays are fresh, so sorting mutates nothing shared.
    expect(runtimeMethods.sort()).toEqual(Object.keys(METHODS).sort());
    expect(jobServiceHasAll).toBe(true);
    expect(exportsCovered).toBe(true);
    expect(FLOW_NAMES.flow).toBe("run");
    expect(PUB_SUB_NAMES.publish).toBe("publish");
  });

  it("re-exports the same values as pg-boss", () => {
    for (const [name, value] of Object.entries(pgBossExports)) {
      expect((publicApi as Record<string, unknown>)[name]).toBe(value);
    }
  });
});
