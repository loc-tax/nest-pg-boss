import { AssertionError } from "node:assert";
import type { Job } from "pg-boss";
import { defer, lastValueFrom } from "rxjs";
import {
  applyDisableWorkersDefaults,
  assertNoRemovedOptions,
  assertValidModuleOptions,
  handleRetry,
  REMOVED_MODULE_OPTIONS,
  toWorkHandler,
} from "./utils";
import { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";

const baseOptions: PGBossModuleOptions = {
  onError: () => {},
};

describe("applyDisableWorkersDefaults", () => {
  it("returns options unchanged when disableWorkers is not set", () => {
    const result = applyDisableWorkersDefaults(baseOptions);
    expect(result).toBe(baseOptions);
  });

  it("returns options unchanged when disableWorkers is false", () => {
    const options = { ...baseOptions, disableWorkers: false };
    const result = applyDisableWorkersDefaults(options);
    expect(result).toBe(options);
  });

  it("defaults supervise and schedule to false when disableWorkers is true", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
    });
    expect(result.supervise).toBe(false);
    expect(result.schedule).toBe(false);
  });

  it("preserves an explicit supervise: true override", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      supervise: true,
    });
    expect(result.supervise).toBe(true);
    expect(result.schedule).toBe(false);
  });

  it("preserves an explicit schedule: true override", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      schedule: true,
    });
    expect(result.supervise).toBe(false);
    expect(result.schedule).toBe(true);
  });

  it("treats explicit undefined as 'set' and does not overwrite it", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      supervise: undefined,
      schedule: undefined,
    });
    expect(result.supervise).toBeUndefined();
    expect(result.schedule).toBeUndefined();
  });

  it("does not mutate the input", () => {
    const options: PGBossModuleOptions = {
      ...baseOptions,
      disableWorkers: true,
    };
    applyDisableWorkersDefaults(options);
    expect(Object.hasOwn(options, "supervise")).toBe(false);
    expect(Object.hasOwn(options, "schedule")).toBe(false);
  });
});

const attemptsFor = async (error: Error) => {
  let attempts = 0;
  await expect(
    lastValueFrom(
      defer(async () => {
        attempts++;
        throw error;
      }).pipe(handleRetry(3, 0)),
    ),
  ).rejects.toBe(error);
  return attempts;
};

describe("handleRetry", () => {
  it("retries ordinary errors", async () => {
    await expect(attemptsFor(new Error("connection refused"))).resolves.toBe(3);
  });

  it("does not retry the assertion errors pg-boss throws for permanent problems", async () => {
    await expect(
      attemptsFor(new AssertionError({ message: "schema too old" })),
    ).resolves.toBe(1);
  });
});

describe("toWorkHandler", () => {
  const firstJob = { id: "first" };
  const secondJob = { id: "second" };
  const jobs = [firstJob, secondJob] as unknown as Job<unknown>[];

  it("unwraps the array and forwards further arguments without batchSize", async () => {
    const handler = vi.fn(async () => "output");
    const workHandler = toWorkHandler(handler, {}) as (
      ...args: unknown[]
    ) => Promise<unknown>;

    await expect(workHandler([firstJob], "tx")).resolves.toBe("output");
    expect(handler).toHaveBeenCalledWith(firstJob, "tx");
  });

  it.each([
    ["batchSize", { batchSize: 5 }],
    ["batchSize: 1", { batchSize: 1 }],
    ["perJobResults", { perJobResults: true }],
  ])("keeps the array with %s", async (_label, options) => {
    const handler = vi.fn(async () => {});
    await toWorkHandler(handler, options)(jobs);
    expect(handler).toHaveBeenCalledWith(jobs);
  });
});

describe("assertValidModuleOptions", () => {
  it("rejects stopOptions.destroy, which pg-boss would ignore", () => {
    expect(() =>
      assertValidModuleOptions({
        ...baseOptions,
        stopOptions: { destroy: true } as object,
      }),
    ).toThrow("destroy (use close)");
  });

  it("rejects options that cannot be defaulted across a module", () => {
    expect(() =>
      assertValidModuleOptions({
        ...baseOptions,
        defaultQueueOptions: { policy: "short" } as object,
      }),
    ).toThrow("defaultQueueOptions cannot set policy");
    expect(() =>
      assertValidModuleOptions({
        ...baseOptions,
        defaultWorkOptions: { batchSize: 5 } as object,
      }),
    ).toThrow("defaultWorkOptions cannot set batchSize");
  });

  it("accepts valid defaults", () => {
    expect(() =>
      assertValidModuleOptions({
        ...baseOptions,
        stopOptions: { close: true },
        defaultQueueOptions: { retryLimit: 0 },
        defaultWorkOptions: { drain: true, localConcurrency: 2 },
      }),
    ).not.toThrow();
  });
});

describe("assertNoRemovedOptions", () => {
  it("names each removed option with its replacement", () => {
    expect(() =>
      assertNoRemovedOptions(
        { noSupervisor: true, retryLimit: 3, max: 5 },
        REMOVED_MODULE_OPTIONS,
        "forRoot()",
      ),
    ).toThrow(
      "forRoot() uses options pg-boss no longer supports: noSupervisor (use supervise: false), retryLimit (declare it per queue on createJob)",
    );
  });

  it("rejects retention and polling settings that kept a similar name", () => {
    expect(() =>
      assertNoRemovedOptions(
        { retentionSeconds: 60, newJobCheckIntervalSeconds: 5 },
        REMOVED_MODULE_OPTIONS,
        "forRoot()",
      ),
    ).toThrow(/retentionSeconds .*newJobCheckIntervalSeconds/);
  });

  it("accepts retryDelay, which is this module's connection retry delay", () => {
    expect(() =>
      assertNoRemovedOptions(
        { retryDelay: 1000, retryAttempts: 3 },
        REMOVED_MODULE_OPTIONS,
        "forRoot()",
      ),
    ).not.toThrow();
  });

  it("accepts current options", () => {
    expect(() =>
      assertNoRemovedOptions(
        { max: 2, supervise: false },
        REMOVED_MODULE_OPTIONS,
        "forRoot()",
      ),
    ).not.toThrow();
  });
});
