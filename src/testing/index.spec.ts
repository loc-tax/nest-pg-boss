import type { Job as PgBossJob } from "pg-boss";
import { createJob, type JobOf } from "../job.service";
import { createTestJob } from "./index";

interface Payload {
  at: Date;
  count: number;
}

const TestedJob = createJob<Payload>("tested");

describe("createTestJob", () => {
  it("delivers data as JSON, as the database would", () => {
    const at = new Date("2030-01-02T03:04:05.000Z");

    const job = createTestJob(TestedJob, { at, count: 1 });
    const typed: PgBossJob<{ at: string; count: number }> = job;

    expect(typed.data).toEqual({ at: at.toISOString(), count: 1 });
    expect(job.name).toBe("tested");
    expect(job.signal.aborted).toBe(false);
  });

  it("checks data against the job and keeps its literal types", () => {
    const StatusJob = createJob<{ status: "pending" | "done" }>("status");
    const handle = (job: JobOf<typeof StatusJob>) => job.data.status;

    expect(handle(createTestJob(StatusJob, { status: "pending" }))).toBe(
      "pending",
    );
    expect(() =>
      // @ts-expect-error this data does not belong to this job
      createTestJob(StatusJob, { other: 1 }),
    ).not.toThrow();
  });

  it("applies overrides", () => {
    const job = createTestJob(
      TestedJob,
      { at: new Date(), count: 1 },
      { retryCount: 2, state: "retry" },
    );

    expect(job).toMatchObject({ retryCount: 2, state: "retry" });
  });
});
