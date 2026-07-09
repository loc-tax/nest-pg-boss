import PGBoss from "pg-boss";
import { JobService } from "./job.service";

describe("JobService", () => {
  it("exposes the pg-boss queue name via queueName", () => {
    const service = new JobService<{ id: string }>(
      "StartLibraryFileAnalysis",
      {} as PGBoss,
    );
    expect(service.queueName).toBe("StartLibraryFileAnalysis");
  });
});
