import { setTimeout as sleep } from "node:timers/promises";
import { Injectable } from "@nestjs/common";
import { PgBoss, type Db, type Job as PgBossJob } from "pg-boss";
import { createJob, FlowService, step } from "../src";
import { POLL, useApps, waitFor } from "./support";

const ExtractJob = createJob<{ file: string }>("flow-extract", {
  retryLimit: 0,
});
const LoadJob = createJob<{ output: string }>("flow-load", { retryLimit: 0 });
const NotifyJob = createJob<{ output: string }>("flow-notify", {
  retryLimit: 0,
});
const RootJob = createJob<{ commit: boolean; tag: string }>("flow-root", {
  retryLimit: 0,
});

@Injectable()
class Pipeline {
  readonly seen: string[] = [];
  failFile: string | undefined;

  constructor(private readonly flows: FlowService) {}

  @ExtractJob.Handle(POLL)
  async extract(job: PgBossJob<{ file: string }>) {
    this.seen.push(`extract:${job.data.file}`);
    if (job.data.file === this.failFile) {
      throw new Error("extract failed");
    }
  }

  @LoadJob.Handle(POLL)
  async load(job: PgBossJob<{ output: string }>) {
    this.seen.push(`load:${job.data.output}`);
  }

  @NotifyJob.Handle(POLL)
  async notify(job: PgBossJob<{ output: string }>) {
    this.seen.push(`notify:${job.data.output}`);
  }

  /** Starts a flow inside the transaction pg-boss opened for this job. */
  @RootJob.Handle({ ...POLL, transactional: true })
  async root(job: PgBossJob<{ commit: boolean; tag: string }>, tx: Db) {
    await this.flows.run(
      {
        first: step(ExtractJob, { file: `tx-${job.data.tag}` }),
        last: step(
          LoadJob,
          { output: `tx-${job.data.tag}` },
          { dependsOn: ["first"] },
        ),
      },
      { db: tx },
    );
    if (!job.data.commit) {
      throw new Error("rolling back");
    }
  }
}

const flowJobsFor = async (boss: PgBoss, tag: string) => ({
  extract: await boss.findJobs("flow-extract", {
    data: { file: `tx-${tag}` },
  }),
  load: await boss.findJobs("flow-load", { data: { output: `tx-${tag}` } }),
});

describe("FlowService (e2e)", () => {
  const { build } = useApps();

  const start = async () => {
    const app = await build({
      providers: [Pipeline],
      options: { flowIntervalSeconds: 1 },
    });
    return {
      app,
      flows: app.get(FlowService),
      pipeline: app.get(Pipeline),
      boss: app.get(PgBoss),
    };
  };

  it("runs a child only after all parents, with typed refs and ids", async () => {
    const { flows, pipeline } = await start();

    const ids = await flows.run({
      one: step(ExtractJob, { file: "ok-1" }),
      two: step(ExtractJob, { file: "ok-2" }),
      load: step(LoadJob, { output: "report" }, { dependsOn: ["one", "two"] }),
    });
    const loadId: string = ids.load;

    await waitFor(() => pipeline.seen.includes("load:report"));

    expect(pipeline.seen.at(-1)).toBe("load:report");
    expect(pipeline.seen).toEqual(
      expect.arrayContaining(["extract:ok-1", "extract:ok-2"]),
    );
    await expect(flows.getDependencies(LoadJob, loadId)).resolves.toHaveLength(
      2,
    );
    await expect(
      flows.getDependents(ExtractJob, ids.one),
    ).resolves.toMatchObject([{ name: "flow-load", id: loadId }]);
  });

  it("rejects bad refs and payloads at compile time", async () => {
    const { flows } = await start();
    const typeChecks = () => {
      void flows.run({
        one: step(ExtractJob, { file: "x" }),
        // @ts-expect-error "typo" is not a step of this flow
        load: step(LoadJob, { output: "y" }, { dependsOn: ["typo"] }),
      });
      // @ts-expect-error `output` is not part of the extract payload
      step(ExtractJob, { output: "wrong" });
    };
    expect(typeChecks).toBeTypeOf("function");
  });

  it("creates queues for jobs the module has not seen", async () => {
    const { flows, boss } = await start();
    const UnseenJob = createJob<{ tag: string }>("flow-unseen", {
      retryLimit: 5,
    });

    await flows.run({
      one: step(UnseenJob, { tag: "new" }),
      two: step(UnseenJob, { tag: "newer" }, { dependsOn: ["one"] }),
    });

    await expect(boss.getQueue("flow-unseen")).resolves.toMatchObject({
      retryLimit: 5,
    });
  });

  it("releases a child through resolve() without waiting for the timer", async () => {
    const app = await build({
      providers: [Pipeline],
      options: { flowIntervalSeconds: 3_600 },
    });
    const flows = app.get(FlowService);
    const pipeline = app.get(Pipeline);

    await flows.run({
      one: step(ExtractJob, { file: "manual" }),
      load: step(LoadJob, { output: "manual" }, { dependsOn: ["one"] }),
    });
    await waitFor(() => pipeline.seen.includes("extract:manual"));
    await sleep(500);

    await flows.resolve();
    await waitFor(() => pipeline.seen.includes("load:manual"), 5_000);
  });

  it("keeps a failed parent's descendants blocked until they are cancelled", async () => {
    const { flows, pipeline, boss } = await start();
    pipeline.failFile = "bad";

    const ids = await flows.run({
      bad: step(ExtractJob, { file: "bad" }),
      load: step(LoadJob, { output: "never" }, { dependsOn: ["bad"] }),
      notify: step(NotifyJob, { output: "never" }, { dependsOn: ["load"] }),
    });

    const stateOf = async (queue: string, id: string) =>
      (await boss.findJobs(queue, { id }))[0];

    await waitFor(
      async () => (await stateOf("flow-extract", ids.bad))?.state === "failed",
    );
    await flows.resolve();
    await sleep(1_500);

    expect(pipeline.seen).not.toContain("load:never");
    await expect(stateOf("flow-load", ids.load)).resolves.toMatchObject({
      state: "created",
      blocked: true,
    });

    await expect(flows.cancelDependents(ExtractJob, ids.bad)).resolves.toBe(2);

    await expect(stateOf("flow-load", ids.load)).resolves.toMatchObject({
      state: "cancelled",
    });
    await expect(stateOf("flow-notify", ids.notify)).resolves.toMatchObject({
      state: "cancelled",
    });
  });

  it("counts a descendant shared by two parents once", async () => {
    const { flows, pipeline } = await start();
    pipeline.failFile = "diamond";

    const ids = await flows.run({
      bad: step(ExtractJob, { file: "diamond" }),
      left: step(LoadJob, { output: "left" }, { dependsOn: ["bad"] }),
      right: step(LoadJob, { output: "right" }, { dependsOn: ["bad"] }),
      shared: step(
        NotifyJob,
        { output: "shared" },
        { dependsOn: ["left", "right"] },
      ),
    });

    await expect(flows.cancelDependents(ExtractJob, ids.bad)).resolves.toBe(3);
  });

  it("unblocks a child when its failed parent is retried", async () => {
    const { flows, pipeline, boss } = await start();
    pipeline.failFile = "flaky";

    const ids = await flows.run({
      flaky: step(ExtractJob, { file: "flaky" }),
      load: step(LoadJob, { output: "after-retry" }, { dependsOn: ["flaky"] }),
    });
    await waitFor(
      async () =>
        (await boss.findJobs("flow-extract", { id: ids.flaky }))[0]?.state ===
        "failed",
    );

    pipeline.failFile = undefined;
    await boss.retry("flow-extract", ids.flaky);

    await waitFor(() => pipeline.seen.includes("load:after-retry"));
  });

  describe("with a caller's transaction", () => {
    it("leaves nothing behind when the transaction rolls back", async () => {
      const { boss } = await start();

      const rootId = await boss.send("flow-root", {
        commit: false,
        tag: "rollback",
      });

      await waitFor(
        async () =>
          (await boss.findJobs("flow-root", { id: rootId ?? "" }))[0]?.state ===
          "failed",
      );

      await expect(flowJobsFor(boss, "rollback")).resolves.toEqual({
        extract: [],
        load: [],
      });
    });

    it("runs the flow when the transaction commits", async () => {
      const { boss, pipeline } = await start();

      await boss.send("flow-root", { commit: true, tag: "commit" });

      await waitFor(() => pipeline.seen.includes("load:tx-commit"));

      const { extract, load } = await flowJobsFor(boss, "commit");
      expect(extract).toHaveLength(1);
      expect(load).toHaveLength(1);
    });
  });
});
