import { Injectable } from "@nestjs/common";
import { trace } from "@opentelemetry/api";
import {
  InMemorySpanExporter,
  NodeTracerProvider,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import { createJob, type JobOf } from "../src";
import { POLL, serviceOf, useApps, waitFor } from "./support";

const TracedJob = createJob<{ index: number }>("telemetry-traced", {
  retryLimit: 0,
});

@Injectable()
class Handler {
  readonly traceIds: (string | undefined)[] = [];

  @TracedJob.Handle(POLL)
  async handle(_job: JobOf<typeof TracedJob>) {
    this.traceIds.push(trace.getActiveSpan()?.spanContext().traceId);
  }
}

describe("OpenTelemetry through this package (e2e)", () => {
  const { build } = useApps();
  const exporter = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });

  beforeAll(() => {
    provider.register();
  });

  afterAll(async () => {
    await provider.shutdown();
  });

  it("continues the sender's trace in the handler", async () => {
    const app = await build({
      jobs: [TracedJob],
      providers: [Handler],
      options: { openTelemetry: { enabled: true, propagateContext: true } },
    });
    const handler = app.get(Handler);

    const senderTraceId = await trace
      .getTracer("test")
      .startActiveSpan("sender", async (span) => {
        await serviceOf(app, TracedJob).send({ index: 1 });
        span.end();
        return span.spanContext().traceId;
      });

    await waitFor(() => handler.traceIds.length === 1);

    expect(handler.traceIds).toEqual([senderTraceId]);
    await waitFor(() =>
      exporter
        .getFinishedSpans()
        .some(
          (span) =>
            span.name === "process telemetry-traced" &&
            span.spanContext().traceId === senderTraceId,
        ),
    );
  });
});
