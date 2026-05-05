import "reflect-metadata";
import { MetadataScanner, type ModulesContainer } from "@nestjs/core";
import { HandlerScannerService } from "./handler-scanner.service";
import { PG_BOSS_JOB_METADATA } from "./pg-boss.constants";
import type { HandlerMetadata } from "./interfaces/handler-metadata.interface";

const decorate = (
  prototype: object,
  methodKey: string,
  metadata: HandlerMetadata,
): void => {
  const target = (prototype as Record<string, unknown>)[methodKey] as object;
  Reflect.defineMetadata(PG_BOSS_JOB_METADATA, metadata, target);
};

const buildContainer = (
  providersByModule: { instance: unknown }[][],
): ModulesContainer => {
  const modules = providersByModule.map((providers) => ({
    providers: new Map(providers.map((p, i) => [`provider-${i}`, p])),
  }));
  return {
    values: () => modules.values(),
  } as unknown as ModulesContainer;
};

describe("HandlerScannerService.getJobHandlers", () => {
  it("returns handlers from providers with @PGBossHandler-decorated methods", () => {
    class FooHandler {
      handle() {}
    }
    decorate(FooHandler.prototype, "handle", {
      token: "JobService(foo)",
      jobName: "foo",
      workOptions: {},
    });

    const container = buildContainer([[{ instance: new FooHandler() }]]);
    const service = new HandlerScannerService(new MetadataScanner(), container);

    const handlers = service.getJobHandlers();
    expect(handlers).toHaveLength(1);
    expect(handlers[0]?.metadata.jobName).toBe("foo");
    expect(typeof handlers[0]?.callback).toBe("function");
  });

  it("returns multiple handlers from one provider, ignoring undecorated methods", () => {
    class MultiHandler {
      a() {}
      b() {}
      c() {}
    }
    decorate(MultiHandler.prototype, "a", {
      token: "ta",
      jobName: "a",
      workOptions: {},
    });
    decorate(MultiHandler.prototype, "b", {
      token: "tb",
      jobName: "b",
      workOptions: {},
    });

    const container = buildContainer([[{ instance: new MultiHandler() }]]);
    const service = new HandlerScannerService(new MetadataScanner(), container);

    const handlers = service.getJobHandlers();
    expect(handlers.map((h) => h.metadata.jobName).sort()).toEqual(["a", "b"]);
  });

  it("skips providers with no instance without scanning Object.prototype", () => {
    const scanner = new MetadataScanner();
    const getAllMethodNamesSpy = jest.spyOn(scanner, "getAllMethodNames");

    const container = buildContainer([
      [{ instance: undefined }, { instance: null }],
    ]);
    const service = new HandlerScannerService(scanner, container);

    const handlers = service.getJobHandlers();

    expect(handlers).toHaveLength(0);
    expect(getAllMethodNamesSpy).not.toHaveBeenCalled();
  });

  it("skips providers whose instance is a primitive (value providers)", () => {
    const scanner = new MetadataScanner();
    const getAllMethodNamesSpy = jest.spyOn(scanner, "getAllMethodNames");

    const container = buildContainer([
      [{ instance: "config-string" }, { instance: 42 }, { instance: true }],
    ]);
    const service = new HandlerScannerService(scanner, container);

    expect(service.getJobHandlers()).toHaveLength(0);
    expect(getAllMethodNamesSpy).not.toHaveBeenCalled();
  });

  it("skips plain-object value providers without scanning Object.prototype", () => {
    const scanner = new MetadataScanner();
    const getAllMethodNamesSpy = jest.spyOn(scanner, "getAllMethodNames");

    const container = buildContainer([
      [
        { instance: { someMethod: () => {} } },
        { instance: Object.create(null) },
      ],
    ]);
    const service = new HandlerScannerService(scanner, container);

    expect(service.getJobHandlers()).toHaveLength(0);
    expect(getAllMethodNamesSpy).not.toHaveBeenCalled();
  });

  it("only scans prototypes of real object instances when mixed with empties", () => {
    class FooHandler {
      handle() {}
    }
    decorate(FooHandler.prototype, "handle", {
      token: "JobService(foo)",
      jobName: "foo",
      workOptions: {},
    });

    const scanner = new MetadataScanner();
    const getAllMethodNamesSpy = jest.spyOn(scanner, "getAllMethodNames");

    const container = buildContainer([
      [
        { instance: undefined },
        { instance: new FooHandler() },
        { instance: "string-config" },
      ],
    ]);
    const service = new HandlerScannerService(scanner, container);

    const handlers = service.getJobHandlers();
    expect(handlers).toHaveLength(1);
    expect(handlers[0]?.metadata.jobName).toBe("foo");
    expect(getAllMethodNamesSpy).toHaveBeenCalledTimes(1);
    expect(getAllMethodNamesSpy).toHaveBeenCalledWith(FooHandler.prototype);
  });
});
