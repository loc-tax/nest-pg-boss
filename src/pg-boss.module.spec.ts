import "reflect-metadata";
import { HandlerScannerService } from "./handler-scanner.service";
import * as publicApi from "./index";
import { PGBossModule } from "./pg-boss.module";

describe("PGBossModule", () => {
  it("exports HandlerScannerService so consumers can enumerate registered handlers", () => {
    const exportedProviders = Reflect.getMetadata("exports", PGBossModule);

    expect(exportedProviders).toContain(HandlerScannerService);
  });

  it("exposes HandlerScannerService through the package entry point", () => {
    expect(publicApi.HandlerScannerService).toBe(HandlerScannerService);
  });
});
