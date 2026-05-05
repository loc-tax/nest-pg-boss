import { applyDisableWorkersDefaults } from "./utils";
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

  it("defaults noSupervisor and noScheduling to true when disableWorkers is true", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
    });
    expect(result.noSupervisor).toBe(true);
    expect(result.noScheduling).toBe(true);
  });

  it("preserves an explicit noSupervisor: false override", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      noSupervisor: false,
    });
    expect(result.noSupervisor).toBe(false);
    expect(result.noScheduling).toBe(true);
  });

  it("preserves an explicit noScheduling: false override", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      noScheduling: false,
    });
    expect(result.noSupervisor).toBe(true);
    expect(result.noScheduling).toBe(false);
  });

  it("treats explicit undefined as 'set' and does not overwrite it", () => {
    const result = applyDisableWorkersDefaults({
      ...baseOptions,
      disableWorkers: true,
      noSupervisor: undefined,
      noScheduling: undefined,
    });
    expect(result.noSupervisor).toBeUndefined();
    expect(result.noScheduling).toBeUndefined();
  });

  it("does not mutate the input", () => {
    const options: PGBossModuleOptions = {
      ...baseOptions,
      disableWorkers: true,
    };
    applyDisableWorkersDefaults(options);
    expect(Object.hasOwn(options, "noSupervisor")).toBe(false);
    expect(Object.hasOwn(options, "noScheduling")).toBe(false);
  });
});
