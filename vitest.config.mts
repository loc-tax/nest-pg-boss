import swc from "unplugin-swc";
import { defineConfig } from "vitest/config";

// Nest resolves constructor dependencies from the `design:paramtypes` metadata
// that `emitDecoratorMetadata` produces.
// Vitest's default esbuild transform silently drops that metadata, which breaks
// DI in the e2e suite, so the tests are transformed with SWC instead.
const swcPlugin = () =>
  swc.vite({
    module: { type: "es6" },
    jsc: {
      target: "es2022",
      parser: { syntax: "typescript", decorators: true },
      transform: { legacyDecorator: true, decoratorMetadata: true },
    },
  });

export default defineConfig({
  test: {
    projects: [
      {
        plugins: [swcPlugin()],
        test: {
          name: "unit",
          globals: true,
          environment: "node",
          include: ["src/**/*.spec.ts"],
        },
      },
      {
        plugins: [swcPlugin()],
        test: {
          name: "e2e",
          globals: true,
          environment: "node",
          include: ["test/**/*.e2e-spec.ts"],
          // Starting and stopping the Postgres testcontainer dominates these
          // timings, and the afterAll hook alone sleeps 10s while it waits for
          // the pg-boss workers to drain.
          testTimeout: 60_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
