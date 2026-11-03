import { execFile } from "node:child_process";
import { resolve } from "node:path";
import { promisify } from "node:util";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";

const run = promisify(execFile);
const root = resolve(__dirname, "..");
// Inside node_modules so that the compiled files still resolve the dependencies.
const outDir = resolve(root, "node_modules/.cache/nest-pg-boss-cjs");

describe("CommonJS build (e2e)", () => {
  let postgres: StartedPostgreSqlContainer;

  beforeAll(async () => {
    postgres = await new PostgreSqlContainer("postgres:16-alpine").start();
    await run(
      resolve(root, "node_modules/.bin/tsc"),
      ["-p", "tsconfig.build.json", "--outDir", outDir],
      { cwd: root },
    );
  });

  afterAll(async () => {
    await postgres?.stop();
  });

  it("loads pg-boss through require() and processes a job", async () => {
    const { stdout, stderr } = await run(
      process.execPath,
      [
        resolve(__dirname, "fixtures/cjs-consumer.cjs"),
        resolve(outDir, "index.js"),
      ],
      {
        cwd: root,
        env: { ...process.env, DATABASE_URL: postgres.getConnectionUri() },
      },
    );

    expect(JSON.parse(stdout)).toEqual({
      isArray: false,
      data: { from: "cjs" },
      retryLimit: 0,
    });
    expect(stderr).toBe("");
  });
});
