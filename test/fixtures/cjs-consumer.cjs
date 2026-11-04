// Boots the compiled library as a plain CommonJS application against a real
// database: the `require()` chain into the ESM-only pg-boss is the thing under test.
"use strict";

require("reflect-metadata");
const { Module } = require("@nestjs/common");
const { NestFactory } = require("@nestjs/core");
const { PgBoss } = require("pg-boss");
const { PGBossModule, createJob } = require(process.argv[2]);

const CjsJob = createJob("cjs-consumer", { retryLimit: 0 });

class CjsHandler {
  received = [];

  async handle(job) {
    this.received.push(job);
  }
}
Reflect.decorate(
  [CjsJob.Handle({ pollingIntervalSeconds: 0.5 })],
  CjsHandler.prototype,
  "handle",
  Object.getOwnPropertyDescriptor(CjsHandler.prototype, "handle"),
);

// oxlint-disable-next-line typescript/no-extraneous-class -- a Nest module is an empty class carrying a decorator.
class AppModule {}
Reflect.decorate(
  [
    Module({
      imports: [
        PGBossModule.forRoot({
          connectionString: process.env.DATABASE_URL,
          onError: (error) => console.error(error),
        }),
        PGBossModule.forJobs([CjsJob]),
      ],
      providers: [CjsHandler],
    }),
  ],
  AppModule,
);

const main = async () => {
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: false,
  });
  const handler = app.get(CjsHandler);
  await app.get(CjsJob.ServiceProvider.provide).send({ from: "cjs" }, {});

  const deadline = Date.now() + 30_000;
  while (handler.received.length === 0 && Date.now() < deadline) {
    // oxlint-disable-next-line no-await-in-loop -- sequential poll until the job arrives.
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  const queue = await app.get(PgBoss).getQueue("cjs-consumer");
  await app.close();

  console.log(
    JSON.stringify({
      isArray: Array.isArray(handler.received[0]),
      data: handler.received[0]?.data,
      retryLimit: queue?.retryLimit,
    }),
  );
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
