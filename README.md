# `@loctax/nest-pg-boss`

<p align="center">
    Use <a href="https://github.com/timgit/pg-boss" target="_blank">pg-boss 12</a> in your Nest.js service!
<p align="center">

<p align="center">
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/v/@loctax/nest-pg-boss.svg" alt="NPM Version"/></a>
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/l/@loctax/nest-pg-boss.svg" alt="Package License"/></a>
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/dm/@loctax/nest-pg-boss.svg" alt="NPM Downloads"/></a>
    <a href="https://github.com/apricote/nest-pg-boss/actions?query=workflow%3A%22CI%22" target="_blank"><img src="https://img.shields.io/github/actions/workflow/status/apricote/nest-pg-boss/ci.yaml?branch=main" alt="CI Status"/></a>
</p>

## Installation

```bash
npm install @loctax/nest-pg-boss pg-boss
```

Requires Node.js 22.12 or later, NestJS 11 or 12, and pg-boss 12. pg-boss also
needs its peer dependency `@opentelemetry/api`. npm and pnpm install it for you
by default. With a package manager that does not auto-install peers, add it
yourself, or loading pg-boss fails.

pg-boss is ESM-only. This package is CommonJS and loads it through Node's
`require(esm)`, so both CommonJS and ESM applications work. In TypeScript, use
`"module": "nodenext"` or `"node20"`, because `"node16"` rejects importing
pg-boss types from a CommonJS file.

Upgrading from a version for pg-boss 9? See
[Migrating from pg-boss 9](#migrating-from-pg-boss-9).

## Usage

### Setup

To begin using `@loctax/nest-pg-boss`, initialize the root module:

```ts
import { PGBossModule } from "@loctax/nest-pg-boss";

// app.module.ts
@Module({
  imports: [
    PGBossModule.forRootAsync({
      useFactory: (config: ConfigService) => ({
        application_name: "default",
        // Connection details
        host: config.get<string>("DB_HOST"),
        user: config.get<string>("DB_USERNAME"),
        password: config.get<string>("DB_PASSWORD"),
        database: config.get<string>("DB_DATABASE"),
        schema: "public",
        max: config.get<number>("DB_POOL_MAX"),
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
```

For a list of available settings, check out the [pg-boss docs](https://pgboss.io/api/constructor).

Besides the pg-boss settings, `onError` is required and receives pg-boss's `error`
events. The optional `onWarning` receives its
[`warning` events](https://pgboss.io/api/events), such as a growing queue backlog,
slow queries or a handler that outruns its timeout. Without it they go unheard.

### Jobs

```typescript
// jobs.ts
import { createJob } from "@loctax/nest-pg-boss";

interface IFoobarJobData {
  foo: string;
  bar: boolean;
}

const FoobarJob = createJob<IFoobarJobData>("foobar");
```

#### Queues

pg-boss only accepts jobs for a queue that exists, so the module creates the
queue of every job it knows about during startup. To configure a queue, pass any
[`createQueue` option](https://pgboss.io/api/queues) as the second argument:

```typescript
const DeadFoobarJob = createJob<IFoobarJobData>("foobar-dead", {
  deleteAfterSeconds: 30 * 24 * 60 * 60,
});

const FoobarJob = createJob<IFoobarJobData>("foobar", {
  policy: "short",
  retryLimit: 5,
  retryDelay: 30,
  retryBackoff: true,
  deleteAfterSeconds: 24 * 60 * 60,
  // Takes the job rather than a queue name, so its queue is created first.
  deadLetter: DeadFoobarJob,
});
```

| Policy            | Effect                                                                       |
| ----------------- | ---------------------------------------------------------------------------- |
| `standard`        | The default, with no limits.                                                 |
| `short`           | At most one job in the `created` state. Retries and active jobs don't count. |
| `singleton`       | At most one `active` job. Any number can wait.                               |
| `stately`         | At most one job in each of the `created`, `retry` and `active` states.       |
| `exclusive`       | At most one job across the `created`, `retry` and `active` states.           |
| `key_strict_fifo` | In order per `singletonKey`, which every job must then set.                  |

Limits apply per `singletonKey`. A `send()` that a policy turns away resolves to
`null`.

> **Jobs without a key all share one limit.** On a queue with a limiting policy, every
> job sent without `singletonKey` counts as the same key. On an `exclusive` queue, a
> second keyless job is dropped while the first is queued or active, even if its data
> is completely different. If each job should have its own limit, give it a key.

How startup behaves:

- A job given to `forJobs` has its queue ready before its `JobService` can be
  injected, so sending from a constructor or `onModuleInit` is safe.
- A job that only has a `@Job.Handle()` handler has its queue created during
  `onApplicationBootstrap`, before any worker starts.
- Instances with `disableWorkers` create queues the same way, since sending to a
  missing queue fails. Share the `createJob` declaration between the senders and
  the workers so they agree.
- On a queue that already exists, declared options that differ are updated, so
  changing a declaration takes effect on the next start. Options you remove from
  a declaration keep their stored value.
- pg-boss cannot change `policy` or `partition` on an existing queue, so a
  mismatch fails startup with an error rather than running with the wrong
  guarantee. Use a new queue name instead.
- Set `createQueues: false` on the module if queues are managed elsewhere. Sending
  to a queue that does not exist then rejects. Declared schedules and subscriptions are
  still applied, because a job declared them explicitly.

#### Create new Jobs

```typescript
// module.ts
import { PGBossModule } from "@loctax/nest-pg-boss";
import { FoobarService } from "./service.ts";

@Module({
  imports: PGBossModule.forJobs([FoobarJob]),
  providers: [FoobarService],
})
class FoobarModule {}
```

```typescript
// service.ts
import { JobService } from "@loctax/nest-pg-boss";
import { FoobarJob, IFoobarJobData } from "./jobs.ts";

@Injectable()
class FoobarService {
  constructor(
    @FoobarJob.Inject()
    private readonly foobarJobService: JobService<IFoobarJobData>,
  ) {}

  async sendJob() {
    await this.foobarJobService.send({ foo: "oof", bar: true }, {});
  }
}
```

#### Process Jobs

Jobs can be processed by using the `@FoobarJob.Handle()` decorator. The handler
receives one job at a time.

```typescript
// service.ts
@Injectable()
class FoobarService {
  /* ... */

  @FoobarJob.Handle()
  async handleJob(job: JobOf<typeof FoobarJob>) {
    // do something
  }
}
```

`JobOf<typeof FoobarJob>` is what the handler receives, including the job's data type,
so the handler can't drift from its job. `JobWithMetadataOf` is the same for
`includeMetadata: true`. You can optionally pass
[work options](https://pgboss.io/api/workers) to `.Handle()`:

```typescript
@FoobarJob.Handle({ localConcurrency: 10, pollingIntervalSeconds: 5 })
```

Setting `batchSize`, even to `1`, makes the handler receive an array instead. So
does `perJobResults`, because such a handler answers with one result per job:

```typescript
@FoobarJob.Handle({ batchSize: 10 })
async handleJobs(jobs: JobOf<typeof FoobarJob>[]) {
  // A rejection fails every job in the batch.
}
```

With `includeMetadata: true`, the job also has its metadata, and with
`transactional: true`, the transaction is the second argument, in both forms.

Handlers are typed with what really arrives. pg-boss stores job data as JSON, so
for a job declared with a `Date`, senders pass a `Date` and handlers receive a
string. `JobOf` already accounts for that, and `Jsonified<T>` names the type by itself.

An unknown option in `.Handle()`, such as a typo, is a compile error, also next to
valid options.

Pass `.Handle()` a literal, because `batchSize` and `perJobResults` decide what the
handler receives. For options chosen at runtime, type the variable as
`DefaultWorkOptions`, which leaves those two out.

#### Work through a backlog quickly

By default, a worker that has finished a job waits out `pollingIntervalSeconds`
before fetching the next one, so one worker manages about one job every two seconds
however long the queue is. pg-boss's `burstWhenBatchFull` is ignored at the default
batch size of one, and `burstWhenReadyExceeds` reacts to statistics that can be a
minute old. `drain` fills that gap for handlers that take one job at a time:

```typescript
@FoobarJob.Handle({ drain: true })
async handleJob(job: JobOf<typeof FoobarJob>) {}
```

After each handler that returns, that worker fetches again immediately, and it goes
back to polling at its first empty fetch.

- It still claims one job at a time, so jobs stay available to other workers, and
  each has its own expiry, heartbeat and retries. A crash only reruns the job in
  flight, and stopping the app finishes that job and leaves the rest untouched.
- A worker whose handler threw is not woken by `drain`, so a failing dependency does not
  burn through the retries of a whole backlog by itself. Other workers carry on while
  their own handlers succeed. This is not a guaranteed pause: pg-boss's burst options or
  a NOTIFY can still start the next fetch, and a `perJobResults` handler that returns
  failed results has returned, so it keeps draining.
- It works with `localConcurrency`, which also multiplies throughput, and with
  `batchSize`, including a final partial batch. Each of the workers counts as a
  separate pg-boss worker, for example in `getWipData()`. With `localGroupConcurrency`,
  they stay one registration, so a success wakes them all, including one that just failed.
- With `localGroupConcurrency` tiers, a backlog can sit idle for up to one polling
  interval. A worker can claim a job while its group is at the limit, and pg-boss then
  hands that job back. If the other workers have already gone idle, the job waits for
  the next poll. No job is lost and no retry is used. Set `pollingIntervalSeconds` to
  the longest wait you can accept.
- It removes the pause that polling put between jobs. If you relied on that as a
  rate limit, leave `drain` off, or pace the calls in your handler. Concurrency limits
  such as a `singleton` queue do not limit the rate of fast jobs.

For handlers that can accept an array, `batchSize` with `burstWhenBatchFull` is
faster still, but a batch is claimed, expires and settles together.

#### Module-wide defaults

```typescript
PGBossModule.forRoot({
  // ...
  defaultQueueOptions: { retryLimit: 0, deleteAfterSeconds: 7 * 24 * 60 * 60 },
  defaultWorkOptions: { drain: true, localConcurrency: 2 },
});
```

Options on `createJob` and `.Handle()` win over these.

- Defaults are treated like declared options, so an existing queue is updated to
  match at the next startup.
- An instance without `defaultQueueOptions` leaves those settings alone, so a
  send-only process cannot undo the defaults of the workers. Two instances with
  different defaults for the same queue overwrite each other at each start, so keep
  them in shared configuration. A job's retry settings are copied when it is sent,
  so jobs already queued keep theirs.
- `policy`, `partition` and `deadLetter` can only be set per job, because they
  describe one queue's purpose, and a default dead letter queue would apply to
  itself. Options that change a handler's arguments can only be set per handler.

#### Schedules

Declare recurring runs on the job, and they are kept in sync at every startup:

```typescript
const ReportJob = createJob<ReportData>("report", {
  schedules: [
    { cron: "0 6 * * *", data: { kind: "daily" }, options: { key: "daily" } },
    {
      // An RFC 5545 recurrence rule also works.
      cron: "FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17",
      data: { kind: "monthly" },
      options: { key: "monthly", tz: "Europe/Amsterdam", missed: "once" },
    },
  ],
});
```

When `schedules` is present, that list owns the queue's schedules, and any stored
key not in it is removed. So `schedules: []` removes them all, and a job with
declared schedules should not also add some through `JobService.schedule()`. Leave
the option out and nothing is synced, which is how to manage schedules at runtime.

Instances can start at the same time, because each writes the same end state, and an
unchanged schedule is not rewritten. An instance needs `schedule: true`, the default
outside `disableWorkers`, for jobs to actually be sent from a schedule, and at least
one instance must have it.

Only instances with `schedule: true` remove schedules. `disableWorkers` senders add and
update them but never remove any, so a sender released separately cannot delete a
schedule a newer worker added. It follows that a stale schedule goes away when an
instance that runs schedules starts.

**Every instance applies its own list at each start, and the last one to start wins.**
During a rolling deploy, a crash restart or a scale-up, a worker on an older release
removes the schedules a newer release added, and any instance on an older release
reverts the ones it changed. The recurring job then does not run until an instance on
the newer release restarts, and nothing reports this. So finish the deploy with a newer
instance starting last, and check `getSchedules` after adding a schedule.

#### The full API on `JobService`

`JobService` has pg-boss's job and queue operations, bound to the queue and typed
to the job's data:

| Purpose  | Methods                                                                                                                                           |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Create   | `send`, `sendAfter`, `sendThrottled`, `sendDebounced`, `insert`, `update`, `upsert`                                                               |
| Work     | `work`, `offWork`, `fetch`, `complete`, `fail`, `touch`                                                                                           |
| Manage   | `findJobs`, `cancel`, `resume`, `retry`, `deleteJob`, `redrive`, `previewRedrive`, `getDependencies`, `getDependents`                             |
| Schedule | `schedule`, `unschedule`, `getSchedule`, `getSchedules`                                                                                           |
| Queue    | `getQueue`, `getQueueStats`, `updateQueue`, `deleteQueue`, `deleteQueuedJobs`, `deleteStoredJobs`, `deleteAllJobs`, `getBlockedKeys`, `supervise` |
| Test     | `getSpy`                                                                                                                                          |

`complete`, `fail`, `touch`, `cancel` and `deleteJob` accept the job you fetched as
well as its id. Passing the job limits the call to that attempt, so once a job has
been retried, a late call from the earlier attempt changes nothing.

#### Flows

A flow is a set of jobs where a child waits for its parents. Handlers need nothing
special, because the jobs reach them as usual. Inject `FlowService`:

```typescript
import { FlowService, step } from "@loctax/nest-pg-boss";

const ids = await this.flows.run({
  extractOne: step(ExtractJob, { file: "one.csv" }),
  extractTwo: step(ExtractJob, { file: "two.csv" }),
  load: step(
    LoadJob,
    { table: "report" },
    {
      dependsOn: ["extractOne", "extractTwo"],
    },
  ),
});
// ids.load is the id of the load job.
```

Payloads and `dependsOn` are checked at compile time, and a flow needs at least two
steps. The whole flow is inserted atomically. Pass `{ db: tx }` as the second
argument to make it part of your own transaction, so it disappears if that rolls
back.

**A child of a failed parent stays blocked.** It does not fail or run by itself,
because retrying the parent can still release it. Either retry the parent, or give
up with `flows.cancelDependents(ExtractJob, id)`, which also cancels the children's
children. The same applies to a cancelled parent. Watching for failures is up to the
application, for example with a dead letter queue on the parent's job.

`flows.resolve()` releases waiting children right away, instead of at the next
`flowIntervalSeconds`. `flows.getDependencies(job, id)` and
`flows.getDependents(job, id)` show how jobs are linked.

#### Pub/sub

```typescript
const OrderPlaced = createEvent<OrderPlacedData>("order-placed");

const EmailJob = createJob<OrderPlacedData>("order-email", {
  subscribeTo: [OrderPlaced],
});

// Anywhere, with PubSubService injected. Every subscribed queue receives a job.
await this.pubSub.publish(OrderPlaced, { orderId: "a1" });
```

The compiler checks that a job's data can hold the event's payload. Subscribers are
created at startup, but dropping an event from `subscribeTo` does not unsubscribe,
because pg-boss has no way to list subscriptions. Call
`pubSub.unsubscribe(OrderPlaced, EmailJob)` once.

#### Lifecycle events

`onError` and `onWarning` on the module cover what happens while pg-boss starts. For
listeners that need dependencies, decorate a provider method:

```typescript
@Injectable()
class QueueHealth {
  constructor(private readonly metrics: MetricsService) {}

  @OnPgBossEvent("warning")
  recordWarning(warning: Warning) {
    this.metrics.increment("pg_boss.warning");
  }
}
```

The events are `error`, `warning`, `wip`, `bam`, `flow` and `stopped`. Listening
starts during `onApplicationBootstrap`. A listener that throws is logged and does
not affect pg-boss.

#### Testing

Import test helpers from the separate entry point, so they stay out of production
imports:

```typescript
import { createTestJob } from "@loctax/nest-pg-boss/testing";

const job = createTestJob(FoobarJob, { foo: "oof", bar: true });
await service.handleJob(job);
```

`createTestJob` fills in everything a real job has, including `signal` and metadata,
and sends the data through JSON like the real thing. Its third argument overrides
any field. The entry point also exports `TestClock` and its helpers.

For tests against a real database, set `__test__enableSpies: true` on the module and
wait on `jobService.getSpy().waitForJobWithId(id, "completed")`.

#### Everything else

Anything not tied to one queue is available by injecting `PgBoss`, which is also
exported here: `getQueues`, `getInstances`, `isInstalled`, `schemaVersion`,
`detectSchemaDrift`, `getReindexCommands`, `getBamStatus`, `getBamEntries`,
`getWipData`, `notifyWorker`, `previewSchedule`, `getDb` and the `is*` status
checks. A wrapper would read exactly the same. `getJobById` is deprecated by pg-boss
in favor of `findJobs`.

All of pg-boss's runtime exports are re-exported: `getConstructionPlans`,
`getMigrationPlans`, `getRollbackPlans`, `getUninstallPlans`, `getIndexBloatPlans`,
`percentile`, `addBins`, `states`, `policies`, `scheduleKinds`, `events`, `TestClock`
and the database adapters such as `fromKnex`. All of its types are re-exported too, so
an application needs no import from `pg-boss`. Two have new names, because they mean
something else here: pg-boss's `Job` is `PgBossJob`, since `Job` here is what
`createJob` returns, and its `WorkHandler` is `PgBossWorkHandler`. The type of a `db`
option or a transaction is `Db`, because pg-boss does not export `IDatabase` by that
name.

A test in this repository fails to compile when pg-boss gains a method or export that
has not been given a place here.

pg-boss's [CLI](https://pgboss.io/cli), [dashboard](https://pgboss.io/dashboard) and
[proxy](https://pgboss.io/proxy) are separate tools that work against the same
database. They need nothing from this package, and it does not wrap them.

#### Using your own database connection

To share a connection or a transaction, pass a `db` to the module or to a single call.
**Use an adapter from pg-boss rather than writing your own:** `fromKnex`, `fromKysely`,
`fromDrizzle`, `fromPrisma`, `fromPglite` and `fromBunSql` are all exported here.

A hand-written `executeSql` has to do all of the following, and a value typed `any`
gets past the compiler, so a mistake only shows up at runtime as an error such as
`Cannot read properties of undefined (reading 'length')`:

- Resolve to an object with a `rows` array, not to the rows themselves.
- For a query with several statements, resolve to an array with one such result per
  statement.
- Accept `$1`-style placeholders, including the same number used more than once.
- Leave a literal `?` alone, because pg-boss's own SQL uses it as a `jsonb` operator. A
  bridge to a library that fills `?` placeholders has to escape it.

The optional `listen` and `beginTransaction` methods are what `useListenNotify` and
`transactional` workers need from a custom `db`.

#### OpenTelemetry

Spans and metrics are on by default, and cost nothing until an OpenTelemetry SDK is
registered. To continue the sender's trace in the handler, as a `process {queue}` span,
turn on context propagation:

```typescript
PGBossModule.forRoot({
  // ...
  openTelemetry: { propagateContext: true },
});
```

If you also use `PgInstrumentation`, each polling worker produces about three `pg`
spans a second, because pg-boss does not suppress them. Setting
`new PgInstrumentation({ requireParentSpan: true })` removes those from idle workers and
keeps the ones inside jobs.

#### Things to know about pg-boss

- `expireInSeconds` is capped at 24 hours, and a handler still running by then fails,
  even with a healthy heartbeat. Split longer work into steps, for example with a flow.
  `heartbeatSeconds`, with a minimum of 10, is an addition to expiry and not a way
  around it. It notices a dead worker sooner than a long expiry would.
- `update()` replaces a job's data rather than merging into it.
- `retryDelayMax` needs `retryBackoff: true` declared next to it.
- `insert()` resolves to the job ids only with `{ returnId: true }`, otherwise to
  `null`.
- A policy applies to the whole queue, not to a job. The limit is per `singletonKey`,
  and jobs without a key count as one group, so on an `exclusive` queue a second job
  without a key is turned away.
- `job.signal` aborts when pg-boss shuts down, and, on a queue with
  `heartbeatSeconds`, when a heartbeat finds that the job's claim was lost.
- `offWork()` waits for current handlers by default. From inside a handler, pass
  `{ wait: false }`, or it waits for itself forever.

#### Known limits

These come from pg-boss, and this package cannot fix them:

- **`groupConcurrency` is a soft limit.** Workers that fetch at the same moment do
  not see each other's claims, so more jobs of a group can run at once than the limit.
  With `groupConcurrency: 1` and `localConcurrency: 4`, up to 3 ran at once in our
  test without this package involved. For a strict limit on one instance, use
  `localGroupConcurrency`. It does not coordinate across instances. For a strict
  limit across instances, use one queue per group with the `singleton` policy.
- **Subscriptions cannot be listed,** so `subscribeTo` can add them but not remove
  them.
- **There is no way to hand back a claimed job without using a retry,** because
  pg-boss keeps that operation internal. That is one reason `drain` claims jobs one at
  a time rather than in batches.
- **Work cannot run longer than 24 hours in one job,** because that is the cap on
  `expireInSeconds`.
- **TypeScript needs `skipLibCheck: true`.** pg-boss 12.38's declaration files refer
  to a `ResolvedConstructorOptions` type they do not export, and to `pg` types they do
  not depend on, so strict checking of `node_modules` fails with or without this
  package.
- **Migrating a pg-boss 9 schema** is not possible, as described below.

#### Inspect registered handlers

`HandlerScannerService` is exported from the global module. It returns every
`@Job.Handle()` method found in the Nest container together with its job name
and `WorkOptions`, which is useful for reporting per-queue capacity (e.g. a
`localConcurrency` gauge next to the queue backlog):

```typescript
@Injectable()
class QueueMetricsService implements OnApplicationBootstrap {
  constructor(private readonly handlerScanner: HandlerScannerService) {}

  onApplicationBootstrap() {
    for (const { metadata } of this.handlerScanner.getJobHandlers()) {
      console.log(metadata.jobName, metadata.workOptions.localConcurrency ?? 1);
    }
  }
}
```

Call it from `onApplicationBootstrap` or later so every provider has been
instantiated; the scan only sees providers that already have an instance.

`metadata.workOptions` holds the effective options, including `defaultWorkOptions`. So
`localConcurrency ?? 1` is the number of workers this instance runs for the handler,
also with `drain`.

## Migrating from pg-boss 9

Version 6 of this package moves from pg-boss 9 to pg-boss 12. See the pg-boss
release notes for [10.0.0](https://github.com/timgit/pg-boss/releases/tag/10.0.0),
[11.0.0](https://github.com/timgit/pg-boss/releases/tag/11.0.0) and
[12.0.0](https://github.com/timgit/pg-boss/releases/tag/12.0.0) for the full list.

### Plan the database move first

pg-boss 12 cannot migrate a pg-boss 9 schema. `start()` refuses it with
`Cannot migrate pg-boss schema from version 20`, and leaves the old schema
untouched. Existing jobs and schedules do not carry over by themselves.

The simplest path is to point the new version at a new `schema`, such as
`schema: "pgboss_v12"`:

1. Deploy the new version with the new schema. It creates its own tables.
2. Keep instances of the old version running until their schema has drained, or
   copy the queued jobs across. The 10.0.0 release notes describe how.
3. Create recurring schedules again, because they live in the old schema.
4. Drop the old schema once it is empty.

PostgreSQL 13 or later is required.

### Retries now happen by default

The default `retryLimit` went from 0 to 2, so a handler that throws now runs up to
three times. **Check that your handlers are safe to run again.** Where they are
not, declare `retryLimit: 0`:

```typescript
const ChargeJob = createJob<ChargeData>("charge", { retryLimit: 0 });
```

This package keeps pg-boss's default rather than overriding it.

### Retention moved to the queue

Completed jobs now stay in the job table and are deleted after 7 days. Queued jobs
expire after 14 days. The settings below no longer work at the top level of `forRoot`.
Declare them for every job with `defaultQueueOptions`, per job on `createJob`, or per
`send()`, and note that only the names in seconds remain:

| Before                                                                            | After                                             |
| --------------------------------------------------------------------------------- | ------------------------------------------------- |
| `retryLimit`, `retryBackoff`                                                      | Same names                                        |
| `expireInSeconds`, `expireInMinutes`, `expireInHours`                             | `expireInSeconds`, converted to seconds           |
| `retentionSeconds`, `retentionMinutes`, `retentionHours`, `retentionDays`         | `retentionSeconds`, converted to seconds          |
| `deleteAfterSeconds`, `deleteAfterMinutes`, `deleteAfterHours`, `deleteAfterDays` | `deleteAfterSeconds`, converted to seconds        |
| `archiveCompletedAfterSeconds`, `archiveFailedAfterSeconds`                       | No replacement, because the archive table is gone |

`retryDelay` on `forRoot` stays, and is only the delay between connection attempts
on startup. To delay job retries, declare `retryDelay` on `createJob`.

### Code changes

| Before                                                      | After                                                                   |
| ----------------------------------------------------------- | ----------------------------------------------------------------------- |
| `import PGBoss from "pg-boss"`                              | `import { PgBoss } from "pg-boss"`                                      |
| `PGBoss.SendOptions`                                        | `import type { SendOptions } from "@loctax/nest-pg-boss"`               |
| `PGBoss.Job<T>`, in a handler                               | `JobOf<typeof SomeJob>`                                                 |
| `@Inject(PGBoss)`, `app.get(PGBoss)`                        | `@Inject(PgBoss)`, `app.get(PgBoss)`                                    |
| `noSupervisor: true`                                        | `supervise: false`                                                      |
| `noScheduling: true`                                        | `schedule: false`                                                       |
| `teamConcurrency`                                           | `localConcurrency`, the number of workers polling on this instance      |
| `teamSize`, with a high `teamConcurrency`                   | `localConcurrency`                                                      |
| `teamSize`, with a low `teamConcurrency`                    | `drain: true`, if the point was to get through a backlog quickly        |
| `teamRefill`                                                | No replacement. `drain: true` covers the same need                      |
| `newJobCheckInterval*`                                      | `pollingIntervalSeconds`, with a minimum of 0.5                         |
| `useSingletonQueue` with `enforceSingletonQueueActiveLimit` | A `stately` queue                                                       |
| `singletonKey` on its own                                   | An `exclusive` queue, with the same limit across queued and active jobs |
| `jobService.sendOnce(data, options, key)`                   | An `exclusive` queue and `singletonKey: key`, which is the same limit   |
| `jobService.sendSingleton(data, options)`                   | A `short` queue and `send()`, the closest fit (see below)               |
| `onComplete`                                                | `deadLetter` on `createJob`, which only receives failed jobs            |
| `stopOptions: { destroy }`                                  | `stopOptions: { close }`, which now defaults to `true`                  |
| `boss.getQueueSize(name)`                                   | `boss.getQueues([name])` or `boss.findJobs(name)`                       |
| `rowCount` from `boss.insert()`                             | `returnId: true`, then the length of the result                         |
| The `expired` job state                                     | `failed`, which now covers jobs that expired                            |

- A `stately` queue limits `created` and `retry` separately, where `useSingletonQueue`
  limited them together, so a retrying job and a newly queued one can coexist.
- `sendSingleton` allowed one job across the `created` and `retry` states. A `short`
  queue only limits `created`, so a retrying job and a newly queued one can coexist.
- A package entry points map now applies, so deep imports such as
  `@loctax/nest-pg-boss/dist/utils` no longer resolve. Import from the package root.
- `JobService.schedule` and `send` no longer need an empty options object.
- A hand-written `db` must follow the contract in
  [Using your own database connection](#using-your-own-database-connection). pg-boss now
  reads `rows` from every result, so one that returns plain rows crashes at runtime.
- A job with a `Date` or other non-JSON field in its data now needs its handler typed
  with what arrives, which `Jsonified<T>` names. Where this used to compile, the
  handler was reading a string as a `Date`.
- `@Job.Handle()` handlers without `batchSize` still receive one job, and handlers
  with `batchSize` still receive an array.
- Module options from pg-boss 9 that pg-boss would silently ignore now throw at
  startup, including `stopOptions.destroy`, so an old configuration fails loudly
  instead of quietly losing its retention or shutdown behavior. Old `Handle()` options
  (`teamSize`, `teamConcurrency`, `teamRefill`, `newJobCheckInterval*`,
  `enforceSingletonQueueActiveLimit`) are compile errors. From plain JavaScript, or
  through `any`, pg-boss ignores them, so check for those by hand.
- `Handle()` no longer compiles with `batchSize` or `perJobResults` hidden behind a
  wider type, such as a variable of type `WorkOptions`, because then the handler's
  job shape is unknown. Pass them as literal options. Options in a plain JavaScript
  or `any` value skip these type checks, and the runtime value decides the shape.
- `getJobHandlers()` from `HandlerScannerService` returns a `callback` in the form
  `work()` expects, which takes an array.
- Queue names may only contain letters, numbers, underscores, hyphens, periods and
  forward slashes. A name with anything else, such as a colon, fails at startup.
- `JobService.insert(jobs, options)` keeps its signature. Direct callers of pg-boss
  must pass the queue name first: `boss.insert(name, jobs, options)`.
- pg-boss's `complete`, `fail`, `cancel` and `getJobById` now need the queue name
  as their first argument.
- If your tests replace `PgBoss` with a mock, set `createQueues: false` on the
  module. Otherwise the mock needs `createQueue`, `getQueue` and `updateQueue`, with
  `getQueue` returning the stored queue or `null`.

## Test

```bash
# everything
$ pnpm test

# unit tests only, no Docker required
$ pnpm test:unit

# e2e tests only, starts a Postgres testcontainer and needs Docker
$ pnpm test:e2e
```

## License

`@loctax/nest-pg-boss` is [MIT licensed](LICENSE).
