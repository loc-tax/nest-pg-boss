# `@loctax/nest-pg-boss`

<p align="center">
    Use <a href="https://github.com/timgit/pg-boss" target="_blank">pg-boss 9</a> in your Nest.js service!
<p align="center">

<p align="center">
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/v/@loctax/nest-pg-boss.svg" alt="NPM Version"/></a>
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/l/@loctax/nest-pg-boss.svg" alt="Package License"/></a>
    <a href="https://www.npmjs.com/package/@loctax/nest-pg-boss" target="_blank"><img src="https://img.shields.io/npm/dm/@loctax/nest-pg-boss.svg" alt="NPM Downloads"/></a>
    <a href="https://github.com/apricote/nest-pg-boss/actions?query=workflow%3A%22CI%22" target="_blank"><img src="https://img.shields.io/github/actions/workflow/status/apricote/nest-pg-boss/ci.yaml?branch=main" alt="CI Status"/></a>
</p>

## Installation

```bash
npm install @loctax/nest-pg-boss
```

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

For a list of available settings, check out the [pg-boss docs](https://github.com/timgit/pg-boss/blob/master/docs/readme.md#newoptions).

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
    private readonly foobarJobService: JobService<IFoobarJobData>
  ) {}

  async sendJob() {
    await this.foobarJobService.send({ foo: "oof", bar: true }, {});
  }
}
```

#### Process Jobs

Jobs can be processed by using the `@FoobarJob.Handle()` decorator.

```typescript
// service.ts
@Injectable()
class FoobarService {
  /* ... */

  @FoobarJob.Handle()
  async handleJob(job: Job<FoobarJobData>) {
    // do something
  }
}
```

You can optionally pass an object with [WorkOptions](https://github.com/timgit/pg-boss/blob/1f541263a906781efaf607f539340c9609db77df/types.d.ts#L119) to `.Handle()`:

```typescript
@FoobarJob.Handle({ teamSize: 10, teamConcurrency: 2 })
```

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
