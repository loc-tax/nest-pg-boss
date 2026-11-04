import {
  Global,
  Inject,
  Logger,
  Module,
  type DynamicModule,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";
import { MetadataScanner, ModuleRef } from "@nestjs/core";
import { PgBoss } from "pg-boss";
import { defer, lastValueFrom } from "rxjs";
import {
  applyDisableWorkersDefaults,
  assertValidModuleOptions,
  handleRetry,
  markBootstrapped,
  stopAfterFailedStartup,
} from "./utils";
import { PGBossJobModule } from "./pg-boss-job.module";
import { HandlerScannerService } from "./handler-scanner.service";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";
import type { Job } from "./job.service";
import type { PgBossLifecycleEvent } from "./on-pg-boss-event.decorator";
import { FlowService } from "./flow.service";
import { PubSubService } from "./events";
import { ensureQueueOnce } from "./queues";
import { registerWorkers } from "./workers";
import {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN,
  type ASYNC_OPTIONS_TYPE,
  type OPTIONS_TYPE,
} from "./pg-boss.module-definition";

@Global()
@Module({
  providers: [
    MetadataScanner,
    HandlerScannerService,
    FlowService,
    PubSubService,
  ],
  // Exported so consumers can enumerate the registered handlers and their
  // WorkOptions (e.g. to report per-queue capacity metrics) without
  // duplicating the metadata scan. The options are exported for the job service
  // factories in `forJobs`, which live in another module.
  exports: [
    HandlerScannerService,
    FlowService,
    PubSubService,
    MODULE_OPTIONS_TOKEN,
  ],
})
export class PGBossModule
  extends ConfigurableModuleClass
  implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(this.constructor.name);
  private instance: PgBoss | undefined;
  private readonly attachedListeners: [
    PgBossLifecycleEvent,
    (...args: unknown[]) => void,
  ][] = [];

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly handlerScannerService: HandlerScannerService,
    @Inject(MODULE_OPTIONS_TOKEN)
    private readonly options: PGBossModuleOptions,
  ) {
    super();
  }

  static forRoot(options: typeof OPTIONS_TYPE): DynamicModule {
    const instanceProvider = {
      provide: PgBoss,
      useFactory: async () => await this.createInstanceFactory(options),
    };

    const dynamicModule = super.forRoot(options);
    if (!dynamicModule.providers) {
      dynamicModule.providers = [];
    }
    dynamicModule.providers.push(instanceProvider);
    dynamicModule.exports ||= [];
    dynamicModule.exports.push(instanceProvider);

    return dynamicModule;
  }

  static forRootAsync(options: typeof ASYNC_OPTIONS_TYPE): DynamicModule {
    const instanceProvider = {
      provide: PgBoss,
      useFactory: async (pgBossModuleOptions: PGBossModuleOptions) => {
        return await this.createInstanceFactory(pgBossModuleOptions);
      },
      inject: [MODULE_OPTIONS_TOKEN],
    };

    const dynamicModule = super.forRootAsync(options);
    if (!dynamicModule.providers) {
      dynamicModule.providers = [];
    }
    dynamicModule.providers.push(instanceProvider);
    if (!dynamicModule.exports) {
      dynamicModule.exports = [];
    }
    dynamicModule.exports.push(instanceProvider);

    return dynamicModule;
  }

  private static async createInstanceFactory(options: PGBossModuleOptions) {
    assertValidModuleOptions(options);
    const resolvedOptions = applyDisableWorkersDefaults(options);

    const pgBoss = await lastValueFrom(
      defer(async () => {
        const boss = new PgBoss(resolvedOptions);

        boss.on("error", resolvedOptions.onError);
        // Before start(), which can already emit warnings.
        if (resolvedOptions.onWarning) {
          boss.on("warning", resolvedOptions.onWarning);
        }

        try {
          await boss.start();
        } catch (error) {
          // A partial start leaves pg-boss's pool and timers running, and a
          // retry below would build another instance next to them.
          await stopAfterFailedStartup(boss);
          throw error;
        }
        return boss;
      }).pipe(
        handleRetry(
          resolvedOptions.retryAttempts,
          resolvedOptions.retryDelay,
          resolvedOptions.verboseRetryLog,
          resolvedOptions.toRetry,
        ),
      ),
    );

    return pgBoss;
  }

  static forJobs(jobs: Job[]) {
    return {
      module: PGBossJobModule,
      providers: jobs.map((job) => job.ServiceProvider),
      exports: jobs.map((job) => job.ServiceProvider.provide),
    };
  }

  onModuleInit() {
    this.instance = this.moduleRef.get<PgBoss>(PgBoss);
  }

  async onApplicationBootstrap(): Promise<void> {
    try {
      this.attachEventListeners();
      await this.createHandlerQueues();
      await this.setupWorkers();
      if (this.instance) {
        markBootstrapped(this.instance);
      }
    } catch (error) {
      if (this.instance) {
        await stopAfterFailedStartup(this.instance);
      }
      this.detachEventListeners();
      throw error;
    }
  }

  async onModuleDestroy(): Promise<void> {
    try {
      if (this.instance) {
        await this.instance.stop(this.options.stopOptions);
      }
    } catch (e) {
      this.logger.error((e as Error).message);
    }
    this.detachEventListeners();
  }

  /**
   * The queues of jobs given to `forJobs` already exist by now, because their
   * service factories create them. This covers jobs that only have a handler.
   */
  private async createHandlerQueues() {
    if (!this.instance) {
      throw new Error(
        "createHandlerQueues must be called after onApplicationBootstrap",
      );
    }

    const boss = this.instance;
    const declarations = new Map(
      this.handlerScannerService.getJobHandlers().map(({ metadata }) => [
        metadata.jobName,
        {
          queueName: metadata.jobName,
          queueOptions: metadata.queueOptions,
          schedules: metadata.schedules,
          subscribeTo: metadata.subscribeTo,
        },
      ]),
    );

    await Promise.all(
      [...declarations.values()].map((declaration) =>
        ensureQueueOnce(boss, declaration, this.options),
      ),
    );
  }

  private attachEventListeners() {
    if (!this.instance) {
      throw new Error(
        "attachEventListeners must be called after onApplicationBootstrap",
      );
    }

    for (const {
      event,
      listener,
    } of this.handlerScannerService.getEventListeners()) {
      const callback = (...args: unknown[]) => {
        void Promise.resolve()
          .then(() => listener(...args))
          .catch((error: unknown) => {
            this.logger.error(
              `A listener for the pg-boss "${event}" event failed: ${error instanceof Error ? error.message : String(error)}`,
              error instanceof Error ? error.stack : undefined,
            );
          });
      };
      this.instance.on(event as "warning", callback);
      this.attachedListeners.push([event, callback]);
    }
  }

  /** So a stopped instance holds no reference to providers Nest has destroyed. */
  private detachEventListeners() {
    for (const [event, callback] of this.attachedListeners.splice(0)) {
      this.instance?.off(event as "warning", callback);
    }
  }

  private async setupWorkers() {
    if (this.options.disableWorkers) {
      this.logger.log(
        "disableWorkers is set so will not attempt to register them",
      );
      return;
    }

    if (!this.instance) {
      throw new Error(
        "setupWorkers must be called after onApplicationBootstrap",
      );
    }

    const jobHandlers = this.handlerScannerService.getJobHandlers();

    await Promise.all(
      jobHandlers.map(async (handler) => {
        if (!this.instance) {
          throw new Error(
            "setupWorkers must be called after onApplicationBootstrap",
          );
        }

        const workerIDs = await registerWorkers(
          this.instance,
          handler.metadata.jobName,
          handler.metadata.workOptions,
          handler.callback,
        );
        this.logger.log(
          { workerIDs, jobName: handler.metadata.jobName },
          "Registered Worker",
        );
      }),
    );
  }

  public getBossInstance() {
    return this.instance;
  }
}
