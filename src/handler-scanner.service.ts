import { Inject, Injectable, Logger, Optional } from "@nestjs/common";
import { MetadataScanner, ModulesContainer } from "@nestjs/core";
import {
  PG_BOSS_EVENT_METADATA,
  PG_BOSS_JOB_METADATA,
} from "./pg-boss.constants";
import type { PGBossModuleOptions } from "./interfaces/pg-boss-options.interface";
import type { PgBossLifecycleEvent } from "./on-pg-boss-event.decorator";
import { MODULE_OPTIONS_TOKEN } from "./pg-boss.module-definition";
import { HandlerMetadata } from "./interfaces/handler-metadata.interface";
import { toWorkHandler } from "./utils";
import type { WorkHandler } from "pg-boss";

@Injectable()
export class HandlerScannerService {
  private readonly logger = new Logger(this.constructor.name);

  constructor(
    private readonly metadataScanner: MetadataScanner,
    public readonly modulesContainer: ModulesContainer,
    @Optional()
    @Inject(MODULE_OPTIONS_TOKEN)
    private readonly options?: PGBossModuleOptions,
  ) {}

  public static exploreMethodMetadata(
    instancePrototype: unknown,
    methodKey: string,
  ): HandlerMetadata | null {
    const targetCallback = (instancePrototype as any)[methodKey];
    const metadata = Reflect.getMetadata(PG_BOSS_JOB_METADATA, targetCallback);
    if (metadata == null) {
      return null;
    }

    return metadata;
  }

  /**
   * Every `@Job.Handle()` method. `metadata.workOptions` are the effective options,
   * with the module's `defaultWorkOptions` filled in.
   */
  getJobHandlers(): {
    metadata: HandlerMetadata;
    callback: WorkHandler<unknown>;
  }[] {
    return this.scanMethods((instance, instancePrototype, methodName) => {
      const declared = HandlerScannerService.exploreMethodMetadata(
        instancePrototype,
        methodName,
      );

      if (declared == null) {
        return null;
      }

      const metadata = {
        ...declared,
        workOptions: {
          ...this.options?.defaultWorkOptions,
          ...declared.workOptions,
        },
      };

      return {
        metadata,
        callback: toWorkHandler(
          bindMethod(instance, methodName),
          metadata.workOptions,
        ),
      };
    });
  }

  /** Every `@OnPgBossEvent()` method. */
  getEventListeners(): {
    event: PgBossLifecycleEvent;
    listener: (...args: any[]) => unknown;
  }[] {
    return this.scanMethods((instance, instancePrototype, methodName) => {
      const method = (instancePrototype as Record<string, object>)[methodName];
      const event: PgBossLifecycleEvent | undefined =
        method && Reflect.getMetadata(PG_BOSS_EVENT_METADATA, method);

      return event
        ? { event, listener: bindMethod(instance, methodName) }
        : null;
    });
  }

  private scanMethods<Found>(
    inspect: (
      instance: object,
      instancePrototype: object,
      methodName: string,
    ) => Found | null,
  ): Found[] {
    // See https://github.com/owl1n/nest-queue/blob/master/src/queue.provider.ts
    const modules = [...this.modulesContainer.values()];
    const providersMap = modules
      .filter(({ providers }) => providers.size > 0)
      .map(({ providers }) => providers);

    const providerInstances = providersMap.flatMap((map) =>
      Array.from(map.values()),
    );

    return providerInstances
      .flatMap(({ instance }) => {
        // Many DI entries have no usable instance to scan: undefined for
        // unresolved scoped providers, primitives for `useValue` config tokens.
        // Falling through to `Object.getPrototypeOf({})` would scan
        // `Object.prototype` and run a `Reflect.getMetadata` lookup against
        // every method on it (`toString`, `hasOwnProperty`, ...) for every
        // such provider on bootstrap.
        if (instance == null || typeof instance !== "object") {
          return [];
        }

        const instancePrototype = Object.getPrototypeOf(instance);
        if (
          instancePrototype == null ||
          instancePrototype === Object.prototype
        ) {
          return [];
        }

        return this.metadataScanner
          .getAllMethodNames(instancePrototype)
          .map((methodName) =>
            inspect(instance, instancePrototype, methodName),
          );
      })
      .filter(notEmpty);
  }
}

function bindMethod(
  instance: object,
  methodName: string,
): (...args: any[]) => Promise<any> {
  return (instance as Record<string, (...args: any[]) => Promise<any>>)[
    methodName
  ].bind(instance);
}

function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
  if (value === null || value === undefined) {
    return false;
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const testDummy: TValue = value;
  return true;
}
