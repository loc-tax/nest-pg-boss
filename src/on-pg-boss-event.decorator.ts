import { SetMetadata } from "@nestjs/common";
import type { PgBossEventMap } from "pg-boss";
import { PG_BOSS_EVENT_METADATA } from "./pg-boss.constants";

export type PgBossLifecycleEvent = keyof PgBossEventMap;

/**
 * Calls a provider method on a pg-boss lifecycle event, for listeners that need
 * injected dependencies such as a metrics service. Listening starts during
 * `onApplicationBootstrap`, so use `onError` and `onWarning` on the module for
 * anything raised while pg-boss starts. A listener that throws is logged and does
 * not affect pg-boss.
 */
export const OnPgBossEvent =
  <Event extends PgBossLifecycleEvent>(event: Event) =>
  <Method extends (...args: PgBossEventMap[Event]) => unknown>(
    target: object,
    propertyKey: string | symbol,
    descriptor: TypedPropertyDescriptor<Method>,
  ): void => {
    SetMetadata(PG_BOSS_EVENT_METADATA, event)(target, propertyKey, descriptor);
  };
