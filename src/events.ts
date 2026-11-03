import { Injectable } from "@nestjs/common";
import { PgBoss, type SendOptions } from "pg-boss";

/** A pub/sub event, made by `createEvent`. `Payload` is only a type. */
export interface PgBossEvent<Payload extends object = any> {
  eventName: string;
  /** Never set at runtime. It carries the payload type for the compiler. */
  readonly payload?: Payload;
}

/**
 * Declares a pub/sub event. pg-boss delivers a published event by sending a job
 * to every queue subscribed to it, so a subscriber is a job with
 * `subscribeTo: [TheEvent]` and an ordinary `@Job.Handle()`.
 */
export const createEvent = <Payload extends object>(
  eventName: string,
): PgBossEvent<Payload> => ({ eventName });

@Injectable()
export class PubSubService {
  constructor(private readonly boss: PgBoss) {}

  async publish<Payload extends object>(
    event: PgBossEvent<Payload>,
    data: Payload,
    options?: SendOptions,
  ): Promise<void> {
    await this.boss.publish(event.eventName, data, options);
  }

  /** For subscriptions decided at runtime. Prefer `subscribeTo` on `createJob`. */
  async subscribe(
    event: PgBossEvent,
    job: { queueName: string },
  ): Promise<void> {
    await this.boss.subscribe(event.eventName, job.queueName);
  }

  /**
   * `subscribeTo` never removes a subscription, because pg-boss offers no way to
   * list them. After dropping an event from `subscribeTo`, call this once.
   */
  async unsubscribe(
    event: PgBossEvent,
    job: { queueName: string },
  ): Promise<void> {
    await this.boss.unsubscribe(event.eventName, job.queueName);
  }
}
