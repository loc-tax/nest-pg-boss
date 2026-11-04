import type { JobQueueOptions, JobSchedule } from "../queues";
import type { HandleOptions } from "../workers";

export interface HandlerMetadata {
  token: string;
  jobName: string;
  workOptions: HandleOptions;
  queueOptions?: JobQueueOptions;
  schedules?: JobSchedule[];
  subscribeTo?: { eventName: string }[];
}
