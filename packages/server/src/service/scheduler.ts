import { Model } from "./model";
import { components } from "../api";
import cron from "node-cron";

type ApiSource = components["schemas"]["Source"];
type ApiView = components["schemas"]["View"];
type ApiQuery = components["schemas"]["Query"];
type ApiSchedule = components["schemas"]["Schedule"];

const SCHEDULE_ANNOTATION = "# schedule";

class Schedule {
   private model: Model;
   private source: ApiSource | undefined;
   private view: ApiView | undefined;
   private query: ApiQuery | undefined;

   private schedule: string;
   private action: string;
   private connection: string;
   private task: cron.ScheduledTask;
   private lastRunTime: number | undefined;
   private lastRunStatus: string;

   public constructor(
      model: Model,
      source: ApiSource | undefined,
      view: ApiView | undefined,
      query: ApiQuery | undefined,
      annotation: string,
   ) {
      this.model = model;
      this.source = source;
      this.view = view;
      this.query = query;
      const { origSchedule, cronSchedule, action, connection } =
         Schedule.parseAnnotation(annotation);
      this.schedule = origSchedule;
      this.action = action;
      this.connection = connection;
      this.lastRunTime = undefined;
      this.lastRunStatus = "unknown";

      this.task = cron.schedule(cronSchedule, () => {
         this.lastRunTime = Date.now();
         this.lastRunStatus = "ok";
      });
   }

   public stop() {
      this.task.stop();
   }

   public get(): ApiSchedule {
      const query = this.source
         ? `${this.source.name} > ${this.view?.name}`
         : this.query?.name;
      return {
         resource: `${this.model.getPath()}  > ${query}`,
         schedule: this.schedule,
         action: this.action,
         connection: this.connection,
         lastRunTime: this.lastRunTime,
         lastRunStatus: this.lastRunStatus,
      };
   }

   private static parseAnnotation(annotation: string) {
      // Example schedule annotation
      // # schedule @hourly materialize duckdb
      // # schedule "0 * * * *" materialize duckdb
      // TODO: Don't split quoted strings.  Use regex instead of split.
      const annotationSplit = annotation.split(/\s+/);
      if (annotationSplit.length != 6) {
         console.log("Length: " + annotationSplit.length);
         throw new Error(
            "Invalid annotation string does not have enough parts: " +
               annotation,
         );
      }

      if (annotationSplit[0] != "#") {
         throw new Error(
            "Invalid annotation string does not have start with #: " +
               annotationSplit[0],
         );
      }

      if (annotationSplit[1] != "schedule") {
         throw new Error(
            "Invalid annotation string does not start with schedule command: " +
               annotationSplit[1],
         );
      }

      const standardCron = Schedule.translateNonStandardCron(
         annotationSplit[2],
      );
      if (!cron.validate(standardCron)) {
         throw new Error(
            "Invalid annotation string does not have valid cron schedule: " +
               standardCron,
         );
      }

      if (
         annotationSplit[3] != "materialize" &&
         annotationSplit[3] != "report"
      ) {
         throw new Error(
            "Invalid annotation string unrecognized command: " + annotation,
         );
      }

      // TODO: Validate connection exists.

      return {
         origSchedule: annotationSplit[2],
         cronSchedule: standardCron,
         action: annotationSplit[3],
         connection: annotationSplit[4],
      };
   }

   public static translateNonStandardCron(schedule: string): string {
      let standardCron = schedule;
      switch (schedule) {
         case "@yearly":
         case "@anually":
            standardCron = "0 0 1 1 *";
            break;
         case "@monthly":
            standardCron = "0 0 1 * *";
            break;
         case "@weekly":
            standardCron = "0 0 * * 0";
            break;
         case "@daily":
         case "@midnight":
            standardCron = "0 0 * * *";
            break;
         case "@hourly":
            standardCron = "0 * * * *";
            break;
         case "@minutely":
            standardCron = "* * * * *";
      }
      return standardCron;
   }
}

export class Scheduler {
   private schedules: Schedule[];

   private constructor(schedules: Schedule[]) {
      this.schedules = schedules;
   }

   public static create(models: Map<string, Model>): Scheduler {
      const schedules: Schedule[] = [];

      models.forEach((m) => {
         m.getSources()?.forEach((s) => {
            s.views?.forEach((v) => {
               v.annotations?.forEach((a) => {
                  if (a.startsWith(SCHEDULE_ANNOTATION)) {
                     schedules.push(new Schedule(m, s, v, undefined, a));
                  }
               });
            });
         });

         m.getQueries()?.forEach((q) => {
            q.annotations?.forEach((a) => {
               if (a.startsWith(SCHEDULE_ANNOTATION)) {
                  schedules.push(new Schedule(m, undefined, undefined, q, a));
               }
            });
         });
      });

      return new Scheduler(schedules);
   }

   public list(): ApiSchedule[] {
      return this.schedules.map((s) => s.get());
   }
}
