import { describe, expect, it } from "bun:test";
import winston from "winston";
import { plainLogEntry } from "./worker_log";

/** The entries a winston logger builds from each call, as its `write` sees them. */
function entriesOf(log: (logger: winston.Logger) => void): unknown[] {
   const logger = winston.createLogger({
      level: "info",
      transports: [new winston.transports.Console({ silent: true })],
   });
   const entries: unknown[] = [];
   (logger as unknown as { write: (entry: unknown) => boolean }).write = (
      entry,
   ) => {
      entries.push(entry);
      return true;
   };
   log(logger);
   return entries;
}

describe("plainLogEntry", () => {
   it("keeps an Error passed as meta, which a JSON copy reduces to {}", () => {
      const error = new Error("boom");
      expect(JSON.stringify(error)).toBe("{}");
      const [entry] = entriesOf((logger) => logger.error("Failed", error));
      const plain = plainLogEntry(entry);
      expect(plain.level).toBe("error");
      expect(plain.message).toBe("Failed boom");
      expect(plain.meta.stack).toBe(error.stack);
   });

   it("keeps an Error logged on its own", () => {
      const error = new Error("boom");
      const [entry] = entriesOf((logger) => logger.error(error));
      const plain = plainLogEntry(entry);
      expect(plain.message).toBe("boom");
      expect(plain.meta.stack).toBe(error.stack);
   });

   it("keeps an Error nested in meta, and the other fields", () => {
      const error = new Error("boom");
      const [entry] = entriesOf((logger) =>
         logger.warn("Failed", { modelPath: "m.malloy", cause: error }),
      );
      const plain = plainLogEntry(entry);
      expect(plain.meta.modelPath).toBe("m.malloy");
      expect(plain.meta.cause).toEqual({
         name: "Error",
         message: "boom",
         stack: error.stack,
      });
   });

   it("takes calls in every form, logger.log included", () => {
      const entries = entriesOf((logger) => {
         logger.log("info", "one", { a: 1 });
         logger.verbose("two");
         logger.log({ level: "warn", message: "three" });
      });
      expect(entries.map((e) => plainLogEntry(e).message)).toEqual([
         "one",
         "two",
         "three",
      ]);
   });

   it("keeps the other fields when one cannot be copied as JSON", () => {
      const circular: Record<string, unknown> = {};
      circular.self = circular;
      const [entry] = entriesOf((logger) =>
         logger.warn("x", { modelPath: "m.malloy", rows: 10n, loop: circular }),
      );
      expect(plainLogEntry(entry)).toEqual({
         level: "warn",
         message: "x",
         meta: { modelPath: "m.malloy", rows: "10", loop: "[object Object]" },
      });
   });
});
