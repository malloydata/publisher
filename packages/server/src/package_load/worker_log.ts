// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A winston entry as plain data, so it survives the message to the main
 * thread: an `Error`, whether the entry itself or a field of it, keeps its
 * name, message and stack, which copying it as JSON would drop. Each field is
 * copied on its own, so one that cannot be (a circular object) becomes its
 * string form without taking the others with it; a BigInt becomes its digits.
 */
export function plainLogEntry(entry: unknown): {
   level: string;
   message: string;
   meta: Record<string, unknown>;
} {
   const info = entry as Record<string, unknown>;
   const { level, message, ...fields } = info;
   // winston builds the entry from the Error itself for `logger.error(err)`,
   // and `format.errors` also reads an Error passed as the message.
   const error =
      entry instanceof Error
         ? entry
         : message instanceof Error
           ? message
           : undefined;
   if (error && fields.stack === undefined) fields.stack = error.stack;
   const json = (value: unknown): string | undefined =>
      JSON.stringify(value, (_key, v: unknown) =>
         v instanceof Error
            ? { name: v.name, message: v.message, stack: v.stack }
            : typeof v === "bigint"
              ? v.toString()
              : v,
      );
   const meta: Record<string, unknown> = {};
   for (const [key, value] of Object.entries(fields)) {
      try {
         const text = json(value);
         if (text !== undefined) meta[key] = JSON.parse(text);
      } catch {
         meta[key] = String(value);
      }
   }
   let text: string;
   if (typeof message === "string") {
      text = message;
   } else if (error) {
      text = error.message;
   } else {
      try {
         text = json(message) ?? String(message);
      } catch {
         text = String(message);
      }
   }
   return { level: String(level), message: text, meta };
}
