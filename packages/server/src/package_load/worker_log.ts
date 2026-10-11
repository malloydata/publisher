// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A winston entry as plain data, so it survives the message to the main
 * thread: an `Error`, whether the entry itself or a field of it, keeps its
 * name, message and stack, which copying it as JSON would drop.
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
   const plain = (value: unknown): string => {
      try {
         return (
            JSON.stringify(value, (_key, v: unknown) =>
               v instanceof Error
                  ? { name: v.name, message: v.message, stack: v.stack }
                  : v,
            ) ?? String(value)
         );
      } catch {
         return String(value);
      }
   };
   let meta: Record<string, unknown>;
   try {
      meta = JSON.parse(plain(fields)) as Record<string, unknown>;
   } catch {
      meta = {};
   }
   return {
      level: String(level),
      message:
         typeof message === "string"
            ? message
            : error
              ? error.message
              : plain(message),
      meta,
   };
}
