// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A spy for specs that prove read admission does no I/O: while
 * `ReadAdmission.decide` runs, any SQL a connection runs or any query a
 * Malloy runtime compiles to SQL is recorded, with what ran it. Admission
 * reads only what the load, bind and build recorded, so a spec expects
 * {@link AdmissionIOSpy.calls} to stay empty. In-process only: it patches
 * the prototypes this process loaded.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { QueryMaterializer } from "@malloydata/malloy";
import { ReadAdmission } from "../service/materialization/serve/read_admission";

export interface AdmissionIOSpy {
   /** What ran during admission: `runSQL <dialect>` or `compile`. */
   readonly calls: readonly string[];
   /** How many calls it saw at all, admission or not: proof it is wired. */
   seen(): number;
   /** How many times admission decided. */
   decisions(): number;
   restore(): void;
}

type Method = (this: unknown, ...args: unknown[]) => unknown;

export function spyAdmissionIO(): AdmissionIOSpy {
   const admitting = new AsyncLocalStorage<true>();
   const calls: string[] = [];
   let seen = 0;
   let decisions = 0;
   const undo: (() => void)[] = [];
   const patch = (proto: object, name: string, label: () => string) => {
      const target = proto as Record<string, Method>;
      const original = target[name];
      target[name] = function (this: unknown, ...args: unknown[]) {
         seen++;
         if (admitting.getStore()) calls.push(label());
         return original.apply(this, args);
      };
      undo.push(() => {
         target[name] = original;
      });
   };
   patch(DuckDBConnection.prototype, "runSQL", () => "runSQL duckdb");
   patch(PostgresConnection.prototype, "runSQL", () => "runSQL postgres");
   patch(QueryMaterializer.prototype, "getPreparedResult", () => "compile");
   const decide = ReadAdmission.prototype.decide as unknown as Method;
   (ReadAdmission.prototype as unknown as Record<string, Method>).decide =
      function (this: unknown, ...args: unknown[]) {
         decisions++;
         return admitting.run(true, () => decide.apply(this, args));
      };
   undo.push(() => {
      (ReadAdmission.prototype as unknown as Record<string, Method>).decide =
         decide;
   });
   return {
      calls,
      seen: () => seen,
      decisions: () => decisions,
      restore() {
         for (const u of undo.reverse()) u();
      },
   };
}
