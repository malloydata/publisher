// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { connectPgClient } from "../../../src/service/connection";

/** The Postgres superuser the admin integration specs provision with; they skip without it. */
export const SUPERUSER = process.env.PUBLISHER_TEST_ADMIN_SUPERUSER;

/** Runs one statement as the superuser against `database`, on its own connection. */
export async function superuser(
   sql: string,
   database = "postgres",
): Promise<Record<string, unknown>[]> {
   const client = await connectPgClient(
      "test",
      `${SUPERUSER} dbname=${database}`,
   );
   try {
      return (await client.query(sql)).rows;
   } finally {
      await client.end();
   }
}

/** Polls `read` until it answers, for a test waiting on another connection. */
export async function until<T>(read: () => Promise<T | undefined>): Promise<T> {
   for (let i = 0; i < 1500; i++) {
      const value = await read();
      if (value !== undefined) return value;
      await new Promise((resolve) => setTimeout(resolve, 10));
   }
   throw new Error("timed out waiting");
}
