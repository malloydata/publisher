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
