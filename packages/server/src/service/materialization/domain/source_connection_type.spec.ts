// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Why a `storage=` source on a connection the build cannot read is refused
// (`storage_source_unsupported`, warned `persist-not-built`). A `publisher`
// connection is a credential-less proxy to the host the package is published
// to, so the usual fixes (drop `storage=`, move to a warehouse connection)
// do not apply on this server: its message says the host builds it.
import { describe, expect, it } from "bun:test";
import {
   storageBuildReadsConnectionType,
   unreadableSourceConnectionMessage,
} from "./source_connection_type";

const source = { name: "orders_daily", connectionName: "wh" };

describe("unreadableSourceConnectionMessage", () => {
   it("says a publisher connection's storage= build runs on the host it is published to, with no fix that does not apply", () => {
      expect(storageBuildReadsConnectionType("publisher")).toBe(false);
      const message = unreadableSourceConnectionMessage(source, "publisher");
      expect(message).toContain("'wh' is a 'publisher' connection");
      expect(message).toContain(
         "A 'storage=' build runs on that host, not on this server.",
      );
      expect(message).toContain("answered live here");
      expect(message).not.toContain("Drop 'storage='");
      expect(message).not.toContain("warehouse connection.");
   });

   it("keeps its wording, fixes included, for a connection the build cannot read anywhere", () => {
      expect(unreadableSourceConnectionMessage(source, "duckdb")).toBe(
         "Source 'orders_daily' cannot be materialized into a storage " +
            "destination: its connection 'wh' is a 'duckdb' connection, " +
            "which cannot be passed through to a 'storage=' build (it reads " +
            "bigquery, snowflake, postgres sources, and a DuckLake source " +
            "into its own catalog only). The source is not built and is " +
            "answered live. Drop 'storage=' to build it in its own " +
            "connection, or put the source on a warehouse connection.",
      );
   });
});
