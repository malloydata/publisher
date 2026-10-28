// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { assertUnderDataPath } from "./storage_table_load";

const inside = (uri: string, root = "/data/lake/db_a") =>
   expect(() => assertUnderDataPath(uri, root)).not.toThrow();
const outside = (uri: string, root = "/data/lake/db_a") =>
   expect(() => assertUnderDataPath(uri, root)).toThrow(
      /must sit under the catalog's data path/,
   );

describe("a load's object must sit under the catalog's data path", () => {
   it("admits an object under the path, local or in a bucket", () => {
      inside("/data/lake/db_a/uploads/orders.csv");
      inside("/data/lake/db_a/orders.csv", "/data/lake/db_a/");
      inside("s3://b/lake/db_a/u/x.parquet", "s3://b/lake/db_a");
   });

   it("refuses a sibling prefix that only shares the string", () => {
      outside("/data/lake/db_ab/x.csv");
      outside("s3://b/lake/db_ab/x.csv", "s3://b/lake/db_a");
   });

   it("refuses a path that climbs out through dot segments", () => {
      outside("/data/lake/db_a/../../../etc/passwd.csv");
      outside("/data/lake/db_a/u/../../db_b/x.csv");
      outside("s3://b/lake/db_a/../db_b/x.csv", "s3://b/lake/db_a");
      outside("/data/lake/db_a/./x.csv");
      outside("/data/lake/db_a//x.csv");
   });

   it("refuses globs and backslashes, which readers would expand or reinterpret", () => {
      outside("/data/lake/db_a/*.csv");
      outside("/data/lake/db_a/x?.csv");
      outside("/data/lake/db_a/{a,b}.csv");
      outside("/data/lake/db_a/[ab].csv");
      outside("/data/lake/db_a/..\\\\x.csv");
   });

   it("refuses the path itself and anything when no path is known", () => {
      outside("/data/lake/db_a/");
      outside("/x.csv", "");
   });
});
