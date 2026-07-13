import { describe, expect, it } from "bun:test";
import { redactPgSecrets } from "./pg_helpers";

describe("redactPgSecrets", () => {
   it("redacts bare password values", () => {
      expect(redactPgSecrets("host=h password=hunter2 dbname=d")).toBe(
         "host=h password=*** dbname=d",
      );
   });

   it("redacts single-quoted password values", () => {
      expect(redactPgSecrets("host=h password='s3 cret' dbname=d")).toBe(
         "host=h password=*** dbname=d",
      );
   });

   it("leaves non-secret content alone", () => {
      expect(redactPgSecrets("user=alice dbname=billing")).toBe(
         "user=alice dbname=billing",
      );
   });
});
