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

   it("redacts the password in a URL-form connection string", () => {
      expect(redactPgSecrets("postgres://user:pass@host/db")).toBe(
         "postgres://user:***@host/db",
      );
   });

   it("preserves port and query string around a URI password", () => {
      expect(redactPgSecrets("postgresql://u:p@h:5432/d?sslmode=require")).toBe(
         "postgresql://u:***@h:5432/d?sslmode=require",
      );
   });

   it("redacts a percent-encoded password whole", () => {
      expect(redactPgSecrets("postgres://u:p%40ss@h/d")).toBe(
         "postgres://u:***@h/d",
      );
   });

   it("redacts a literal @ in the password up to the last @", () => {
      expect(redactPgSecrets("postgres://u:p@ss@h/d")).toBe(
         "postgres://u:***@h/d",
      );
   });

   it("redacts a password containing colons whole", () => {
      expect(redactPgSecrets("postgres://u:pa:ss@h/d")).toBe(
         "postgres://u:***@h/d",
      );
   });

   it("redacts a raw slash in a pg password via the mop-up pass", () => {
      expect(redactPgSecrets("postgres://u:pa/ss@h/d")).toBe(
         "postgres://u:***@h/d",
      );
   });

   it("redacts the password when the username is empty", () => {
      expect(redactPgSecrets("postgres://:secret@h/d")).toBe(
         "postgres://:***@h/d",
      );
   });

   it("leaves IPv6 hosts intact", () => {
      expect(redactPgSecrets("postgres://u:p@[::1]:5432/d")).toBe(
         "postgres://u:***@[::1]:5432/d",
      );
   });

   it("does not redact a username-only userinfo", () => {
      expect(redactPgSecrets("postgres://user@host/db")).toBe(
         "postgres://user@host/db",
      );
   });

   it("leaves URIs without userinfo alone", () => {
      expect(redactPgSecrets("postgres://host/db")).toBe("postgres://host/db");
      expect(redactPgSecrets("postgres://host:5432/db")).toBe(
         "postgres://host:5432/db",
      );
   });

   it("does not mangle a passwordless URI with :...@ in its query", () => {
      expect(redactPgSecrets("postgres://h?options=endpoint:foo@bar")).toBe(
         "postgres://h?options=endpoint:foo@bar",
      );
      expect(redactPgSecrets("postgres://h/d?x=a@b")).toBe(
         "postgres://h/d?x=a@b",
      );
   });

   it("leaves email addresses in prose alone", () => {
      expect(redactPgSecrets("contact admin@example.com for help")).toBe(
         "contact admin@example.com for help",
      );
   });

   it("does not mangle https URLs with @ in the path", () => {
      expect(
         redactPgSecrets("fetched https://github.com:443/@scope/pkg ok"),
      ).toBe("fetched https://github.com:443/@scope/pkg ok");
   });

   it("redacts every URI in a message independently", () => {
      expect(
         redactPgSecrets(
            "tried postgres://u1:p1@h1/d1 then postgres://u2:p2@h2/d2",
         ),
      ).toBe("tried postgres://u1:***@h1/d1 then postgres://u2:***@h2/d2");
   });

   it("keeps comma-joined URIs in one token separate", () => {
      expect(
         redactPgSecrets(
            "candidates: postgres://u1:p1@h1/d1,postgres://u2:p2@h2/d2",
         ),
      ).toBe("candidates: postgres://u1:***@h1/d1,postgres://u2:***@h2/d2");
   });

   it("leaves trailing sentence punctuation alone", () => {
      expect(redactPgSecrets("could not connect to postgres://u:p@h/d.")).toBe(
         "could not connect to postgres://u:***@h/d.",
      );
   });

   it("handles keyword and URI forms in one message", () => {
      expect(
         redactPgSecrets(
            "attach failed: host=h password=x and fallback postgres://u:p@h2/d",
         ),
      ).toBe(
         "attach failed: host=h password=*** and fallback postgres://u:***@h2/d",
      );
   });

   it("keeps the URI intact when a password contains 'password='", () => {
      // URI passes run before the keyword pass; keyword-first would eat
      // the @host/db tail via \S+.
      expect(redactPgSecrets("postgres://u:password=abc@h/d")).toBe(
         "postgres://u:***@h/d",
      );
   });

   it("over-redacts rather than leaks a ?password= query parameter", () => {
      // Pre-existing keyword-pass behavior: \S+ eats through &sslmode=...
      // Over-redaction, never a leak.
      expect(
         redactPgSecrets("postgres://h/d?password=abc&sslmode=require"),
      ).toBe("postgres://h/d?password=***");
   });

   it("matches uppercase schemes", () => {
      expect(redactPgSecrets("POSTGRES://U:Secret@H/D")).toBe(
         "POSTGRES://U:***@H/D",
      );
   });

   it("redacts non-pg scheme userinfo passwords too", () => {
      expect(redactPgSecrets("amqp://guest:guest@rabbit:5672/vhost")).toBe(
         "amqp://guest:***@rabbit:5672/vhost",
      );
   });

   it("redacts an Azure user@servername username form", () => {
      // Azure single-server usernames carry a literal @ (user@servername);
      // userinfo splits at the LAST @, so the password after the first : is
      // redacted while the @-bearing username stays intact.
      expect(
         redactPgSecrets(
            "postgres://myadmin@myserver:Zx7SeCReT9q@myserver.postgres.database.azure.com:5432/db",
         ),
      ).toBe(
         "postgres://myadmin@myserver:***@myserver.postgres.database.azure.com:5432/db",
      );
   });

   it("redacts an @-in-username form whose password also contains @", () => {
      expect(redactPgSecrets("postgres://myadmin@srv:p@ss@host/db")).toBe(
         "postgres://myadmin@srv:***@host/db",
      );
   });

   it("redacts an @-in-username form with a raw / in the password", () => {
      expect(redactPgSecrets("postgresql://u@srv:sec/ret@h/d")).toBe(
         "postgresql://u@srv:***@h/d",
      );
   });

   it("redacts a scheme abutting a word char", () => {
      // No leading anchor on the scheme, so a scheme glued to a preceding
      // word char (an invalid-scheme prefix, or a stray digit) still redacts.
      expect(redactPgSecrets("x_postgres://u:secretpw@h/d")).toBe(
         "x_postgres://u:***@h/d",
      );
      expect(redactPgSecrets("3postgres://u:secretpw@h/d")).toBe(
         "3postgres://u:***@h/d",
      );
   });

   it("never reveals a secret when re-applied (monotonic)", () => {
      // redactPgSecrets is not strictly idempotent in general (re-applying
      // can redact further, e.g. an unquoted *** left by the keyword pass),
      // but re-application must never turn a redacted secret back into
      // cleartext.
      const input = "postgres://u:p@ss@h/d and host=h password=hunter2";
      const once = redactPgSecrets(input);
      const twice = redactPgSecrets(once);
      expect(once).not.toContain("hunter2");
      expect(twice).not.toContain("hunter2");
      expect(twice).not.toContain("p@ss");
   });

   it("redacts the DSN inside a real DuckDB attach error message", () => {
      // Shape observed from DuckDB's postgres extension: the full DSN is
      // echoed verbatim inside the IO Error text.
      const msg =
         'IO Error: Unable to connect to Postgres at "postgres://alice:supersecretpw@127.0.0.1:5432/mydb": connection to server at "127.0.0.1", port 5432 failed: Connection refused';
      const redacted = redactPgSecrets(msg);
      expect(redacted).toContain("postgres://alice:***@127.0.0.1:5432/mydb");
      expect(redacted).not.toContain("supersecretpw");
   });
});
