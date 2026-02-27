import { describe, expect, it } from "bun:test";
import { getDatabaseToken, requestContext } from "./request_context";

describe("request_context", () => {
   describe("getDatabaseToken", () => {
      it("should return undefined when no context is active", () => {
         expect(getDatabaseToken()).toBeUndefined();
      });

      it("should return the token from the active context", async () => {
         let captured: string | undefined;
         await requestContext.run({ databaseToken: "test-token" }, () => {
            captured = getDatabaseToken();
         });
         expect(captured).toBe("test-token");
      });

      it("should return undefined when context has no token", async () => {
         let captured: string | undefined = "should-be-overwritten";
         await requestContext.run({}, () => {
            captured = getDatabaseToken();
         });
         expect(captured).toBeUndefined();
      });

      it("should isolate tokens across concurrent contexts", async () => {
         const results: (string | undefined)[] = [];
         await Promise.all([
            requestContext.run({ databaseToken: "token-a" }, async () => {
               // Simulate async work
               await new Promise((r) => setTimeout(r, 10));
               results.push(getDatabaseToken());
            }),
            requestContext.run({ databaseToken: "token-b" }, async () => {
               results.push(getDatabaseToken());
            }),
         ]);
         expect(results).toContain("token-a");
         expect(results).toContain("token-b");
         expect(results).toHaveLength(2);
      });
   });
});
