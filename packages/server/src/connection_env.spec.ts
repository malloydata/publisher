import { describe, expect, it } from "bun:test";
import { getEnvConfig } from "./connection_env";

describe("connection_env", () => {
   it.only("should return an empty array", async () => {
      const envConfig = await getEnvConfig();
      expect(envConfig).toEqual([]);
   });
});
