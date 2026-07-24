import { describe, expect, test } from "bun:test";
import {
   addPackage,
   assertCanAddPackage,
   defaultConfig,
   parseJson,
   resolveEnvironmentName,
   targetEnvironment,
   type PublisherConfig,
} from "./config";
import { ScaffoldError } from "./errors";

describe("parseJson", () => {
   test("reads a file carrying a UTF-8 BOM, as the rest of the toolchain does", () => {
      expect(parseJson('\uFEFF{"name":"app"}')).toEqual({ name: "app" });
   });

   test("still throws on JSON that is actually broken", () => {
      expect(() => parseJson('{"name":"app",}')).toThrow();
   });
});

describe("defaultConfig", () => {
   test("wraps one package in a single environment", () => {
      const config = defaultConfig("default", {
         name: "sales",
         location: "./sales",
      });
      expect(config).toEqual({
         frozenConfig: false,
         environments: [
            {
               name: "default",
               packages: [{ name: "sales", location: "./sales" }],
               connections: [],
            },
         ],
      });
   });

   test("allows an environment with no package (setup-only)", () => {
      const config = defaultConfig("default");
      expect(config.environments[0].packages).toEqual([]);
   });
});

describe("addPackage", () => {
   test("adds to the matching environment and reports a change", () => {
      const config = defaultConfig("default", { name: "a", location: "./a" });
      const result = addPackage(config, "default", {
         name: "b",
         location: "./b",
      });
      expect(result).toEqual({ added: true, envName: "default" });
      expect(config.environments[0].packages.map((p) => p.name)).toEqual([
         "a",
         "b",
      ]);
   });

   test("re-registering the identical entry is a no-op", () => {
      const config = defaultConfig("default", { name: "a", location: "./a" });
      const result = addPackage(config, "default", {
         name: "a",
         location: "./a",
      });
      expect(result.added).toBe(false);
      expect(config.environments[0].packages).toHaveLength(1);
   });

   test("same name at a different location is an error", () => {
      const config = defaultConfig("default", { name: "a", location: "./a" });
      expect(() =>
         addPackage(config, "default", { name: "a", location: "./elsewhere" }),
      ).toThrow(ScaffoldError);
   });

   test("refuses to touch a frozen config", () => {
      const config = defaultConfig("default", { name: "a", location: "./a" });
      config.frozenConfig = true;
      expect(() =>
         addPackage(config, "default", { name: "b", location: "./b" }),
      ).toThrow(/frozenConfig/);
   });

   test("falls back to the first environment and reports its name", () => {
      const config: PublisherConfig = {
         frozenConfig: false,
         environments: [{ name: "examples", packages: [] }],
      };
      const result = addPackage(config, "default", {
         name: "a",
         location: "./a",
      });
      expect(result).toEqual({ added: true, envName: "examples" });
      expect(config.environments).toHaveLength(1);
      expect(config.environments[0].name).toBe("examples");
      expect(config.environments[0].packages).toHaveLength(1);
   });

   test("creates the environment when there are none", () => {
      const config: PublisherConfig = { frozenConfig: false, environments: [] };
      const result = addPackage(config, "default", {
         name: "a",
         location: "./a",
      });
      expect(result).toEqual({ added: true, envName: "default" });
      expect(config.environments).toEqual([
         {
            name: "default",
            packages: [{ name: "a", location: "./a" }],
            connections: [],
         },
      ]);
   });
});

describe("resolveEnvironmentName", () => {
   test("prefers the named environment, then the first, then the name", () => {
      const named: PublisherConfig = {
         environments: [
            { name: "prod", packages: [] },
            { name: "default", packages: [] },
         ],
      };
      expect(resolveEnvironmentName(named, "default")).toBe("default");

      const other: PublisherConfig = {
         environments: [{ name: "examples", packages: [] }],
      };
      expect(resolveEnvironmentName(other, "default")).toBe("examples");

      expect(resolveEnvironmentName({ environments: [] }, "default")).toBe(
         "default",
      );
   });
});

describe("assertCanAddPackage", () => {
   test("throws on a frozen config or a name at a different location", () => {
      const frozen = defaultConfig("default", { name: "a", location: "./a" });
      frozen.frozenConfig = true;
      expect(() =>
         assertCanAddPackage(frozen, "default", { name: "b", location: "./b" }),
      ).toThrow(/frozenConfig/);

      const taken = defaultConfig("default", { name: "a", location: "./a" });
      expect(() =>
         assertCanAddPackage(taken, "default", { name: "a", location: "./z" }),
      ).toThrow(ScaffoldError);
   });

   test("does not mutate the config", () => {
      const config = defaultConfig("default", { name: "a", location: "./a" });
      assertCanAddPackage(config, "default", { name: "b", location: "./b" });
      expect(config.environments[0].packages).toHaveLength(1);
   });
});

/**
 * A config with the right outer shape and a null inside it is still the user's
 * file, and every other bad shape in it is answered with a sentence naming the
 * problem. These two reached a property read on null, which the CLI reports as a
 * bug in this tool: the wrong diagnosis, and it sends the reader to this
 * repository over a file only they can fix.
 */
describe("entries in the config that are not objects", () => {
   const pkg = { name: "sales", location: "./sales" };

   /** Whatever went wrong, said in this tool's own voice. */
   function refusalFrom(run: () => void): void {
      try {
         run();
      } catch (err) {
         if (err instanceof ScaffoldError) {
            return;
         }
         throw new Error(
            `A config this tool cannot use came back as ` +
               `${(err as Error).name}: ${(err as Error).message}. Anything ` +
               `that is not a ScaffoldError is printed as a bug in ` +
               `create-malloy-package, over a file only the user can fix.`,
         );
      }
   }

   test("an environment that is null", () => {
      const config = { environments: [null] } as unknown as PublisherConfig;
      refusalFrom(() => targetEnvironment(config, "default"));
      refusalFrom(() => resolveEnvironmentName(config, "default"));
      refusalFrom(() => assertCanAddPackage(config, "default", pkg));
      refusalFrom(() => addPackage(config, "default", pkg));
   });

   test("a package entry that is null", () => {
      const config = {
         environments: [{ name: "default", packages: [null] }],
      } as unknown as PublisherConfig;
      refusalFrom(() => assertCanAddPackage(config, "default", pkg));
      refusalFrom(() => addPackage(config, "default", pkg));
   });
});
