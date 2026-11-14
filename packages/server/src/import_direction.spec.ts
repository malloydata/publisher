// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

/**
 * Pins the layer import-direction lint rule (`publisher/import-direction`,
 * defined in eslint.config.mjs) two ways:
 *
 *  1. Fixtures: deliberately violating (and deliberately allowed) imports,
 *     linted as if they lived at a path under src/, prove each of R1-R10
 *     and H5 fires, and that type-only, non-relative and look-alike imports
 *     do not where the rule allows them.
 *  2. The allowlist is exact: linting the real tree with an EMPTY allowlist
 *     reports precisely the pairs IMPORT_DIRECTION_ALLOWLIST names. A new
 *     violation fails here (fix the import), and so does a stale entry whose
 *     import is gone (delete the entry; the list only shrinks).
 *
 * The config is read through ESLint itself, so this exercises the rule exactly
 * as `bun run lint` runs it.
 */

// No @types/eslint in the tree: type only the surface used here.
type LintMessage = {
   ruleId: string | null;
   message: string;
   fatal?: boolean;
};
type RuleEntry = [number | string, { allow: Record<string, string> }];
type FlatConfig = Record<string, unknown>;
type ResolvedConfig = {
   plugins?: Record<string, unknown>;
   languageOptions?: { parser?: unknown };
   rules?: Record<string, unknown>;
};
type ESLintInstance = {
   calculateConfigForFile(file: string): Promise<ResolvedConfig | undefined>;
   lintText(
      code: string,
      options: { filePath: string },
   ): Promise<{ messages: LintMessage[] }[]>;
};
type LinterInstance = {
   verify(code: string, config: FlatConfig[], filename: string): LintMessage[];
};
// eslint is CommonJS; require keeps tsc from asking for its declarations.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const eslintApi = require("eslint") as {
   loadESLint(opts: {
      useFlatConfig: boolean;
      cwd: string;
   }): Promise<new (opts: { cwd: string }) => ESLintInstance>;
   Linter: new (opts: { configType: "flat"; cwd: string }) => LinterInstance;
};

const RULE = "publisher/import-direction";
const SERVER_DIR = path.resolve(__dirname, "..");
const SRC_DIR = path.join(SERVER_DIR, "src");

let eslint: ESLintInstance;
let linter: LinterInstance;
let plugin: unknown;
let parser: unknown;
let allowlist: Record<string, string>;

function lint(
   srcRelative: string,
   code: string,
   allow: Record<string, string> = {},
): string[] {
   return linter
      .verify(
         code,
         [
            {
               files: ["**/*.ts"],
               plugins: { publisher: plugin },
               languageOptions: { parser },
               rules: { [RULE]: ["error", { allow }] },
            },
         ],
         path.join(SRC_DIR, srcRelative),
      )
      .filter((m) => {
         // A parse error must not pass as "no violation". Other messages are
         // the real files' eslint-disable comments naming rules this one-rule
         // config does not load.
         expect(m.fatal ?? false).toBe(false);
         return m.ruleId === RULE;
      })
      .map((m) => m.message);
}

function ruleIds(messages: string[]): string[] {
   return messages.map(
      (m) => /Import direction ((?:R|H)\d+)/.exec(m)?.[1] ?? m,
   );
}

beforeAll(async () => {
   const ESLint = await eslintApi.loadESLint({
      useFlatConfig: true,
      cwd: SERVER_DIR,
   });
   eslint = new ESLint({ cwd: SERVER_DIR });
   linter = new eslintApi.Linter({ configType: "flat", cwd: SERVER_DIR });
   const config = await eslint.calculateConfigForFile(
      path.join(SRC_DIR, "service", "package.ts"),
   );
   plugin = config?.plugins?.publisher;
   parser = config?.languageOptions?.parser;
   const entry = config?.rules?.[RULE] as RuleEntry | undefined;
   expect(plugin).toBeDefined();
   expect(entry).toBeDefined();
   allowlist = entry![1].allow;
});

describe("import-direction rule: fixtures", () => {
   it("R1: the domain may not import an adapter, by value or by type", () => {
      expect(
         ruleIds(lint("service/x.ts", `import { a } from "../mcp/tools/y";`)),
      ).toEqual(["R1"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `import type { C } from "../controller/c.controller";`,
            ),
         ),
      ).toEqual(["R1"]);
      expect(
         ruleIds(
            lint("service/entities/x.ts", `import { r } from "../../routes";`),
         ),
      ).toEqual(["R1"]);
      expect(
         ruleIds(lint("retrieval/search/x.ts", `import "../../mcp/server";`)),
      ).toEqual(["R1"]);
      expect(
         ruleIds(lint("service/x.ts", `export * from "../mcp/handler_utils";`)),
      ).toEqual(["R1"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `export { a } from "../mcp/handler_utils.js";`,
            ),
         ),
      ).toEqual(["R1"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `export async function f() { return import("../mcp/server"); }`,
            ),
         ),
      ).toEqual(["R1"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `export type T = import("../mcp/server").McpServer;`,
            ),
         ),
      ).toEqual(["R1"]);
   });

   it("R2: an MCP tool may import a controller's type, never its value", () => {
      expect(
         ruleIds(
            lint(
               "mcp/tools/x_tool.ts",
               `import { CompileController } from "../../controller/compile.controller";`,
            ),
         ),
      ).toEqual(["R2"]);
      expect(
         ruleIds(
            lint(
               "mcp/tools/x_tool.ts",
               `import { type A, B } from "../../controller/compile.controller";`,
            ),
         ),
      ).toEqual(["R2"]);
      expect(
         lint(
            "mcp/tools/x_tool.ts",
            `import type { CompileController } from "../../controller/compile.controller";`,
         ),
      ).toEqual([]);
      expect(
         lint(
            "mcp/tools/x_tool.ts",
            `import { type CompileController } from "../../controller/compile.controller";`,
         ),
      ).toEqual([]);
   });

   it("R3: governance may not import service/model.ts", () => {
      expect(
         ruleIds(
            lint(
               "service/governance/x.ts",
               `import { Model } from "../model";`,
            ),
         ),
      ).toEqual(["R3"]);
      expect(
         ruleIds(
            lint(
               "service/governance/ir/x.ts",
               `import type { Model } from "../../model";`,
            ),
         ),
      ).toEqual(["R3"]);
      expect(
         lint(
            "service/governance/x.ts",
            `import { y } from "../model_policy";`,
         ),
      ).toEqual([]);
   });

   it("R4: the domain may not import the retrieval module, by value or by type", () => {
      expect(
         ruleIds(
            lint(
               "service/package.ts",
               `import { a } from "../retrieval/llm/chat_model_slot";`,
            ),
         ),
      ).toEqual(["R4"]);
      expect(
         ruleIds(
            lint(
               "service/entities/x.ts",
               `import type { T } from "../../retrieval/model/types";`,
            ),
         ),
      ).toEqual(["R4"]);
      expect(
         ruleIds(lint("service/x.ts", `export * from "../retrieval";`)),
      ).toEqual(["R4"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `export type T = import("../retrieval/config").RetrievalConfig;`,
            ),
         ),
      ).toEqual(["R4"]);
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `export async function f() { return import("../retrieval/registry"); }`,
            ),
         ),
      ).toEqual(["R4"]);
      // The other direction is the intended one.
      expect(
         lint(
            "retrieval/index/x.ts",
            `import { s } from "../../service/package_retrieval_settings";`,
         ),
      ).toEqual([]);
      // Look-alikes and layers outside service/ are not R4's business.
      expect(
         lint(
            "service/x.ts",
            `import { r } from "./retrieval_local";\nimport { c } from "../retrieval_config";`,
         ),
      ).toEqual([]);
      expect(
         lint("package_load/x.ts", `import { r } from "../retrieval/config";`),
      ).toEqual([]);
   });

   it("R5: materialization/domain/ imports only itself and api.ts", () => {
      const at = "service/materialization/domain/x.ts";
      expect(
         ruleIds(lint(at, `import { a } from "../plan/build_plan";`)),
      ).toEqual(["R5"]);
      expect(
         ruleIds(lint(at, `import type { M } from "../../model";`)),
      ).toEqual(["R5"]);
      expect(
         lint(
            at,
            `import { b } from "./freshness";\nimport type { components } from "../../../api";\nimport { Tag } from "@malloydata/malloy";`,
         ),
      ).toEqual([]);
   });

   it("R6: materialization/text/ imports only domain/, governance/ir and the text leaves", () => {
      const at = "service/materialization/text/x.ts";
      expect(
         ruleIds(lint(at, `import { s } from "../plan/source_map";`)),
      ).toEqual(["R6"]);
      expect(ruleIds(lint(at, `import { l } from "../../../logger";`))).toEqual(
         ["R6", "H5"],
      );
      expect(
         lint(
            at,
            [
               `import { a } from "./malloy_lexer";`,
               `import { b } from "../domain/types";`,
               `import { c } from "../../governance/ir/source_graph";`,
               `import { d } from "../../malloy_text";`,
               `import { e } from "../../overlay_key";`,
               `import { f } from "../../annotation_reader";`,
            ].join("\n"),
         ),
      ).toEqual([]);
   });

   it("R7 and R8: plan/, run/ and serve/ never import one another, by value or by type", () => {
      expect(
         ruleIds(
            lint(
               "service/materialization/plan/x.ts",
               `import type { T } from "../run/source_build";\nimport { m } from "../../model";`,
            ),
         ),
      ).toEqual(["R7", "R7"]);
      expect(
         ruleIds(
            lint(
               "service/materialization/run/x.ts",
               `import type { P } from "../plan/build_plan";\nimport { s } from "../serve/query_path";`,
            ),
         ),
      ).toEqual(["R8", "R8"]);
      expect(
         ruleIds(
            lint(
               "service/materialization/serve/x.ts",
               `import { r } from "../run/reclaimer";`,
            ),
         ),
      ).toEqual(["R8"]);
      // What they share may not reach a sibling either.
      expect(
         ruleIds(
            lint(
               "service/materialization/host.ts",
               `import type { P } from "./plan/build_plan";`,
            ),
         ),
      ).toEqual(["R8"]);
      // Within a layer, and down to the leaves, is the intended direction.
      expect(
         lint(
            "service/materialization/serve/x.ts",
            `import { a } from "./query_path";\nimport { b } from "../domain/types";\nimport { c } from "../text/source_map";`,
         ),
      ).toEqual([]);
   });

   it("R9: governance imports nothing of materialization but domain/", () => {
      expect(
         ruleIds(
            lint(
               "service/governance/x.ts",
               `import { s } from "../materialization/serve/query_path";`,
            ),
         ),
      ).toEqual(["R9", "R10"]);
      expect(
         lint(
            "service/governance/x.ts",
            `import type { T } from "../materialization/domain/types";`,
         ),
      ).toEqual([]);
   });

   it("R10: outside the module, only materialization/index.ts and domain/", () => {
      expect(
         ruleIds(
            lint(
               "service/x.ts",
               `import { P } from "./materialization/package_materialization";`,
            ),
         ),
      ).toEqual(["R10"]);
      expect(
         lint(
            "service/x.ts",
            `import { P } from "./materialization";\nimport { Q } from "./materialization/index";\nimport type { T } from "./materialization/domain/types";`,
         ),
      ).toEqual([]);
   });

   it("H5: logger and metrics only in run/, the facade and the query pipeline", () => {
      expect(
         ruleIds(
            lint(
               "service/materialization/serve/x.ts",
               `import { logger } from "../../../logger";\nimport { r } from "../../../materialization_metrics";`,
            ),
         ),
      ).toEqual(["H5", "H5"]);
      expect(
         lint(
            "service/materialization/run/x.ts",
            `import { logger } from "../../../logger";`,
         ),
      ).toEqual([]);
      expect(
         lint(
            "service/materialization/package_materialization.ts",
            `import { logger } from "../../logger";`,
         ),
      ).toEqual([]);
      expect(
         lint(
            "service/query_serving.ts",
            `import { logger } from "../logger";`,
         ),
      ).toEqual([]);
   });

   it("leaves look-alikes, packages and other layers alone", () => {
      expect(
         lint(
            "service/x.ts",
            [
               `import { a } from "../mcp_config";`,
               `import { b } from "./mcp/local";`,
               `import { c } from "@modelcontextprotocol/sdk/server/mcp.js";`,
               `import { d } from "../storage/DatabaseInterface";`,
            ].join("\n"),
         ),
      ).toEqual([]);
      // Adapters and the composition root may import anything.
      expect(
         lint(
            "routes/x.ts",
            `import { c } from "../controller/compile.controller";`,
         ),
      ).toEqual([]);
      expect(
         lint("main_server.ts", `import { m } from "./mcp/server";`),
      ).toEqual([]);
   });

   it("an allowlist entry excepts exactly its file pair", () => {
      const allow = {
         "service/package.ts -> mcp/skills/package_skills": "U2",
      };
      const code = [
         `import { a } from "../mcp/skills/package_skills";`,
         `import { b } from "../mcp/skills/build_skills_bundle";`,
      ].join("\n");
      expect(lint("service/package.ts", code, allow)).toEqual([
         expect.stringContaining(
            "(service/package.ts -> mcp/skills/build_skills_bundle)",
         ),
      ]);
      expect(lint("service/other.ts", code, allow)).toHaveLength(2);
   });

   it("is wired into the real config, which exempts specs", async () => {
      const code = `import { initializeMcpServer } from "../mcp/server";\nexport const x = initializeMcpServer;\n`;
      const [prod] = await eslint.lintText(code, {
         filePath: path.join(SRC_DIR, "service", "__fixture__.ts"),
      });
      expect(prod.messages.map((m) => m.ruleId)).toContain(RULE);
      const [spec] = await eslint.lintText(code, {
         filePath: path.join(SRC_DIR, "service", "__fixture__.spec.ts"),
      });
      expect(spec.messages.map((m) => m.ruleId)).not.toContain(RULE);
   });
});

function listTs(dir: string): string[] {
   return fs
      .readdirSync(dir, { recursive: true, encoding: "utf8" })
      .filter((f) => f.endsWith(".ts"))
      .map((f) => path.join(dir, f));
}

describe("import-direction allowlist", () => {
   it("names exactly today's violations, each tagged with its slice", async () => {
      const found = new Set<string>();
      for (const file of listTs(SRC_DIR)) {
         const config = await eslint.calculateConfigForFile(file);
         if (!config?.rules?.[RULE]) continue; // exempt by the config
         for (const message of lint(
            path.relative(SRC_DIR, file),
            fs.readFileSync(file, "utf8"),
         )) {
            found.add(/\(([^()]+ -> [^()]+)\)$/.exec(message)![1]);
         }
      }
      const listed = Object.keys(allowlist).sort();
      const actual = [...found].sort();
      // Unlisted: a new violation. Fix the import; do not extend the list.
      expect(actual.filter((k) => !listed.includes(k))).toEqual([]);
      // Stale: the import is gone. Delete the entry.
      expect(listed.filter((k) => !actual.includes(k))).toEqual([]);
      for (const slice of Object.values(allowlist)) {
         // An architecture-plan slice, or a materialization phase 5 step.
         expect(slice).toMatch(
            /^(U[1-4]|D[1-6]|V[1-5]|C[1-4]|L|R|2[a-l]|2g\.[1-4]|5)$/,
         );
      }
   });
});
