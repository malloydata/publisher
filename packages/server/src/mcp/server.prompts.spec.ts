// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { initializeMcpServer } from "./server";
import * as bundle from "../service/skills/build_skills_bundle";
import { mcpServicesFor } from "../test_helpers/mcp_services";

describe("the bundled skill prompts", () => {
   it("are parsed once, not on every initialization (one per stateless MCP POST)", () => {
      const parse = spyOn(bundle, "parseSkill");
      try {
         const services = mcpServicesFor({});
         initializeMcpServer(services);
         initializeMcpServer(services);
         expect(parse).not.toHaveBeenCalled();
      } finally {
         parse.mockRestore();
      }
   });
});
