// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's Skill sets and Workflows sections: what a package declares, read-only. */
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it } from "bun:test";
import type { SkillSetSummary, WorkflowSummary } from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   noVersions,
} from "../../../test/serverProvider";

let workflows: WorkflowSummary[] | undefined;
let skillSets: SkillSetSummary[] | undefined;

mockServerProvider(
   {
      versions: noVersions,
      packages: {
         getPackage: () => Promise.resolve({ data: { name: "pkg" } }),
      },
      // The sections read the package's workflows and skill sets from their own routes.
      workflows: {
         listWorkflows: () => Promise.resolve({ data: workflows ?? [] }),
      },
      skillSets: {
         listSkillSets: () => Promise.resolve({ data: skillSets ?? [] }),
      },
      reports: { listReports: () => Promise.resolve({ data: [] }) },
      models: { listModels: () => Promise.resolve({ data: [] }) },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      materializations: { listMaterializations: pending },
   },
   { mutable: true },
);

const { default: Package } = await import("./Package");

const mount = () =>
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

beforeEach(() => {
   clearCache();
   workflows = undefined;
   skillSets = undefined;
});

it("lists each workflow with its description and the skill sets it lists, by name", async () => {
   workflows = [
      {
         name: "weekly-report",
         description: "Writes the weekly revenue report",
         skill_set_names: ["analyst", "default"],
      },
      { name: "brief", skill_set_names: [] },
   ];

   mount();

   const section = await screen.findByRole("region", { name: "Workflows" });
   expect(section.textContent).toContain("weekly-report");
   expect(section.textContent).toContain("Writes the weekly revenue report");
   expect(section.textContent).toContain("Skill sets: analyst, default");
   // A workflow listing no set says nothing of sets, and none is labelled as lacking instructions.
   expect(section.textContent?.match(/Skill sets:/g)).toHaveLength(1);
   expect(section.textContent).not.toContain("No instructions");
   expect(section.querySelectorAll('[role="button"]')).toHaveLength(0);
});

it("lists every skill set, default first and marked always on", async () => {
   skillSets = [
      { name: "analyst", description: "Revenue rules", digest: "a" },
      { name: "default", digest: "b" },
      { name: "zeta", digest: "c" },
   ];

   mount();

   const section = await screen.findByRole("region", { name: "Skill sets" });
   const text = section.textContent ?? "";
   expect(text).toContain("Revenue rules");
   expect(text.indexOf("default")).toBeLessThan(text.indexOf("analyst"));
   expect(text.indexOf("analyst")).toBeLessThan(text.indexOf("zeta"));
   expect(text.match(/Always on/g)).toHaveLength(1);
   expect(section.querySelectorAll('[role="button"]')).toHaveLength(0);
});

it("shows no section for a package that declares neither", async () => {
   mount();

   await screen.findByRole("region", { name: "Models" });
   expect(screen.queryByRole("region", { name: "Workflows" })).toBeNull();
   expect(screen.queryByRole("region", { name: "Skill sets" })).toBeNull();
});
