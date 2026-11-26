// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's Workflows section: what a package declares, read-only. */
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it } from "bun:test";
import type { WorkflowSummary } from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   noVersions,
} from "../../../test/serverProvider";

let workflows: WorkflowSummary[] | undefined;

mockServerProvider(
   {
      versions: noVersions,
      packages: {
         getPackage: () => Promise.resolve({ data: { name: "pkg" } }),
      },
      // The section reads the package's workflows from their own route.
      workflows: {
         listWorkflows: () => Promise.resolve({ data: workflows ?? [] }),
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
});

it("lists each workflow with its description and its skill sets by name, and flags one with no instructions", async () => {
   workflows = [
      {
         name: "weekly-report",
         description: "Writes the weekly revenue report",
         has_instructions: true,
         skill_sets: [
            {
               name: "analyst",
               path: "workflows/weekly-report/skill_sets/analyst",
               digest: "a",
            },
            {
               name: "house",
               path: "workflows/default/skill_sets/house",
               digest: "b",
            },
         ],
      },
      {
         name: "default",
         has_instructions: false,
         skill_sets: [
            {
               name: "house",
               path: "workflows/default/skill_sets/house",
               digest: "b",
            },
         ],
      },
      {
         name: "skills-only",
         has_instructions: false,
         skill_sets: [
            {
               name: "house",
               path: "workflows/default/skill_sets/house",
               digest: "b",
            },
         ],
      },
   ];

   mount();

   const section = await screen.findByRole("region", { name: "Workflows" });
   expect(section.textContent).toContain("weekly-report");
   expect(section.textContent).toContain("Writes the weekly revenue report");
   expect(section.textContent).toContain("Skill sets: analyst, house");
   // The label sits on the row it describes: `default` is advertised, not
   // "without instructions", and only the other skills-only workflow says so.
   const row = (name: string) =>
      screen.getByText(name, { exact: true }).closest("li, [role], div")
         ?.parentElement?.textContent ?? "";
   expect(row("default")).toContain("Advertised skills");
   expect(row("default")).not.toContain("No instructions");
   expect(row("skills-only")).toContain("No instructions");
   expect(row("weekly-report")).not.toContain("No instructions");
   expect(section.textContent?.match(/No instructions/g)).toHaveLength(1);
   expect(section.querySelectorAll('[role="button"]')).toHaveLength(0);
});

it("shows no section for a package that declares no workflows", async () => {
   mount();

   await screen.findByRole("region", { name: "Models" });
   expect(screen.queryByRole("region", { name: "Workflows" })).toBeNull();
});
