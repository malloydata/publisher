// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's Skill sets and Workflows sections: what a package declares, read-only. */
import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, expect, it } from "bun:test";
import type {
   PackageWarning,
   SkillSetSummary,
   WorkflowSummary,
} from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
   noVersions,
} from "../../../test/serverProvider";

let workflows: WorkflowSummary[] | undefined;
let skillSets: SkillSetSummary[] | undefined;
let warnings: PackageWarning[] | undefined;
/** What both listings answer instead of their data, as the server answers when it could not read the package's skill sets. */
let listingFailure: Error | undefined;

const conflict = (message: string) =>
   Object.assign(new Error(message), {
      response: { status: 409, data: { message } },
   });

mockServerProvider(
   {
      versions: noVersions,
      packages: {
         getPackage: () =>
            Promise.resolve({
               data: { name: "pkg", ...(warnings ? { warnings } : {}) },
            }),
      },
      // The sections read the package's workflows and skill sets from their own routes.
      workflows: {
         listWorkflows: () =>
            listingFailure
               ? Promise.reject(listingFailure)
               : Promise.resolve({ data: workflows ?? [] }),
      },
      skillSets: {
         listSkillSets: () =>
            listingFailure
               ? Promise.reject(listingFailure)
               : Promise.resolve({ data: skillSets ?? [] }),
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
   warnings = undefined;
   listingFailure = undefined;
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
   expect(section.textContent).toContain("Revenue rules");
   // The section's own description names the default set, so the rows are told apart by their names.
   const [first, second, third] = ["default", "analyst", "zeta"].map((name) =>
      within(section).getByText(name, { exact: true }),
   );
   const follows = (a: HTMLElement, b: HTMLElement) =>
      Boolean(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);
   expect(follows(first!, second!)).toBe(true);
   expect(follows(second!, third!)).toBe(true);
   // A row is the widest ancestor of its name that holds no other row's.
   const rowOf = (label: HTMLElement, others: HTMLElement[]) => {
      let row = label;
      while (
         row.parentElement &&
         !others.some((other) => row.parentElement!.contains(other))
      ) {
         row = row.parentElement;
      }
      return row;
   };
   expect(rowOf(first!, [second!, third!]).textContent).toContain("Always on");
   expect(rowOf(second!, [first!, third!]).textContent).not.toContain(
      "Always on",
   );
   expect(rowOf(third!, [first!, second!]).textContent).not.toContain(
      "Always on",
   );
   expect(section.textContent).toContain(
      "a chat or a workflow picks the others",
   );
   expect(section.querySelectorAll('[role="button"]')).toHaveLength(0);
});

it("shows no section for a package that declares neither", async () => {
   mount();

   await screen.findByRole("region", { name: "Models" });
   expect(screen.queryByRole("region", { name: "Workflows" })).toBeNull();
   expect(screen.queryByRole("region", { name: "Skill sets" })).toBeNull();
});

it("shows neither section when the server could not read the package's skill sets, and says so in one note", async () => {
   skillSets = [{ name: "default", digest: "b" }];
   workflows = [{ name: "brief", skill_set_names: [] }];
   listingFailure = conflict(
      "skill set directory 'skill_sets/a' cannot be read",
   );
   warnings = [
      {
         code: "skill-sets-unavailable",
         severity: "warn",
         message:
            "Skill sets and workflows could not be read: skill set directory 'skill_sets/a' cannot be read. None is served until the package reloads.",
      },
   ];

   mount();

   await screen.findByRole("region", { name: "Models" });
   expect(screen.queryByRole("region", { name: "Workflows" })).toBeNull();
   expect(screen.queryByRole("region", { name: "Skill sets" })).toBeNull();
   fireEvent.click(await screen.findByRole("button", { name: "Show 1 note" }));
   expect(screen.getByText(/could not be read/)).toBeDefined();
   expect(
      screen.getByText(/None is served until the package reloads/),
   ).toBeDefined();
});

it("names a refused set and a refused workflow in the package's notes, beside the sections that are served", async () => {
   skillSets = [{ name: "default", digest: "b" }];
   workflows = [{ name: "brief", skill_set_names: [] }];
   warnings = [
      {
         code: "skill-set-not-served",
         subject: "broken",
         severity: "warn",
         message: "Skill set 'broken' is not served: its folder is missing.",
      },
      {
         code: "workflow-not-served",
         subject: "nightly",
         severity: "warn",
         message:
            "Workflow 'nightly' is not served: its instructions file is missing.",
      },
   ];

   mount();

   await screen.findByRole("region", { name: "Skill sets" });
   expect(screen.getByRole("region", { name: "Workflows" })).toBeDefined();
   fireEvent.click(await screen.findByRole("button", { name: "Show 2 notes" }));
   expect(screen.getByText(/Skill set 'broken' is not served/)).toBeDefined();
   expect(screen.getByText(/Workflow 'nightly' is not served/)).toBeDefined();
});
