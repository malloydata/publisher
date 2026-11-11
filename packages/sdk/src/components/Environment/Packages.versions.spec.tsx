// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package is never edited in place from the list: a published version is
 * immutable, and a package mounted in place is edited at its source. The list
 * offers Delete, whether or not the package has versions.
 */
import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

const listPackages = mock((_environmentName: string) =>
   Promise.resolve({
      data: [
         { name: "plain" },
         { name: "versioned", versionId: "1.1.0", latestVersion: "1.1.0" },
         // Published under explicit promotion, before any version is latest.
         { name: "unpromoted", versionId: null, latestVersion: null },
      ],
   }),
);

mockServerProvider({ packages: { listPackages } }, { mutable: true });

const { default: Packages } = await import("./Packages");

beforeEach(() => {
   clearCache();
   listPackages.mockClear();
});

async function menuItemsFor(packageName: string): Promise<string[]> {
   render(
      <Packages
         onSelectPackage={() => {}}
         resourceUri="publisher://environments/env"
      />,
      { wrapper: serverWrapper },
   );
   fireEvent.click(
      await screen.findByRole("button", {
         name: `Package actions for ${packageName}`,
      }),
   );
   const items = await screen.findAllByRole("menuitem");
   return items.map((item) => item.textContent ?? "");
}

it.each([["plain"], ["versioned"], ["unpromoted"]])(
   "offers only Delete on a package (%s)",
   async (packageName) => {
      expect(await menuItemsFor(packageName)).toEqual(["Delete"]);
   },
);
