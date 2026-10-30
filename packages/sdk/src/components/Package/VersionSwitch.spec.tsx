// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The version switch's ⋯ menu: Pin as latest, Archive or Unarchive, and
 * Delete version, each offered only where the server would accept it, and
 * each calling the version route it names.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import type { PackageVersion } from "../../client";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

const setPackageLatestVersion = mock(
   (_env: string, _pkg: string, _body: { version: string }) =>
      Promise.resolve({ data: {} }),
);
const updatePackageVersion = mock(
   (_env: string, _pkg: string, _id: string, _body: unknown) =>
      Promise.resolve({ data: {} }),
);
const deletePackageVersion = mock((_env: string, _pkg: string, _id: string) =>
   Promise.resolve({ data: {} }),
);

mockServerProvider(
   {
      versions: {
         setPackageLatestVersion,
         updatePackageVersion,
         deletePackageVersion,
      },
   },
   { mutable: true },
);

const { VersionSwitch, versionRefusals } = await import("./VersionSwitch");

const version = (
   id: string,
   options: { latest?: boolean; archived?: boolean } = {},
): PackageVersion => ({
   resource: `publisher://environments/env/packages/pkg/versions/${id}`,
   packageName: "pkg",
   id,
   latest: options.latest ?? false,
   archiveStatus: options.archived ? "archive" : "unarchive",
   archivedAt: null,
   promotedAt: null,
   demotedAt: null,
   contentHash: "hash",
   manifestLocation: null,
   gitCommitSha: null,
   gitRef: null,
   createdAt: "2026-10-01T00:00:00Z",
   updatedAt: "2026-10-01T00:00:00Z",
});

beforeEach(() => {
   clearCache();
   setPackageLatestVersion.mockClear();
   updatePackageVersion.mockClear();
   deletePackageVersion.mockClear();
});

function renderSwitch(
   versions: PackageVersion[],
   props: {
      selectedVersionId?: string;
      onSelectVersion?: (id: string) => void;
      onPackageDeleted?: () => void;
   } = {},
) {
   render(
      <VersionSwitch
         environmentName="env"
         packageName="pkg"
         versions={versions}
         selectedVersionId={props.selectedVersionId}
         onSelectVersion={props.onSelectVersion ?? (() => {})}
         onPackageDeleted={props.onPackageDeleted}
         canManage
      />,
      { wrapper: serverWrapper },
   );
}

/** Open the ⋯ menu of one version and return its items. */
async function openActions(id: string) {
   fireEvent.click(
      screen.getByRole("button", { name: "Select package version" }),
   );
   fireEvent.click(
      await screen.findByRole("button", {
         name: `More actions for version ${id}`,
      }),
   );
   const menu = await screen.findByRole("menu", {
      name: `Actions for version ${id}`,
   });
   return within(menu);
}

const item = (menu: ReturnType<typeof within>, name: RegExp) =>
   menu.getByRole("menuitem", { name });

describe("versionRefusals", () => {
   it("refuses archiving and deleting latest while other versions remain", () => {
      const versions = [version("1.1.0", { latest: true }), version("1.0.0")];
      expect(versionRefusals(versions[0], versions)).toEqual({
         archive: "The latest version can't be archived",
         delete: "Pin another version as latest first",
      });
      expect(versionRefusals(versions[1], versions)).toEqual({});
   });

   it("allows deleting the only version, which deletes the package", () => {
      const only = [version("1.0.0", { latest: true })];
      expect(versionRefusals(only[0], only).delete).toBeUndefined();
      expect(versionRefusals(only[0], only).archive).toBe(
         "The latest version can't be archived",
      );
   });

   it("refuses taking the last version in service out while archived ones remain", () => {
      // Explicit promotion: no version is latest.
      const versions = [version("1.1.0", { archived: true }), version("1.0.0")];
      expect(versionRefusals(versions[1], versions)).toEqual({
         archive: "The last version in service can't be archived",
         delete:
            "The last version in service; delete the archived versions first",
      });
      // The archived one can always be deleted.
      expect(versionRefusals(versions[0], versions)).toEqual({});
   });
});

describe("the ⋯ menu", () => {
   it("offers Pin as latest, Archive and Delete version on a version in service", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      const menu = await openActions("1.0.0");
      expect(menu.getAllByRole("menuitem").map((i) => i.textContent)).toEqual([
         "Pin as latest",
         "Archive",
         "Delete version",
      ]);
   });

   it("disables Pin, Archive and Delete on latest, saying why", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      const menu = await openActions("1.1.0");
      expect(item(menu, /Pin as latest/).getAttribute("aria-disabled")).toBe(
         "true",
      );
      expect(item(menu, /Archive/).getAttribute("aria-disabled")).toBe("true");
      expect(item(menu, /Delete version/).getAttribute("aria-disabled")).toBe(
         "true",
      );
      expect(
         menu.getByText("Pin another version as latest first"),
      ).toBeTruthy();
   });

   it("offers Unarchive and Delete version on an archived version", async () => {
      renderSwitch([
         version("1.1.0", { latest: true }),
         version("1.0.0", { archived: true }),
      ]);
      const menu = await openActions("1.0.0");
      expect(menu.getAllByRole("menuitem").map((i) => i.textContent)).toEqual([
         "Unarchive",
         "Delete version",
      ]);
      fireEvent.click(item(menu, /Unarchive/));
      await waitFor(() =>
         expect(updatePackageVersion).toHaveBeenCalledWith(
            "env",
            "pkg",
            "1.0.0",
            { archiveStatus: "unarchive" },
         ),
      );
   });

   it("archives a version", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Archive/));
      await waitFor(() =>
         expect(updatePackageVersion).toHaveBeenCalledWith(
            "env",
            "pkg",
            "1.0.0",
            { archiveStatus: "archive" },
         ),
      );
   });

   it("pins a version as latest through PUT latest, after confirming", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Pin as latest/));
      const dialog = await screen.findByRole("dialog", {
         name: "Pin v1.0.0 as latest",
      });
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Pin as latest" }),
      );
      await waitFor(() =>
         expect(setPackageLatestVersion).toHaveBeenCalledWith("env", "pkg", {
            version: "1.0.0",
         }),
      );
   });
});

describe("Delete version", () => {
   it("deletes a version and opens latest when it was the one on screen", async () => {
      const onSelectVersion = mock((_id: string) => {});
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")], {
         selectedVersionId: "1.0.0",
         onSelectVersion,
      });
      fireEvent.click(item(await openActions("1.0.0"), /Delete version/));
      const dialog = await screen.findByRole("dialog", {
         name: "Delete v1.0.0",
      });
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete version" }),
      );
      await waitFor(() =>
         expect(deletePackageVersion).toHaveBeenCalledWith(
            "env",
            "pkg",
            "1.0.0",
         ),
      );
      await waitFor(() =>
         expect(onSelectVersion).toHaveBeenCalledWith("1.1.0"),
      );
   });

   it("says deleting the only version deletes the package, and leaves the package", async () => {
      const onPackageDeleted = mock(() => {});
      renderSwitch([version("1.0.0", { latest: true })], { onPackageDeleted });
      fireEvent.click(item(await openActions("1.0.0"), /Delete version/));
      const dialog = await screen.findByRole("dialog", {
         name: "Delete v1.0.0",
      });
      expect(
         within(dialog).getByText(/so deleting it deletes the package/),
      ).toBeTruthy();
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete package" }),
      );
      await waitFor(() => expect(onPackageDeleted).toHaveBeenCalledTimes(1));
      expect(deletePackageVersion).toHaveBeenCalledWith("env", "pkg", "1.0.0");
   });

   it("shows the server's refusal in the dialog", async () => {
      deletePackageVersion.mockImplementationOnce(() =>
         Promise.reject(
            Object.assign(new Error("Request failed with status code 409"), {
               response: {
                  data: {
                     message:
                        "A materialization of version 1.0.0 of package pkg is running",
                  },
               },
            }),
         ),
      );
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Delete version/));
      const dialog = await screen.findByRole("dialog", {
         name: "Delete v1.0.0",
      });
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete version" }),
      );
      expect(
         await within(dialog).findByText(
            /Couldn't delete v1.0.0: A materialization of version 1.0.0/,
         ),
      ).toBeTruthy();
   });
});
