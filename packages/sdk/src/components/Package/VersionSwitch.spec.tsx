// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The version switch's ⋯ menu: Pin as latest, Archive or Unarchive, and
 * Delete version, each offered only where the server would accept it, and
 * each calling the version route it names. Archive and Delete confirm first,
 * and a delete is confirmed against the versions read afresh, never against
 * a list another tab may have changed.
 */
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
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
import { globalQueryClient } from "../../utils/queryClient";

/** What the server holds now; the mocks below read and change it. */
let serverVersions: PackageVersion[] = [];
let listFails = false;

const listPackageVersions = mock(async (_env: string, _pkg: string) => {
   if (listFails) {
      throw Object.assign(new Error("Request failed with status code 500"), {
         response: { status: 500, data: { message: "boom" } },
      });
   }
   return { data: serverVersions };
});
const setPackageLatestVersion = mock(
   (_env: string, _pkg: string, _body: { version: string }) =>
      Promise.resolve({ data: {} }),
);
const updatePackageVersion = mock(
   (
      _env: string,
      _pkg: string,
      id: string,
      body: { archiveStatus: "archive" | "unarchive" },
   ) => {
      serverVersions = serverVersions.map((v) =>
         v.id === id ? { ...v, archiveStatus: body.archiveStatus } : v,
      );
      return Promise.resolve({ data: {} });
   },
);
const deletePackageVersion = mock((_env: string, _pkg: string, id: string) => {
   serverVersions = serverVersions.filter((v) => v.id !== id);
   return Promise.resolve({ data: {} });
});

mockServerProvider(
   {
      versions: {
         listPackageVersions,
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

let invalidate: ReturnType<typeof spyOn>;

beforeEach(() => {
   clearCache();
   serverVersions = [];
   listFails = false;
   listPackageVersions.mockClear();
   setPackageLatestVersion.mockClear();
   updatePackageVersion.mockClear();
   deletePackageVersion.mockClear();
   invalidate = spyOn(globalQueryClient, "invalidateQueries");
});

afterEach(() => {
   invalidate.mockRestore();
});

/**
 * Render the switch over `versions`, the list the page holds. The server
 * holds the same unless a test says otherwise by setting `serverVersions`.
 */
function renderSwitch(
   versions: PackageVersion[],
   props: {
      selectedVersionId?: string;
      onSelectVersion?: (id: string) => void;
      onReplaceVersion?: (id: string | undefined) => void;
      onPackageDeleted?: () => void;
      canManage?: boolean;
      server?: PackageVersion[];
   } = {},
) {
   serverVersions = props.server ?? versions;
   render(
      <VersionSwitch
         environmentName="env"
         packageName="pkg"
         versions={versions}
         selectedVersionId={props.selectedVersionId}
         onSelectVersion={props.onSelectVersion ?? (() => {})}
         {...(props.onReplaceVersion
            ? { onReplaceVersion: props.onReplaceVersion }
            : {})}
         onPackageDeleted={props.onPackageDeleted}
         canManage={props.canManage ?? true}
      />,
      { wrapper: serverWrapper },
   );
}

const pill = () =>
   screen.getByRole("button", { name: /change version$|^Select a version$/ });

/** Open the ⋯ menu of one version and return its items. */
async function openActions(id: string) {
   fireEvent.click(pill());
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

/**
 * Whether `scope` still says it is checking the versions. A boolean, so a
 * `waitFor` polling it while the read is under way fails cheaply: a failed
 * `toBeNull()` on a happy-dom element makes Bun format the element's whole
 * object graph, synchronously, for seconds per poll.
 */
const stillChecking = (scope: Pick<ReturnType<typeof within>, "queryByText">) =>
   scope.queryByText(/^Checking /) !== null;

/** Delete `id` from its ⋯ menu and return its dialog, once it has read the versions. */
async function startDelete(id: string) {
   fireEvent.click(item(await openActions(id), /Delete version/));
   const dialog = await screen.findByRole("dialog", { name: `Delete v${id}` });
   await waitFor(() => expect(stillChecking(within(dialog))).toBe(false));
   return dialog;
}

/** Whether a refresh asked for the environment's package list again. */
const refreshedPackages = () =>
   invalidate.mock.calls.some(
      ([filters]) =>
         JSON.stringify((filters as { queryKey?: unknown })?.queryKey) ===
         JSON.stringify(["packages", "env"]),
   );
/** Whether a refresh invalidated everything drawn for the package. */
const refreshedPackage = () =>
   invalidate.mock.calls.some(
      ([filters]) =>
         typeof (filters as { predicate?: unknown })?.predicate === "function",
   );

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

   it("is not offered where versions cannot be changed, where an archived row is marked disabled", async () => {
      renderSwitch(
         [
            version("1.1.0", { latest: true }),
            version("1.0.0", { archived: true }),
         ],
         { canManage: false },
      );
      fireEvent.click(pill());
      const list = await screen.findByRole("menu", { name: "Versions of pkg" });
      const rows = within(list).getAllByTestId("version-row");
      expect(rows).toHaveLength(2);
      expect(
         within(list).queryByRole("button", { name: /More actions/ }),
      ).toBeNull();
      for (const row of rows) {
         expect(row.getAttribute("aria-haspopup")).toBeNull();
      }
      // Nothing to offer, and it does not open.
      expect(rows[1].getAttribute("aria-disabled")).toBe("true");
      fireEvent.click(rows[1]);
      expect(
         screen.queryByRole("menu", { name: /Actions for version/ }),
      ).toBeNull();
   });

   it("offers an archived version's actions when it is chosen, since it does not open", async () => {
      const onSelectVersion = mock((_id: string) => {});
      renderSwitch(
         [
            version("1.1.0", { latest: true }),
            version("1.0.0", { archived: true }),
         ],
         { onSelectVersion },
      );
      fireEvent.click(pill());
      const list = await screen.findByRole("menu", { name: "Versions of pkg" });
      fireEvent.click(within(list).getAllByTestId("version-row")[1]);
      const menu = await screen.findByRole("menu", {
         name: "Actions for version 1.0.0",
      });
      expect(
         within(menu)
            .getAllByRole("menuitem")
            .map((i) => i.textContent),
      ).toEqual(["Unarchive", "Delete version"]);
      expect(onSelectVersion).not.toHaveBeenCalled();
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

   it("reads the versions again after a refusal, so the menu stops offering it", async () => {
      setPackageLatestVersion.mockImplementationOnce(() =>
         Promise.reject(
            Object.assign(new Error("Request failed with status code 410"), {
               response: { data: { message: "Version 1.0.0 is archived" } },
            }),
         ),
      );
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Pin as latest/));
      const dialog = await screen.findByRole("dialog", {
         name: "Pin v1.0.0 as latest",
      });
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Pin as latest" }),
      );
      expect(
         await within(dialog).findByText(/Version 1.0.0 is archived/),
      ).toBeTruthy();
      await waitFor(() => expect(refreshedPackage()).toBe(true));
      expect(refreshedPackages()).toBe(true);
   });
});

describe("Archive", () => {
   it("confirms first, and archives only once confirmed", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Archive/));
      const dialog = await screen.findByRole("dialog", {
         name: "Archive v1.0.0",
      });
      // What it drops is said before anything is.
      expect(
         within(dialog).getByText(/Tables built for this version alone/),
      ).toBeTruthy();
      expect(updatePackageVersion).not.toHaveBeenCalled();
      fireEvent.click(within(dialog).getByRole("button", { name: "Archive" }));
      await waitFor(() =>
         expect(updatePackageVersion).toHaveBeenCalledWith(
            "env",
            "pkg",
            "1.0.0",
            { archiveStatus: "archive" },
         ),
      );
   });

   it("cancelling archives nothing", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      fireEvent.click(item(await openActions("1.0.0"), /Archive/));
      const dialog = await screen.findByRole("dialog", {
         name: "Archive v1.0.0",
      });
      fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(updatePackageVersion).not.toHaveBeenCalled();
   });

   it("moves off the version on screen once it is archived, to latest in its place", async () => {
      const onReplaceVersion = mock((_id: string | undefined) => {});
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")], {
         selectedVersionId: "1.0.0",
         onReplaceVersion,
      });
      fireEvent.click(item(await openActions("1.0.0"), /Archive/));
      const dialog = await screen.findByRole("dialog", {
         name: "Archive v1.0.0",
      });
      fireEvent.click(within(dialog).getByRole("button", { name: "Archive" }));
      await waitFor(() =>
         expect(onReplaceVersion).toHaveBeenCalledWith(undefined),
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
      const dialog = await startDelete("1.0.0");
      expect(
         within(dialog).getByText(/Tables built for this version alone/),
      ).toBeTruthy();
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
      // The package and its environment's list are read again.
      await waitFor(() => expect(refreshedPackage()).toBe(true));
      expect(refreshedPackages()).toBe(true);
   });

   it("replaces the deleted version on screen with latest, when the host can", async () => {
      const onReplaceVersion = mock((_id: string | undefined) => {});
      const onSelectVersion = mock((_id: string) => {});
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")], {
         selectedVersionId: "1.0.0",
         onSelectVersion,
         onReplaceVersion,
      });
      const dialog = await startDelete("1.0.0");
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete version" }),
      );
      await waitFor(() =>
         expect(onReplaceVersion).toHaveBeenCalledWith(undefined),
      );
      expect(onSelectVersion).not.toHaveBeenCalled();
   });

   it("says deleting the only version deletes the package, and leaves the package", async () => {
      const onPackageDeleted = mock(() => {});
      renderSwitch([version("1.0.0", { latest: true })], { onPackageDeleted });
      const dialog = await startDelete("1.0.0");
      expect(
         within(dialog).getByText(/so deleting it deletes the package/),
      ).toBeTruthy();
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete package" }),
      );
      await waitFor(() => expect(onPackageDeleted).toHaveBeenCalledTimes(1));
      expect(deletePackageVersion).toHaveBeenCalledWith("env", "pkg", "1.0.0");
      expect(refreshedPackages()).toBe(true);
   });

   it("warns that the package goes when another tab left only this version", async () => {
      // The page lists two; another tab deleted 1.1.0 and 1.0.0 is all that is left.
      const onPackageDeleted = mock(() => {});
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")], {
         server: [version("1.0.0", { latest: true })],
         onPackageDeleted,
      });
      const dialog = await startDelete("1.0.0");
      expect(listPackageVersions).toHaveBeenCalled();
      expect(
         within(dialog).getByText(/so deleting it deletes the package/),
      ).toBeTruthy();
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete package" }),
      );
      await waitFor(() => expect(onPackageDeleted).toHaveBeenCalledTimes(1));
   });

   it("keeps the package when another tab published a version since, and opens latest", async () => {
      // The page lists only 1.0.0; another tab published 1.1.0 since.
      const onPackageDeleted = mock(() => {});
      const onReplaceVersion = mock((_id: string | undefined) => {});
      renderSwitch([version("1.0.0", { latest: true })], {
         server: [version("1.1.0", { latest: true }), version("1.0.0")],
         selectedVersionId: "1.0.0",
         onPackageDeleted,
         onReplaceVersion,
      });
      const dialog = await startDelete("1.0.0");
      expect(within(dialog).queryByText(/deletes the package/)).toBeNull();
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete version" }),
      );
      await waitFor(() =>
         expect(onReplaceVersion).toHaveBeenCalledWith(undefined),
      );
      expect(onPackageDeleted).not.toHaveBeenCalled();
   });

   it("says in the dialog when the fresh versions show the server would refuse", async () => {
      // The page shows no latest; another tab made 1.0.0 latest since.
      renderSwitch([version("1.0.0"), version("0.9.0")], {
         server: [version("1.0.0", { latest: true }), version("0.9.0")],
      });
      const dialog = await startDelete("1.0.0");
      expect(
         within(dialog).getByText(/Pin another version as latest first/),
      ).toBeTruthy();
      expect(
         within(dialog)
            .getByRole("button", { name: "Delete version" })
            .hasAttribute("disabled"),
      ).toBe(true);
   });

   it("opens at once, saying it is checking, and Delete waits for the read", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      const deleteItem = item(await openActions("1.0.0"), /Delete version/);
      fireEvent.click(deleteItem);
      // The click opens the dialog in the same render; the read of the
      // versions answers only after it, so here it is still under way.
      const checking = screen.getByText(
         /^Checking pkg's versions before deleting v1\.0\.0/,
      );
      // The dialog by a DOM selector, not a role query; the closing menu
      // still holds its own "Delete version" item.
      const dialog = checking.closest<HTMLElement>('[role="dialog"]')!;
      expect(dialog).not.toBeNull();
      const deleteButton = () =>
         within(dialog).getByText("Delete version").closest("button");
      expect(deleteButton()?.hasAttribute("disabled")).toBe(true);
      await waitFor(() => expect(stillChecking(within(dialog))).toBe(false));
      expect(deleteButton()?.hasAttribute("disabled")).toBe(false);
      expect(deletePackageVersion).not.toHaveBeenCalled();
   });

   it("closes its dialog and says so when the versions cannot be read", async () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      listFails = true;
      fireEvent.click(item(await openActions("1.0.0"), /Delete version/));
      expect(
         await screen.findByText(/Couldn't check v1.0.0 before deleting it/),
      ).toBeTruthy();
      // Closed: it no longer checks, nor offers a delete (its exit
      // transition may keep the element in the DOM a moment longer).
      await waitFor(() => expect(stillChecking(screen)).toBe(false));
      expect(screen.queryByText(/stops being served/)).toBeNull();
      expect(deletePackageVersion).not.toHaveBeenCalled();
   });

   it("leaves a package it deleted even when the versions cannot be read back, by the dialog's read", async () => {
      const onPackageDeleted = mock(() => {});
      renderSwitch([version("1.0.0", { latest: true })], { onPackageDeleted });
      const dialog = await startDelete("1.0.0");
      listFails = true;
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete package" }),
      );
      await waitFor(() => expect(onPackageDeleted).toHaveBeenCalledTimes(1));
   });

   it("shows the server's refusal in the dialog, and reads the versions again", async () => {
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
      const dialog = await startDelete("1.0.0");
      fireEvent.click(
         within(dialog).getByRole("button", { name: "Delete version" }),
      );
      expect(
         await within(dialog).findByText(
            /Couldn't delete v1.0.0: A materialization of version 1.0.0/,
         ),
      ).toBeTruthy();
      await waitFor(() => expect(refreshedPackage()).toBe(true));
   });
});

describe("by keyboard", () => {
   it("names the pill by the version it shows", () => {
      renderSwitch([version("1.1.0", { latest: true }), version("1.0.0")]);
      expect(
         screen.getByRole("button", { name: "Version v1.1.0, change version" }),
      ).toBeTruthy();
   });

   it("opens a row's actions with ArrowRight, focus inside, a refused action focusable", async () => {
      renderSwitch([
         version("1.1.0", { latest: true }),
         version("1.0.0", { archived: true }),
      ]);
      fireEvent.click(pill());
      const list = await screen.findByRole("menu", { name: "Versions of pkg" });
      const [latestRow, archivedRow] =
         within(list).getAllByTestId("version-row");
      expect(latestRow.getAttribute("aria-haspopup")).toBe("menu");
      // Archived, but not marked disabled: that would disable its ⋯ too.
      expect(archivedRow.getAttribute("aria-disabled")).toBeNull();

      fireEvent.keyDown(latestRow, { key: "ArrowRight" });
      const menu = await screen.findByRole("menu", {
         name: "Actions for version 1.1.0",
      });
      // Every action is refused on latest, so the menu itself takes focus, and
      // each action can still be reached, so its reason is read.
      await waitFor(() => expect(document.activeElement).toBe(menu));
      fireEvent.keyDown(menu, { key: "ArrowDown" });
      const pin = within(menu).getByRole("menuitem", { name: /Pin as latest/ });
      expect(pin.getAttribute("aria-disabled")).toBe("true");
      expect(document.activeElement).toBe(pin);
      fireEvent.keyDown(pin, { key: "ArrowDown" });
      expect(document.activeElement).toBe(
         within(menu).getByRole("menuitem", { name: /Archive/ }),
      );
   });

   it("opens an archived row's actions with Shift+F10", async () => {
      renderSwitch([
         version("1.1.0", { latest: true }),
         version("1.0.0", { archived: true }),
      ]);
      fireEvent.click(pill());
      const list = await screen.findByRole("menu", { name: "Versions of pkg" });
      const archivedRow = within(list).getAllByTestId("version-row")[1];
      fireEvent.keyDown(archivedRow, { key: "F10", shiftKey: true });
      const menu = await screen.findByRole("menu", {
         name: "Actions for version 1.0.0",
      });
      expect(
         within(menu)
            .getAllByRole("menuitem")
            .map((i) => i.textContent),
      ).toEqual(["Unarchive", "Delete version"]);
   });
});
