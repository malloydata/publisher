// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { AppDialog } from "./AppDialog";
import { BackLink } from "./BackLink";
import { ItemRow } from "./ItemRow";
import { PackageSection } from "./PackageSection";
import { AddButton, SecondaryButton } from "./buttons";

/**
 * The five components every Console screen is built out of. They carry the
 * contracts the rest of the app assumes and the Playwright suite selects on —
 * accessible names, landmark roles, keyboard reachability — and none of it was
 * covered. A screenshot cannot show any of it: a row that is not reachable by
 * keyboard and a row that is look exactly alike.
 */

describe("AddButton", () => {
   it("announces the verb while showing only the noun", () => {
      // The plus carries "add", so the label is the thing being added. The
      // accessible name has to put the verb back, or every add button on the
      // page announces as a bare noun.
      render(<AddButton label="Package" onClick={() => {}} />);
      const button = screen.getByRole("button", { name: "Add package" });
      expect(button.textContent).toContain("Package");
      expect(button.textContent).not.toContain("Add package");
   });

   it("calls back when clicked", () => {
      const onClick = mock(() => {});
      render(<AddButton label="Connection" onClick={onClick} />);
      fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
      expect(onClick).toHaveBeenCalledTimes(1);
   });
});

describe("SecondaryButton", () => {
   it("uses the label as its name unless given another", () => {
      render(<SecondaryButton label="Edit" onClick={() => {}} />);
      expect(screen.getByRole("button", { name: "Edit" })).toBeTruthy();
   });

   it("takes an explicit accessible name for a label that is not unique", () => {
      render(
         <SecondaryButton
            label="Settings"
            ariaLabel="Settings for Orders"
            onClick={() => {}}
         />,
      );
      expect(
         screen.getByRole("button", { name: "Settings for Orders" }),
      ).toBeTruthy();
   });

   it("does not fire while disabled", () => {
      const onClick = mock(() => {});
      render(
         <SecondaryButton
            label="Save"
            disabled
            disabledReason="Nothing to save"
            onClick={onClick}
         />,
      );
      fireEvent.click(screen.getByRole("button", { name: "Save" }));
      expect(onClick).not.toHaveBeenCalled();
   });
});

describe("PackageSection", () => {
   it("is a landmark named by its own heading", () => {
      // What makes "Dashboards" addressable to a screen reader moving by
      // region, and to a test that wants the dashboards list rather than
      // every row on the page sharing a word with it.
      render(
         <PackageSection title="Dashboards" count={2}>
            <div>a row</div>
         </PackageSection>,
      );
      const region = screen.getByRole("region", { name: "Dashboards" });
      expect(within(region).getByText("a row")).toBeTruthy();
      expect(
         within(region).getByRole("heading", { name: "Dashboards", level: 6 }),
      ).toBeTruthy();
   });

   it("shows a count of zero rather than hiding it", () => {
      render(
         <PackageSection title="Notebooks" count={0}>
            <div />
         </PackageSection>,
      );
      expect(screen.getByText("(0)")).toBeTruthy();
   });

   it("omits the count entirely when none is given", () => {
      render(
         <PackageSection title="Readme">
            <div />
         </PackageSection>,
      );
      expect(screen.queryByText(/^\(\d+\)$/)).toBeNull();
   });

   it("puts the action inside the section, on the heading's row", () => {
      render(
         <PackageSection
            title="Packages"
            action={<AddButton label="Package" onClick={() => {}} />}
         >
            <div />
         </PackageSection>,
      );
      const region = screen.getByRole("region", { name: "Packages" });
      expect(
         within(region).getByRole("button", { name: "Add package" }),
      ).toBeTruthy();
   });
});

describe("ItemRow", () => {
   const row = (props: Partial<Parameters<typeof ItemRow>[0]> = {}) =>
      render(
         <ItemRow
            label="storefront"
            description="An ecommerce model"
            tint="#2563eb"
            icon={<span />}
            onClick={props.onClick ?? (() => {})}
            {...props}
         />,
      );

   it("is operable by keyboard, not only by pointer", () => {
      // It is a div wearing role=button, so Enter and Space are the component's
      // own responsibility; a real button would get them from the platform.
      const onClick = mock(() => {});
      row({ onClick, ariaLabel: "storefront" });
      const target = screen.getByRole("button", { name: "storefront" });
      fireEvent.keyDown(target, { key: "Enter" });
      fireEvent.keyDown(target, { key: " " });
      expect(onClick).toHaveBeenCalledTimes(2);
   });

   it("ignores keys that are not activation keys", () => {
      const onClick = mock(() => {});
      row({ onClick, ariaLabel: "storefront" });
      fireEvent.keyDown(screen.getByRole("button", { name: "storefront" }), {
         key: "a",
      });
      expect(onClick).not.toHaveBeenCalled();
   });

   it("takes an explicit name so a lookup is not a guess at concatenated text", () => {
      // Without it the accessible name is the row's whole text content, label
      // and description run together, which makes every exact-name lookup a
      // guess at someone's prose.
      row({ ariaLabel: "storefront package" });
      const target = screen.getByRole("button", { name: "storefront package" });
      expect(target.textContent).toContain("An ecommerce model");
   });

   it("falls back to the row's text when given no explicit name", () => {
      row();
      expect(
         screen.getByRole("button", { name: /storefront.*ecommerce model/ }),
      ).toBeTruthy();
   });

   it("still shows the description it does not put in the name", () => {
      row({ ariaLabel: "storefront" });
      expect(screen.getByText("An ecommerce model")).toBeTruthy();
   });
});

describe("AppDialog", () => {
   it("is named by its own title", () => {
      render(
         <AppDialog open onClose={() => {}} title="New dashboard">
            <div>body</div>
         </AppDialog>,
      );
      expect(
         screen.getByRole("dialog", { name: "New dashboard" }),
      ).toBeTruthy();
   });

   it("renders nothing while closed", () => {
      render(
         <AppDialog open={false} onClose={() => {}} title="New dashboard">
            <div>body</div>
         </AppDialog>,
      );
      expect(screen.queryByRole("dialog")).toBeNull();
   });

   it("shows the description and the actions it is given", () => {
      render(
         <AppDialog
            open
            onClose={() => {}}
            title="Delete package"
            description="This cannot be undone."
            actions={<button>Delete package</button>}
         >
            <div />
         </AppDialog>,
      );
      expect(screen.getByText("This cannot be undone.")).toBeTruthy();
      expect(
         screen.getByRole("button", { name: "Delete package" }),
      ).toBeTruthy();
   });

   it("offers a way out only when asked for one", () => {
      const onClose = mock(() => {});
      const { rerender } = render(
         <AppDialog open onClose={onClose} title="Rows">
            <div />
         </AppDialog>,
      );
      expect(screen.queryByRole("button", { name: "Close" })).toBeNull();
      rerender(
         <AppDialog open onClose={onClose} title="Rows" showClose>
            <div />
         </AppDialog>,
      );
      fireEvent.click(screen.getByRole("button", { name: "Close" }));
      expect(onClose).toHaveBeenCalledTimes(1);
   });
});

describe("BackLink", () => {
   it("names the parent it goes to", () => {
      render(<BackLink label="storefront" onClick={() => {}} />);
      expect(
         screen.getByRole("button", { name: "Back to storefront" }),
      ).toBeTruthy();
   });

   it("is a real link when there is a URL, so it can be opened in a new tab", () => {
      // The regression: it used to render an `<a>` with no href, which is
      // neither focusable nor announced as a link, and swallows a middle-click.
      render(
         <BackLink
            label="storefront"
            href="/examples/storefront"
            onClick={() => {}}
         />,
      );
      const link = screen.getByRole("link", { name: "Back to storefront" });
      expect(link.getAttribute("href")).toBe("/examples/storefront");
   });

   it("is still focusable when there is no URL to point at", () => {
      render(<BackLink label="Publisher" onClick={() => {}} />);
      const control = screen.getByRole("button", { name: "Back to Publisher" });
      control.focus();
      expect(document.activeElement).toBe(control);
   });

   it("hands the host the click, with its event", () => {
      const onClick = mock(() => {});
      render(<BackLink label="Publisher" onClick={onClick} />);
      fireEvent.click(
         screen.getByRole("button", { name: "Back to Publisher" }),
      );
      expect(onClick).toHaveBeenCalledTimes(1);
   });

   it("suppresses the anchor's own navigation on a plain click", () => {
      // The regression: with an href present and the default left alone, the
      // page navigated twice — the host's router, and then a full document
      // load that undid it.
      const onClick = mock(() => {});
      render(
         <BackLink
            label="storefront"
            href="/examples/storefront"
            onClick={onClick}
         />,
      );
      const link = screen.getByRole("link", { name: "Back to storefront" });
      const event = new MouseEvent("click", {
         bubbles: true,
         cancelable: true,
      });
      link.dispatchEvent(event);
      expect(onClick).toHaveBeenCalledTimes(1);
      expect(event.defaultPrevented).toBe(true);
   });

   it("leaves a modified click to the browser, so it opens a new tab", () => {
      // The whole reason for the href. Handling this one would swallow it.
      const onClick = mock(() => {});
      render(
         <BackLink
            label="storefront"
            href="/examples/storefront"
            onClick={onClick}
         />,
      );
      const link = screen.getByRole("link", { name: "Back to storefront" });
      const event = new MouseEvent("click", {
         bubbles: true,
         cancelable: true,
         metaKey: true,
      });
      link.dispatchEvent(event);
      expect(onClick).not.toHaveBeenCalled();
      expect(event.defaultPrevented).toBe(false);
   });
});
