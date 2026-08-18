import { ListSubheader, Menu, MenuItem } from "@mui/material";
import { useCallback, useMemo, useState, type ReactNode } from "react";
import type { DrillField } from "./resolveDrill";
import {
   DRILL_SELF,
   drillDestinations,
   drillGivenName,
   humanizeSlug,
   resolveDrill,
   type DrillClickPayload,
   type DrillIntent,
   type DrillNavigation,
} from "./resolveDrill";

export interface UseDrillOptions {
   /**
    * Open another dashboard, seeded with the clicked value. Omitted on a surface
    * that cannot navigate, which makes those destinations inert rather than
    * dead-ended.
    */
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   /**
    * Apply the clicked value to this surface's own control row (`to=self`).
    * Omitted on a surface that has none, such as a notebook cell.
    */
   onSelf?: (given: string, rawValue: unknown) => void;
   /**
    * Whether a `to=self` drill onto this given can actually be honoured: for a
    * notebook, whether it declares that given at all.
    *
    * Without it the affordance says yes to every self-drill and the click is
    * refused afterwards with a console warning, which is the dead link this
    * module exists to avoid. Absent means "any self-drill is fine", the right
    * default for a surface that binds whatever it is handed.
    */
   canSelf?: (given: string) => boolean;
   /**
    * What `to=self` is called in the menu. A notebook filtering itself is not
    * "this dashboard", and the drill tag: written on a shared model dimension,
    * cannot know which document the reader is in, so the surface names itself.
    */
   selfLabel?: string;
}

/**
 * What a surface hands to its result renderer to become drillable: the click
 * handler, and the predicate that decides which cells say so.
 *
 * One object because the two must agree. Passing only the handler is what
 * produced the original asymmetry: clicks worked, but nothing looked clickable.
 */
export interface DrillBinding {
   /**
    * Every cell click. Most carry no `# drill` and mean nothing, so this
    * tolerates being called constantly.
    */
   onClick: (payload: DrillClickPayload) => void;
   /** Whether this field's cells should read as clickable. */
   canDrill: (field: DrillField) => boolean;
}

export interface UseDrillResult {
   /** Hand to `ResultContainer`'s (or `RenderedResult`'s) `drill` prop. */
   drill: DrillBinding;
   /** Render anywhere in the subtree: the menu a multi-destination drill opens. */
   drillMenu: ReactNode;
}

interface DrillMenuState {
   intent: DrillIntent;
   destinations: string[];
   event?: MouseEvent;
   top: number;
   left: number;
}

/**
 * Click-to-drill for any surface that renders a Malloy result.
 *
 * Shared between the dashboard viewer and notebook cells on purpose: `# drill`
 * is declared on a model *dimension*, so it fires wherever that dimension is
 * grouped, and a notebook can therefore drill into a dashboard with no notebook-
 * specific code. What differs between surfaces is only what they can do about it,
 * which is what the two callbacks express.
 */
export function useDrill({
   onNavigate,
   onSelf,
   selfLabel = "Filter this view",
   canSelf,
}: UseDrillOptions): UseDrillResult {
   // A cell inside the renderer's own DOM is the anchor, and this component
   // holds no ref to it, so the menu is positioned from the click coordinates.
   const [menu, setMenu] = useState<DrillMenuState | null>(null);

   // What this surface can actually do about a destination. Applied to the click
   // and to the affordance from one place, so a cell reads as clickable exactly
   // when clicking it would do something.
   const honorable = useCallback(
      (destinations: string[], given: string) =>
         destinations.filter((destination) =>
            destination === DRILL_SELF
               ? onSelf !== undefined && (canSelf?.(given) ?? true)
               : onNavigate !== undefined,
         ),
      [onNavigate, onSelf, canSelf],
   );

   const go = useCallback(
      (intent: DrillIntent, destination: string, event?: MouseEvent) => {
         if (destination === DRILL_SELF) {
            // The raw value, not `intent.value`: only this surface knows the
            // declared type of its own given, so only it can encode correctly.
            onSelf?.(intent.given, intent.rawValue);
            return;
         }
         onNavigate?.(
            {
               dashboard: destination,
               givens: { [intent.given]: intent.value },
            },
            event,
         );
      },
      [onNavigate, onSelf],
   );

   const onClick = useCallback(
      (payload: DrillClickPayload) => {
         // Runs on every cell click, so "not a drill" is the ordinary answer.
         const intent = resolveDrill(payload);
         if (!intent) return;

         // Offer only what this surface can actually do, so a menu never lists a
         // destination that would be a no-op when picked.
         const destinations = honorable(intent.to, intent.given);
         if (destinations.length === 0) return;
         if (destinations.length === 1) {
            go(intent, destinations[0], payload.event);
            return;
         }
         setMenu({
            intent,
            destinations,
            event: payload.event,
            top: payload.event?.clientY ?? 0,
            left: payload.event?.clientX ?? 0,
         });
      },
      [go, honorable],
   );

   const canDrill = useCallback(
      (field: DrillField) =>
         honorable(drillDestinations(field), drillGivenName(field)).length > 0,
      [honorable],
   );

   const drill = useMemo<DrillBinding>(
      () => ({ onClick, canDrill }),
      [onClick, canDrill],
   );

   const close = useCallback(() => setMenu(null), []);

   return {
      drill,
      drillMenu: (
         <Menu
            open={menu !== null}
            onClose={close}
            anchorReference="anchorPosition"
            anchorPosition={{ top: menu?.top ?? 0, left: menu?.left ?? 0 }}
         >
            <ListSubheader sx={{ lineHeight: "32px" }}>
               {menu ? `${menu.intent.label} →` : ""}
            </ListSubheader>
            {/* Deduplicated: `# drill { to=[overview, overview] }` is a typo
                rather than a request for two identical rows, and React keys
                these by destination, so the duplicate warned as well as
                rendered twice. */}
            {Array.from(new Set(menu?.destinations ?? [])).map(
               (destination) => (
                  <MenuItem
                     key={destination}
                     onClick={() => {
                        close();
                        if (menu) go(menu.intent, destination, menu.event);
                     }}
                  >
                     {destination === DRILL_SELF
                        ? selfLabel
                        : humanizeSlug(destination)}
                  </MenuItem>
               ),
            )}
         </Menu>
      ),
   };
}
