// Drill: making a table cell a link to somewhere, the way a dashboard does.
//
// A `# drill { to=[…] given=… }` tag sits on a model *dimension*, so every result
// that groups by that dimension is clickable wherever it is rendered. The Console
// resolves a click off the field's annotations with no drill-specific API, and
// this page does the same thing with the metadata `result.js` already read. What
// each destination means:
//
//   to=<slug>   the tab of that name, with the clicked value written into the
//               named given (the dashboards navigate to a page; here the tabs
//               *are* the dashboards, so it is a tab switch)
//   to=self     filter where you are, leaving the tab alone
//
// Two rules make the affordance honest, both of them the Console's:
//
//   1. Only destinations this page can actually honor are offered, and only those
//      cells are marked. A dead link that looks live is worse than plain text.
//   2. An aggregate is never drillable, even when the tag reaches it. The total
//      of a column is not the value someone clicked.

import { encodeFilterValue, humanizeSlug } from "./format.js";

/** Matches the Console's class name, and the CSS in index.html styles it. */
export const DRILL_CELL_CLASS = "publisher-drill";

const SELF = "self";

export function createDrill({ hasTab, onNavigate, onSelf, selfLabel }) {
   /** The destinations that lead somewhere, in the order the model lists them. */
   function targets(field) {
      if (!field?.drill || field.isAggregate) return [];
      return field.drill.to
         .filter((to) => (to === SELF ? true : hasTab(to)))
         .map((to) => ({ to, given: field.drill.given }));
   }

   const canDrill = (field) => targets(field).length > 0;

   function act(target, value) {
      if (target.to === SELF) onSelf(target.given, value);
      else onNavigate(target.to, target.given, value);
   }

   /**
    * Resolve a click. Returns false when nothing happened, so a caller can leave
    * the event alone: a value that cannot be a filter (a null, a nested cell) is
    * not a drill, however its column is tagged.
    */
   function click(field, rawValue, event) {
      const value = encodeFilterValue(rawValue);
      if (value === undefined) return false;
      const found = targets(field);
      if (found.length === 0) return false;
      if (found.length === 1) {
         act(found[0], value);
         return true;
      }
      openMenu(event, String(rawValue), found, value);
      return true;
   }

   // ---- the menu, for a dimension with more than one destination -------------
   let menu = null;

   function closeMenu() {
      menu?.remove();
      menu = null;
      document.removeEventListener("keydown", onKeydown, true);
   }

   function onKeydown(event) {
      if (event.key === "Escape") closeMenu();
   }

   function openMenu(event, label, found, value) {
      closeMenu();
      menu = document.createElement("div");
      menu.className = "drill-menu";
      menu.setAttribute("role", "menu");

      const heading = document.createElement("div");
      heading.className = "drill-menu-label";
      heading.textContent = `${label} \u2192`;
      menu.appendChild(heading);

      for (const target of found) {
         const item = document.createElement("button");
         item.type = "button";
         item.className = "drill-menu-item";
         item.setAttribute("role", "menuitem");
         item.textContent =
            target.to === SELF ? selfLabel : humanizeSlug(target.to);
         item.addEventListener("click", () => {
            closeMenu();
            act(target, value);
         });
         menu.appendChild(item);
      }

      document.body.appendChild(menu);
      // Placed at the pointer, then pulled back inside the viewport if the menu
      // would hang off the right or bottom edge.
      const { width, height } = menu.getBoundingClientRect();
      const x = Math.min(event.clientX, window.innerWidth - width - 8);
      const y = Math.min(event.clientY, window.innerHeight - height - 8);
      menu.style.left = `${Math.max(8, x)}px`;
      menu.style.top = `${Math.max(8, y)}px`;

      document.addEventListener("keydown", onKeydown, true);
      // The click that opened the menu is still travelling; wait for the next
      // frame before listening, or the menu would close on its own opening click.
      requestAnimationFrame(() => {
         document.addEventListener(
            "pointerdown",
            (e) => {
               if (menu && !menu.contains(e.target)) closeMenu();
            },
            { once: true },
         );
      });
   }

   return { canDrill, click, closeMenu };
}
