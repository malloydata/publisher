// One table renderer for every table tile: headings, alignment, number formats,
// and drill affordance all come from the field metadata, so a tile only has to
// name a view.

import { formatValue, monthOfYearLabel } from "./format.js";
import { DRILL_CELL_CLASS } from "./drill.js";

const NUMERIC_FORMATS = new Set(["currency", "percent", "decimal", "integer"]);

/** Aggregates and formatted numbers align right; identifiers and labels do not. */
const isNumeric = (field) =>
   field.format === "id" ? false : field.isAggregate || NUMERIC_FORMATS.has(field.format);

const cellText = (row, field) =>
   field.name === "month_of_year"
      ? monthOfYearLabel(row[field.name])
      : formatValue(row[field.name], field.format);

export function renderTable(host, { fields, rows }, drill) {
   host.replaceChildren();

   if (rows.length === 0) {
      const empty = document.createElement("p");
      empty.className = "empty";
      empty.textContent = "No rows match the current filters.";
      host.appendChild(empty);
      return;
   }

   const table = document.createElement("table");

   const head = document.createElement("thead");
   const headRow = document.createElement("tr");
   for (const field of fields) {
      const th = document.createElement("th");
      th.textContent = field.label;
      if (isNumeric(field)) th.className = "num";
      headRow.appendChild(th);
   }
   head.appendChild(headRow);
   table.appendChild(head);

   const body = document.createElement("tbody");
   const drillable = fields.map((field) => drill.canDrill(field));
   for (const row of rows) {
      const tr = document.createElement("tr");
      fields.forEach((field, i) => {
         const td = document.createElement("td");
         td.textContent = cellText(row, field);
         if (isNumeric(field)) td.classList.add("num");
         if (drillable[i]) {
            td.classList.add(DRILL_CELL_CLASS);
            td.dataset.fieldIndex = String(i);
         }
         tr.appendChild(td);
      });
      body.appendChild(tr);
   }
   table.appendChild(body);

   // Delegated, so a re-render costs one listener rather than one per cell.
   table.addEventListener("click", (event) => {
      const cell = event.target.closest(`.${DRILL_CELL_CLASS}`);
      if (!cell || !table.contains(cell)) return;
      const index = Number(cell.dataset.fieldIndex);
      const field = fields[index];
      const row = rows[cell.parentElement.rowIndex - 1];
      if (field && row) drill.click(field, row[field.name], event);
   });

   host.appendChild(table);
}
