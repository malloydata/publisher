// One table renderer for every table tile: headings, alignment, and number
// formats all come from the field metadata, so a tile only has to name a view.

import { formatValue, monthOfYearLabel } from "./format.js";

const NUMERIC_FORMATS = new Set(["currency", "percent", "decimal", "integer"]);

/** Aggregates and formatted numbers align right; identifiers and labels do not. */
const isNumeric = (field) =>
   field.format === "id"
      ? false
      : field.isAggregate || NUMERIC_FORMATS.has(field.format);

const cellText = (row, field) =>
   field.name === "month_of_year"
      ? monthOfYearLabel(row[field.name])
      : formatValue(row[field.name], field.format);

export function renderTable(host, { fields, rows }) {
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
   for (const row of rows) {
      const tr = document.createElement("tr");
      for (const field of fields) {
         const td = document.createElement("td");
         // textContent, never innerHTML: a cell is data from the warehouse and
         // must never be parsed as markup.
         td.textContent = cellText(row, field);
         if (isNumeric(field)) td.classList.add("num");
         tr.appendChild(td);
      }
      body.appendChild(tr);
   }
   table.appendChild(body);
   host.appendChild(table);
}
