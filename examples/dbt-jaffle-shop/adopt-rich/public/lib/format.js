// Pure formatting helpers. No DOM, no Publisher: unit-testable with `node --test`.

/** Money. dbt records no display formatting, so the model's `# currency` tag is the source
 *  of truth for which fields land here; this only does the rendering. */
export function money(v) {
   if (v === null || v === undefined || Number.isNaN(v)) return "—";
   return "$" + Number(v).toLocaleString("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
   });
}

/** A fraction rendered as a percentage. The model returns fractions, matching its
 *  `# percent` tag; dbt's own metric returns percentage points, a 100x difference. */
export function percent(v, digits = 1) {
   if (v === null || v === undefined || Number.isNaN(v)) return "—";
   return (Number(v) * 100).toFixed(digits) + "%";
}

export function count(v) {
   if (v === null || v === undefined || Number.isNaN(v)) return "—";
   return Number(v).toLocaleString("en-US");
}

/** Month label from a Malloy timestamp value. */
export function month(v) {
   if (!v) return "—";
   const d = new Date(v);
   if (Number.isNaN(d.getTime())) return String(v);
   return d.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
}

/** "Current" means the most recent row that actually has a value. A trailing null is
 *  missing data, not zero, so it must not be plotted or reported as the current figure. */
export function latestNonNull(rows, key) {
   for (let i = rows.length - 1; i >= 0; i--) {
      const v = rows[i]?.[key];
      if (v !== null && v !== undefined && !Number.isNaN(v)) return rows[i];
   }
   return undefined;
}

/** Month-over-month change computed in the page, for a model that has no such measure.
 *  Returns null for the first row and wherever either side is missing: a missing point is
 *  not a 0% change. */
export function withMoM(rows, key) {
   return rows.map((r, i) => {
      const prev = i > 0 ? rows[i - 1]?.[key] : null;
      const cur = r?.[key];
      const usable = prev !== null && prev !== undefined && prev !== 0 &&
                     cur !== null && cur !== undefined;
      return { ...r, mom: usable ? (cur - prev) / prev : null };
   });
}
