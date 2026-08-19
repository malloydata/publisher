// Thin entry point: render each tile independently so one failing query cannot blank the page.

const state = { env: null, pkg: null };

function card(tile) {
   const el = document.createElement("section");
   el.className = "card" + (tile.wide ? " wide" : "");
   el.innerHTML = `<h2>${tile.title}</h2>
     ${tile.caption ? `<p class="caption">${tile.caption}</p>` : ""}
     <div class="body"><p class="muted">Loading…</p></div>`;
   return el;
}

async function runTile(tile, el) {
   const body = el.querySelector(".body");
   if (tile.placeholder) {
      body.innerHTML = `<div class="missing">${tile.placeholder}</div>`;
      return;
   }
   try {
      const rows = await Publisher.query(tile.model, tile.query);
      if (!rows || rows.length === 0) {
         body.innerHTML = `<p class="muted">No rows returned.</p>`;
         return;
      }
      body.innerHTML = tile.render(rows);
   } catch (err) {
      // Surface the server's reason rather than a generic failure: a compile error, a
      // curated-out source and an authorization denial need different responses.
      const detail = err && (err.response || err.message) ? String(err.response || err.message) : String(err);
      body.innerHTML = `<div class="error"><strong>Query failed.</strong>
        <code>${detail.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])).slice(0, 400)}</code></div>`;
   }
}

export function mount(tiles) {
   const grid = document.getElementById("grid");
   for (const tile of tiles) {
      const el = card(tile);
      grid.appendChild(el);
      runTile(tile, el);
   }
}
