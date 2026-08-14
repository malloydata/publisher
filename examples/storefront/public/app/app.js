// Entry point: state, URL, and the render loop.
//
// State is the current tab plus a value per given, and it lives in the URL:
// `?tab=regions&REGION=West`. Which means a filtered view of this page is a link
// someone can send, and Back undoes a filter change.

import {
   barChart,
   destroyChart,
   lineChart,
   monthLabels,
   seriesLineChart,
   toSeries,
} from "./charts.js";
import { formatValue, monthOfYearLabel } from "./format.js";
import { buildControls, loadGivenContracts } from "./controls.js";
import { readResult } from "./result.js";
import { renderTable } from "./table.js";
import { DEFAULT_TAB, TABS, findTab, hasTab } from "./tabs.js";

const MODEL = "data_app.malloy";

const state = { tab: DEFAULT_TAB, givens: {} };
let contracts = [];
let controls = null;
/** Bumped per render so a slow tile from a previous filter cannot land late. */
let generation = 0;

const el = (id) => document.getElementById(id);

function showError(where, error) {
   console.error(where, error);
   const box = document.createElement("div");
   box.className = "err";
   box.textContent = `${where}: ${error?.message ?? error}`;
   el("errors").appendChild(box);
}

// ---- URL <-> state -----------------------------------------------------------

function readUrl() {
   const params = new URLSearchParams(location.search);
   const tab = params.get("tab");
   state.tab = tab && hasTab(tab) ? tab : DEFAULT_TAB;
   state.givens = {};
   const known = new Set(contracts.map((c) => c.name));
   for (const [key, value] of params) {
      // Contracts have not loaded on the first read; keep everything then, and
      // the next read (after they land) drops anything the model does not
      // declare. The server rejects an undeclared given anyway; this is so the
      // control row and the URL agree about what is in force.
      if (key === "tab") continue;
      if (contracts.length > 0 && !known.has(key)) continue;
      if (value !== "") state.givens[key] = value;
   }
}

function writeUrl({ push = false } = {}) {
   const params = new URLSearchParams();
   if (state.tab !== DEFAULT_TAB) params.set("tab", state.tab);
   for (const [name, value] of Object.entries(state.givens))
      if (value) params.set(name, value);
   const query = params.toString();
   const url = query ? `?${query}` : location.pathname;
   if (push) history.pushState({}, "", url);
   else history.replaceState({}, "", url);
}

/** Only the values that are set: an unset given leaves the model's default alone. */
const activeGivens = () =>
   Object.fromEntries(
      Object.entries(state.givens).filter(([, value]) => value !== ""),
   );

// ---- panels ------------------------------------------------------------------

const panels = new Map();

/** Build a tab's grid once, then reuse it: chart instances survive a refilter. */
function panelFor(tab) {
   const existing = panels.get(tab.slug);
   if (existing) return existing;

   const section = document.createElement("section");
   section.className = "panel";
   section.dataset.tab = tab.slug;
   section.hidden = true;

   const grid = document.createElement("div");
   grid.className = "grid";
   section.appendChild(grid);

   const kpiHost = document.createElement("div");
   kpiHost.className = "kpis";
   if (tab.kpis) grid.appendChild(kpiHost);

   const tiles = new Map();
   for (const tile of tab.tiles) {
      const card = document.createElement("section");
      card.className = "tile";
      card.style.setProperty("--colspan", String(tile.colspan));
      card.dataset.tile = tile.id;

      const heading = document.createElement("h2");
      heading.textContent = tile.title;
      if (tile.note) {
         const note = document.createElement("span");
         note.className = "tile-note";
         note.textContent = tile.note;
         heading.appendChild(note);
      }
      card.appendChild(heading);

      const body = document.createElement("div");
      body.className =
         tile.render === "table" ? "tile-body" : "tile-body chart-wrap";
      let canvas = null;
      if (tile.render !== "table") {
         canvas = document.createElement("canvas");
         body.appendChild(canvas);
      }
      card.appendChild(body);
      grid.appendChild(card);
      tiles.set(tile.id, { card, body, canvas });
   }

   el("panels").appendChild(section);
   const panel = { section, kpiHost, tiles };
   panels.set(tab.slug, panel);
   return panel;
}

function renderKpis(host, result, colspan) {
   host.replaceChildren();
   const row = result.rows[0] ?? {};
   for (const field of result.fields) {
      const card = document.createElement("div");
      card.className = "tile kpi";
      // An attribute rather than an id: every tab builds its own cards, and more
      // than one tab shows revenue.
      card.dataset.kpi = field.name;
      card.style.setProperty("--colspan", String(colspan));

      const label = document.createElement("div");
      label.className = "kpi-label";
      label.textContent = field.label;

      const value = document.createElement("div");
      value.className = "kpi-value";
      value.textContent = formatValue(row[field.name], field.format);

      card.append(label, value);
      host.appendChild(card);
   }
}

function renderTile(tile, target, result) {
   if (result.rows.length === 0 && tile.render !== "table") {
      if (target.canvas) destroyChart(target.canvas);
      target.body.replaceChildren();
      const empty = document.createElement("p");
      empty.className = "empty";
      empty.textContent = "No rows match the current filters.";
      target.body.appendChild(empty);
      return;
   }

   const format = (name) =>
      result.fields.find((field) => field.name === name)?.format;

   if (tile.render === "table") {
      renderTable(target.body, result);
      return;
   }

   // A chart tile's canvas is replaced if an empty state took its place.
   if (!target.body.contains(target.canvas)) {
      target.body.replaceChildren();
      target.canvas = document.createElement("canvas");
      target.body.appendChild(target.canvas);
   }

   const values = result.rows.map((row) => row[tile.y]);
   if (tile.render === "line") {
      lineChart(target.canvas, {
         labels: monthLabels(result.rows, tile.x),
         values,
         format: format(tile.y),
      });
   } else if (tile.render === "bar") {
      barChart(target.canvas, {
         labels: result.rows.map((row) => String(row[tile.x])),
         values,
         format: format(tile.y),
         horizontal: tile.horizontal === true,
      });
   } else if (tile.render === "series") {
      const { labels, series } = toSeries(result.rows, {
         x: tile.x,
         series: tile.series,
         value: tile.y,
         xLabel: monthOfYearLabel,
      });
      seriesLineChart(target.canvas, { labels, series, format: format(tile.y) });
   }
}

function tileError(target, error) {
   if (target.canvas) destroyChart(target.canvas);
   target.body.replaceChildren();
   const box = document.createElement("div");
   box.className = "err";
   box.textContent = error?.message ?? String(error);
   target.body.appendChild(box);
}

// ---- render ------------------------------------------------------------------

async function render() {
   const tab = findTab(state.tab);
   const panel = panelFor(tab);

   for (const [slug, other] of panels) other.section.hidden = slug !== state.tab;
   for (const button of document.querySelectorAll("#tabs button")) {
      const active = button.dataset.tab === state.tab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
   }

   const run = ++generation;
   const givens = activeGivens();
   panel.section.dataset.busy = "true";

   const work = [];
   if (tab.kpis) {
      work.push(
         Publisher.queryFull(MODEL, tab.kpis.query, { givens })
            .then((envelope) => {
               if (run !== generation) return;
               renderKpis(panel.kpiHost, readResult(envelope), tab.kpis.colspan);
            })
            .catch((error) => {
               if (run === generation) showError("Key figures", error);
            }),
      );
   }
   for (const tile of tab.tiles) {
      const target = panel.tiles.get(tile.id);
      work.push(
         Publisher.queryFull(MODEL, tile.query, { givens })
            .then((envelope) => {
               if (run !== generation) return;
               renderTile(tile, target, readResult(envelope));
            })
            .catch((error) => {
               if (run === generation) tileError(target, error);
            }),
      );
   }

   await Promise.all(work);
   if (run === generation) delete panel.section.dataset.busy;
}

// ---- controls and tabs -------------------------------------------------------

function syncControls() {
   for (const widget of controls?.widgets ?? [])
      widget.set(state.givens[widget.name] ?? "");
}

function onGivenChange(name, value) {
   if (name === null) {
      state.givens = {};
      syncControls();
   } else if (value === "") {
      delete state.givens[name];
   } else {
      state.givens[name] = value;
   }
   // Replaced, not pushed: Back should leave the page rather than walk every
   // filter someone tried.
   writeUrl();
   render();
}

function buildTabs() {
   const host = el("tabs");
   for (const tab of TABS) {
      const button = document.createElement("button");
      button.type = "button";
      button.role = "tab";
      button.dataset.tab = tab.slug;
      button.textContent = tab.label;
      button.addEventListener("click", () => {
         if (state.tab === tab.slug) return;
         state.tab = tab.slug;
         writeUrl({ push: true });
         render();
      });
      host.appendChild(button);
   }
}

window.addEventListener("popstate", () => {
   readUrl();
   syncControls();
   render();
});

async function main() {
   buildTabs();
   readUrl();
   render();

   try {
      contracts = await loadGivenContracts(MODEL);
      readUrl();
      controls = buildControls(el("filters"), {
         modelPath: MODEL,
         contracts,
         values: state.givens,
         onChange: onGivenChange,
      });
   } catch (error) {
      showError("Loading filters", error);
   }
}

main();
