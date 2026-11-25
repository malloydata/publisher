// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DOM helpers. Text goes in through textContent only: nothing here assigns
// innerHTML, so a value from the model can never become markup.

const ATTR = /^(aria-|data-|role$|for$)/;

/** Create an element. `props` keys that are attributes (aria-*, data-*, role) are set as attributes. */
export function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "text") node.textContent = String(v);
    else if (k === "class") node.className = v;
    else if (ATTR.test(k)) node.setAttribute(k, v === true ? "" : String(v));
    else node[k] = v;
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return node;
}

/** Replace a container's children. */
export function fill(container, ...children) {
  container.replaceChildren(...children.flat().filter(Boolean));
}

/** A tile's error state: the tile says what failed, the rest of the page carries on. */
export function errorNote(err) {
  const status = err?.status ? ` (${err.status})` : "";
  const msg = err?.response?.message ?? err?.message ?? String(err);
  return el("p", {
    class: "note is-error",
    role: "alert",
    text: `Query failed${status}: ${msg}`,
  });
}

const SVG = "http://www.w3.org/2000/svg";

/** An SVG element with attributes. */
export function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  return node;
}
