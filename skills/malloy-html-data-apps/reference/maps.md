<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Maps With the d3 Chart Module

When the pattern in space is the finding, draw a map. A choropleth of states, counties or countries answers "where" at
a glance, and a click on a place filters the app or opens its page. Boundaries ship as TopoJSON, which is text, and are drawn by the
chart module's `choropleth` with d3 and topojson-client. Every file is pinned and checked, like d3. Data with
coordinates and no boundaries (a complaint's latitude and longitude) is drawn with `hexMap`, below.

## Which boundaries

| Places in the data | File | Object | Key on each shape | Size |
|---|---|---|---|---|
| US states | `us-atlas@3.0.1/states-albers-10m.json` | `states` | `id`: 2-digit FIPS (`"36"`); `properties.name` (`"New York"`) | 82 KB |
| US counties | `us-atlas@3.0.1/counties-albers-10m.json` | `counties` (borders: `states`) | `id`: 5-digit FIPS (`"36061"`) | 795 KB |
| Countries | `world-atlas@2.0.2/countries-110m.json` | `countries` | `id`: ISO 3166-1 numeric (`"840"`); `properties.name` | 108 KB |

The US files are already projected (Albers USA, Alaska and Hawaii inset) to a 975 by 610 box; the world file is in
longitude and latitude and is projected with Equal Earth. Match on FIPS or ISO codes when the data has them; names
differ between sources ("Côte d'Ivoire", "Ivory Coast"), so when you join on names, list every name that found no
shape, and fix them in the model, not in the app. Counties are large: use them only when the data is by county, and put a ranked table beside the map, since county
shapes take no keyboard focus.
Canadian provinces and other regions have no packaged open file here; draw those as a ranked table instead.

## Vendor

d3 and topojson-client are already in the kit (`public/kit/vendor/`). Fetch the one boundary file the app needs, from
the package directory:

```sh
mkdir -p public/vendor/maps
cd public/vendor
base=https://cdn.jsdelivr.net/npm
curl -sSL --fail --max-time 60 -o maps/states-albers-10m.json $base/us-atlas@3.0.1/states-albers-10m.json
curl -sSL --fail --max-time 60 -o maps/LICENSE-atlas.txt $base/us-atlas@3.0.1/LICENSE
sha256sum -c - <<'SUMS'
6e7bb086a3c791490361968a3094f377f7726c5d0c4900fec03cc42db2305a3d  maps/states-albers-10m.json
8048290dfdb6e83fbed17e8985c8cfc4ce9da9b842642f3d3e497280790cfa31  maps/LICENSE-atlas.txt
SUMS
[ $? -eq 0 ] || { rm -f maps/states-albers-10m.json maps/LICENSE-atlas.txt; echo "sum differs: draw a ranked table instead"; exit 1; }
cd ../..
```

On a Mac without `sha256sum`, use `shasum -a 256 -c -` in its place.

The other two boundary files, with their sums:

```
a674dfa31b625e92635f32684a61ae135b05a91507f17f2d5164833237fecf46  maps/counties-albers-10m.json   (us-atlas@3.0.1)
2516c915867c7baf18ddec727aec46c315541a07cfb3d79a6559b05d5e94eee8  maps/countries-110m.json        (world-atlas@2.0.2)
```

Never keep a file whose sum differs. If the fetch fails or the sum differs, draw the places as a ranked table instead and say so. topojson-client carries its licence in `kit/vendor/`. Add to
`public/vendor/README.md` the boundary file, ISC license, from us-atlas (US Census Bureau cartographic boundaries, public domain) or
world-atlas (Natural Earth, public domain). Credit the boundaries in About the data. Load topojson-client with a
plain script before `app.js` in `index.html`: `<script src="./kit/vendor/topojson-client.min.js"></script>`.
It is 7 KB, and the module calls it only once d3 is in.

## Drawing a map

A `block()` of your own draws a choropleth like this; the SVG keeps its proportions as the width changes, and a
click filters or opens the place:

```js
draw: ([rows], body) => C.loadTopo('./vendor/maps/states-albers-10m.json').then((topo) =>
  C.choropleth(body.appendChild(document.createElement('div')), topo, new Map(rows.map((r) => [r.state, r.production])), {
    object: 'states', legendTitle: 'Million lb', onPick: (s) => store.toggleFilter('state', s),
  })),
```

The legend's classes are quintiles of the values shown; say so in the tile's method note.

Points with coordinates go on `hexMap`: one row per location with its count, `group_by: lat, lon` with a `limit` above
the number of locations (a query without one stops at the server's default row cap), and the same query with no
filters as `opts.base`, drawn grey underneath so the place keeps its shape. It needs no boundary file. Its hexagons are
clicked, not tabbed to: put a ranked table beside it.

A map earns its place only when the pattern in space is the finding: neighbours that move together, a gradient
across a region, a cluster along a border. Being about places is not a reason for a map. A map is not a ranking either: where the shapes take no
keyboard focus (counties, hexagons), the ranked table beside it is how a keyboard reader reaches each place; a
ranked table beside a states map that says the same thing is a repeat.
