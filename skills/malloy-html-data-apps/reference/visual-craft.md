<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# Visual craft

> `token-contract.md` is the floor: it buys **consistency**, and an app can satisfy every rule in it and still look like an internal admin tool. This file is the ceiling. Read it when the app should feel like a product someone chose, not a dashboard someone generated.

The failure this prevents is specific and it is easy to miss, because nothing about it is *wrong*: every tile is the same rounded rectangle, on the same surface, with the same border, laid out in a uniform grid. Tokens are all correct. Hierarchy exists on paper - the lead tiles are bigger - but they are the same object bigger, so the page still reads flat. Measured on a real build, that app cleared every token rule and still read as generic.

Seven things separate that from an app that feels designed. None of them is decoration; each is doing a job.

## 1. Fewer containers, then elevation

**Start by deleting boxes.** One `--surface` used eleven times is a grid of boxes, and the fix is not a better box or a taller ladder: it is that most content does not want a container at all. Sections separated by a hairline and generous whitespace on a single canvas read as a designed page; the same sections wrapped in eleven bordered, shadowed rectangles read as a generated one, no matter how good the tokens are.

This is the correction that mattered most on a real build. An app that satisfied every rule in this file, with a full four-level ladder and no `.card` class anywhere, still read as generic because every tile sat in its own container. Redrawing it as one sheet divided by hairlines, with exactly one lifted surface (the drawer), was the single biggest change.

So the order is:

1. **Default to no container.** A heading, a subtitle and the content, on the page ground. Let whitespace and a hairline do the separating.
2. **Add a container only when something must read as a separate plane** - something that floats over the page (a drawer, a popover, a tray), or one region that genuinely leads (a hero band).
3. **Then** give the few surfaces you kept a real elevation difference, using the ladder below.

A useful check: count the bordered rectangles on your first screen. More than two or three and you are almost certainly boxing things that would read better as bare sections.

Where you do keep a surface, a designed page has a small, deliberate **elevation ladder**, and a level says what a thing is.

| Level | What sits here | How it reads |
|---|---|---|
| 0 | page ground | the darkest (or lightest) plane, nothing floats on it directly |
| 1 | ordinary tiles | the working surface, subtle border, minimal shadow |
| 2 | the lead tiles, an open drawer | lifted: lighter surface, a real shadow, often a soft gradient |
| 3 | popovers, menus, tooltips | clearly above everything, strongest shadow, no border needed |

Three things make a level read as lifted, and you generally want at least two:

- **A lighter surface**, not a different hue. A gradient of 2-4% over the base surface is enough, and it beats a flat fill because the top edge catches light.
- **A shadow that matches the level.** A 1px shadow at level 2 is indistinguishable from level 1, which is why the "shadow-card / shadow-pop" pair in the contract is a minimum, not a palette.
- **A hairline highlight on the top edge** (`inset 0 1px 0 rgba(255,255,255,.06)` on dark). This is the cheapest trick in the file and it does more than a shadow: it is what makes a surface look like a physical panel rather than a coloured rectangle.

Borders are for *separation*, not elevation. A tile that is lifted needs less border, not more.

Elevation has consequences for what sits *inside* a panel, and both of these bite immediately: a sticky table header inside a tile needs its own opaque surface background, or rows scroll visibly underneath it, and it needs a stacking context, or in-cell bars and chips draw over it.

## 2. The accent must appear at scale, or the app is monochrome

A palette with a brand accent that only ever appears on a 2px hairline and a sort arrow reads as grey. The accent has to carry something structural: the selected row, the primary series in every chart, the focused control, the active nav item, the one number that matters most on the page.

The discipline that keeps this from turning gaudy: **one accent moment per region**, where a region is the unit the eye scans as a group - a row of lead tiles is one region, not three; a table is one; a chart is one. The eye should be able to find "the thing this section is about" without reading. If two things in a region are accented, neither is.

Reserve the semantic colours (positive/negative) for *values*, never for chrome. A green button and a green number in the same view means green stopped meaning anything.

## 3. Charts need craft, not just a correct type

A correct chart type drawn with library defaults is the single biggest tell of a generated app.

**This section applies to a chart you draw by hand as well.** A small SVG written directly is often the right call for a sparkline, a gauge or a single annotated shape, and it does not escape any of the rules below: hand-drawn text still collides with the marks it labels, still needs a measured gutter, and still needs the grid turned down. The only thing that changes is that there is no library default to turn down, which makes it easier to get right and easier to forget. The chart is usually the largest object on the page and it is where the least design attention goes.

- **Label the data directly.** A legend makes the reader look away from the marks and back again. Put the series name at the end of its line, the category on its bar, the notable points on the scatter. A legend is a fallback for when direct labelling genuinely will not fit. Expect to hand-write this: most chart libraries have no direct-label or annotation layer, so it is a small custom plugin drawing on the canvas. Two traps make the first attempt look broken - text drawn at the plot's right edge prints *over* the marks, so reserve a gutter in the layout and draw past the edge into it, and that gutter has to be **measured** with the same font or the longest label clips.
- **A scatter where you cannot identify a point is a texture, not a chart.** Label the outliers and the extremes, at minimum. If the user has selected an entity, that point is accented and every other point recedes.
- **Recede the grid.** Gridlines should be the faintest thing in the frame - they are scaffolding, not data. Kill the ones you do not need entirely, and never draw a border around the whole plot.
- **Annotate the meaning.** A quadrant chart with unlabelled quadrants makes every reader derive the same insight from scratch. "Good offence, poor defence" in the corner costs nothing and is the actual content.
- **Turn the library's defaults down before anything else.** Every chart library ships weights tuned to be visible in a demo: thick bars in a saturated primary, 1px gridlines at full opacity, heavy axis lines, large points. A chart left at those defaults is recognisable as a default from across the room, however good the form choice was. Concretely: bars and lines take the palette token rather than the library colour, gridlines drop to the faintest token you have (or disappear), axis lines and tick marks go off, and point radius comes down until the marks stop competing with each other. This is usually a dozen lines of options and it is the difference between "a chart" and "a designed chart".
- **Ink goes to data.** Axis lines, tick marks, and borders are almost always too heavy at library defaults. Turn them down until the data is the only confident thing in the frame.

## 4. Density: a number earns its space

A 44px number with a large empty region under it is not emphasis, it is unfinished layout. Emphasis comes from *contrast with its neighbours*, not from absolute size, so a big number in a sparse card reads as smaller than a medium number in a dense one.

Two rules of thumb:

**Three equal KPI cards across the top is the most recognisable generated-app tell there is.** Every generated dashboard opens with it, and it is wrong twice over: three identical containers repeated side by side is repetition rather than a headline, and three equal-weight numbers mean the page has no subject. Build **one hero band** instead - a single region, panels divided by hairlines rather than gaps, and the leading number given a genuinely larger type step than its two supporting neighbours. The reader should be able to tell which number the page is about without reading a word.

- **A lead tile should carry its supporting evidence,** not just its headline: the number, a comparison (versus last period, versus the field), and a small trend. That is what fills the space honestly, and it is more useful than the number alone.
- **Give the hero at most a third of the first screen.** If the three lead tiles push the actual content below the fold, the page has led with its title rather than its substance. An explorer especially should show the population immediately.

The most common structural cause of a sparse-looking page is not typographic at all, and it is invisible in a stylesheet diff: **a card whose content has a fixed height, sitting in a stretch-aligned grid row next to a taller sibling.** The card stretches, the content does not, and the difference becomes dead space under a chart. Either let the content flex to fill its card, or stop the row from stretching. Check this first when a page looks empty but every tile is correct.

**An in-cell encoding needs its own column, not the cell behind the number.** A magnitude bar drawn *behind* a value is the muddiness problem again - a translucent wash under text that reads as a rendering artefact - and a short rule *under* each number is thirty unrelated stubs. What works is a narrow column of its own holding a diverging rule anchored at a shared zero: it becomes one scale you can scan down the table, and the number stays clean. Drop the encoding only if you cannot give it that column.

Tables are where density pays. A stat table with generous row height wastes the one format that can show twenty things at once - tighten the rows, right-align the numbers, use tabular figures, and let the table be dense on purpose.

## 5. Motion is feedback, not decoration

Every state change the user causes should be acknowledged, and nothing else should move.

- **Hover states on everything interactive**, and they must be visible: a surface shift, not a 2% opacity change nobody perceives. A row that opens a drawer on click must look clickable before the click.
- **Transitions belong on state, not on load.** Animating *content* in on first paint delays the thing the reader came for. Animating a drawer open, a filter chip appearing, a sort re-ordering: those are feedback, and they are still right when the state came from the URL on first load.
- **Focus states are not optional and not the browser default.** A keyboard user needs a visible ring drawn from the accent.
- Keep it fast. Anything over ~250ms on a UI transition feels sluggish; the contract's `--dur-fast` / `--dur-base` pair exists so these stay consistent.

## 6. Type carries personality

The default stack at 400/600/700 is the house style of every admin panel. Two cheap decisions move an app a long way:

- **Pair a display face with the UI face.** Numbers are usually the subject in a data app: setting the headline figures in something with character (a tighter grotesque, a condensed face, or the same family at a much heavier weight with tight tracking) separates "the answer" from "the interface" before anyone reads a word. **Bundle the face with the app** - a font file in the package, never a CDN or a hosted-fonts stylesheet, for the same reason chart libraries are vendored: the page may be served somewhere with no network egress, and a font that silently fails to load takes the whole typographic idea with it. A *variable* font is the cheap win here, since one file buys the heavy display weight the contract's regular ceiling otherwise blocks.
- **Use tabular figures everywhere numbers align**, and proportional ones inside prose and names. Mixed tabular figures in a proper noun look broken.

If the app uses one family, get the contrast from weight and tracking instead: display numbers at the heaviest weight with negative tracking, labels small at uppercase with positive tracking, body in the middle. The gap between the extremes is what reads as designed.

**Vendor assets, and inline the small ones.** A chart library gets copied into the package; a logo or icon mark should be **inlined into the markup** rather than linked, for two reasons. A linked file fails silently to a blank square when the page is served from somewhere with no network egress, which is exactly where these apps run. And an inlined SVG can carry `fill="currentColor"` instead of a baked hex, so the mark follows a theme swap like everything else. Strip any XML prolog, DOCTYPE and editor metadata first, and keep the source file and its licence alongside the vendored library.

## 7. It has to survive a narrower window

A data app is a wide layout full of the two things that refuse to shrink: tables and charts. It will be opened in a half-screen window, an embedded panel, and a laptop narrower than yours, and the failure is not subtle - text prints on top of a plot.

**Collisions do not show up as page overflow.** This is the trap. The obvious check is `document.documentElement.scrollWidth > clientWidth`, and on a real build that came back clean at every width *while a standings table was visibly overlapping the scatter chart beside it*. A grid column clips nothing by default, so its content happily draws over the neighbour without widening the document.

The obvious next idea is to compare bounding rects instead, and **that lies too, in exactly the case the fix below creates**. Once you cap a table's container and let it scroll inside itself, the table's own rect is legitimately wider than its container while being visually clipped - so a naive rect comparison reports a collision on every scrollable table on the page. Measured on a real build: false positives at four widths, all of them fine. If you automate this at all, walk the ancestors and ignore overflow inside a **scrollable** box (`overflow-x: auto | scroll`), where the content is reachable. Do not extend that to a clipping box (`hidden`, `contain`): content lost inside one of those is a real bug, and suppressing it is how you stop seeing the thing you were looking for.

**The only check that does not lie is rendering at several widths and looking at the screenshots.** Automate the capture, not the judgement.

Three rules carry most of it:

- **A table will exceed its grid column.** Its min-content width is the sum of its columns, so `grid-template-columns: minmax(0, 1fr)` is not enough on its own: the track can shrink while the table inside it does not. Cap the table's container explicitly and let it scroll inside itself. This is the single most common collision in a data app.
- **Give up the least useful thing first, in stages.** Two breakpoints that collapse every two-column band at once turn a wide page into a very tall one - three full-width headline panels, each with a sparkline stretched across the sheet. A workable progression: narrow the chrome first, then let the headline drop a column, then stack the content bands, then turn a side rail into a top bar, and only then let wide tables scroll.
- **A chart's minimum useful width is a design decision, not a default.** A sparkline stretched over a full sheet flattens into a horizon and stops reading as a trend; cap it. A scatter below roughly 320px cannot carry axis labels and marks at once; stack it instead.

Also check the things that live outside the normal flow, because they are easy to forget and they are where the real breakage hides: an off-canvas drawer (does it become full-width, and does its internal grid drop to two columns?), a fixed bottom tray (does it wrap?), a sticky header (does it still clear the content?), and any element whose width is set in JavaScript rather than CSS.

## The check

Before calling the visual pass done, look at a screenshot of the first screen and answer honestly:

- Can you tell what the page is *about* in one second, without reading?
- Is there anything on screen that a reader would call the focal point?
- How many bordered rectangles are on the first screen? More than two or three means you are boxing things that should be bare sections.
- Does the page open with three equal KPI cards? If so it has no subject yet.
- Could you tell the palette was chosen, or is it the default cool slate and mid-blue?
- Do the charts look like the library's defaults - thick saturated bars, full-opacity gridlines, visible axis lines?
- Could you identify a specific entity in each chart, or only see a shape?
- Does the accent appear anywhere that matters?
- Would you describe the page as dense, or as large elements with gaps between them?
- Does it still hold together at 1200px and at 900px? (Render it and look. An overflow check will not catch a collision.)

A "no" to most of these describes an app that satisfies the token contract and nothing more, which is exactly the app this file exists to prevent.

## When restyling is not working, build a mockup instead

If two passes over an existing app have produced "better, but still generic", stop restyling it. Editing inside the app's own markup keeps reproducing the app's structure: the cards are already there, the interactions constrain the layout, and every change becomes a local correction to something that is wrong globally.

Build a **static mockup** instead - one self-contained HTML file, real numbers pasted in from a handful of queries, no framework, no live data, no interaction. It costs an hour and it removes every constraint that was keeping the design where it was. On a real build this is what surfaced a side rail replacing the top bar and a single canvas replacing eleven cards; neither was reachable by editing the running app, and both ported back cleanly afterwards because the app's render targets were stable ids.

Two things make the port work: **use real numbers in the mockup** (fake data hides the widths that will actually break), and **agree the mockup before porting**, because the port is mechanical and the design decisions are not.
