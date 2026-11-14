<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# The token contract

Read when setting up an app's stylesheet, or when reskinning one for a client.

There is no shipped component library, so visual consistency across apps comes from every app defining the same *kinds* of token and every chart reading them at runtime. The values are free - that is what carries a client's identity. The contract is the shape.

## What to define on `:root`

| Group | Tokens |
|---|---|
| Surfaces | page background, card surface, a raised and a sunken variant, border, strong border |
| Text | strong, default, muted, soft - a real ramp, not one color at three opacities |
| Semantic | positive, negative, warning, each with a soft background variant for fills and badges |
| Chart palette | a categorical ramp of 6-8, ordered so the first is the brand accent, plus grid and axis-tick colors |
| **Type** | a **closed** size scale of exactly 8 steps, plus the weights, line-heights and letter-spacings you actually use |
| **Spacing** | a scale (4 / 8 / 12 / 16 / 24 / 32 / 48), not a single step that gets multiplied and divided in place |
| Structure | a radius scale (small, default, large), a shadow for cards and a deeper one for popovers |
| **Motion** | two transition durations (fast, base), one easing, and a separate duration for any looping animation |

**Every one of these groups needs the same mechanical rule, or half the contract quietly rots.** The colour half of this contract holds up in practice because it carries a rule you can check by grep - never a hex literal in a chart config. Type, spacing and motion have no such rule in most apps, and they are exactly where real apps drift: measured across three shipped data apps, each had 12 to 17 distinct ad-hoc font sizes and zero type tokens. The giveaway is half-point sizes (10.5px, 11.5px, 13.5px) - nobody designs those, you land on them by nudging one label at a time. So state the rule the same way for every group:

> A font-size, spacing value, radius, duration or colour that is not a token is a bug, the same as a hex literal in a chart config.

The scale being **closed** is what does the work. Eight type steps you must choose from produces hierarchy; an open-ended "define a type ramp" produces fourteen sizes a half-point apart. Pick the step nearest your intent and move on - if two things want to differ by half a point, they are the same step.

Build the scale on a **ratio**, not by listing sizes that feel right. Four steps inside a 3px range is the same crowding as half-points, one notch coarser: they invite the same nudging, and nothing in the scale tells you which to reach for. Roughly 1.2x between steps at the small end, widening toward the display sizes, gives steps that are visibly different from each other.

## The values are a decision, and the default is the problem

The contract fixes the *shape* of the palette. It does not choose the colours, and left unchosen an app lands on the same one every time: a cold blue-grey ground somewhere around `#0e1015`, slate surfaces, and a mid-blue accent near `#56a7e8`. That combination is the house style of every admin panel ever generated. It is not wrong; it is *unchosen*, and a reader can tell the difference immediately.

Measured on two real builds: both agents produced almost exactly that palette unprompted, and both apps read as generic until the hue was repitched deliberately.

So make two decisions on purpose and write down the reason:

- **Pick a temperature for the neutrals.** Warm greys (a paper-toned light ground, a warm charcoal dark one) read differently from the default cool slate and cost nothing. A data page is read for a long time; warmth is easier to sit with, and it stops the page looking like a framework default.
- **Derive the accent from something.** The domain, the client's brand, the material the data is about. An accent chosen for a reason is defensible and usually more distinctive than a stock blue. If nothing suggests one, a warm accent against cool neutrals (or the reverse) at least reads as a choice.

The test is whether you can say *why* this hue and not another. "It is the default" is the answer that produces the generic app.

A worked example. The neutrals here are deliberately **warm** and the accent is a burnt orange rather than the default indigo, to show what "chosen" looks like - swap both for whatever your own reason produces; what you must not do is leave them at the framework default:

```css
:root {
  --bg: #f4f1ee;          /* warm paper, not cool #f4f5f8 */
  --surface: #fefdfc;
  --surface-2: #f7f4f1;
  --border: #e6e0da;

  --text-strong: #17130f;  /* warm ink, so text sits on the paper */
  --text: #4a423b;
  --text-muted: #7d746b;

  --positive: #0e9f6e;  --positive-soft: #e3f6ef;
  --negative: #dc2626;  --negative-soft: #fdeaea;
  --warn: #d97706;

  --c1: #d95a12; --c2: #2f7ec9; --c3: #12805a; --c4: #8250d4;
  --c5: #a76d0a; --c6: #c73641; --c7: #17909e; --c8: #7d746b;
  --grid: #efeae4;
  --tick: #9c9289;

  /* Type: a closed scale, ratio-based. Pick the nearest step; never invent one between two. */
  --fs-xs: 10px;  --fs-sm: 12px;  --fs-md: 14px;  --fs-base: 16px;
  --fs-lg: 20px;  --fs-xl: 26px;  --fs-2xl: 34px; --fs-3xl: 46px;
  --fw-normal: 400; --fw-medium: 500; --fw-semi: 600; --fw-bold: 700;
  --fw-display: 800;   /* the headline numbers; see visual-craft.md on type contrast */
  --lh-tight: 1.2; --lh-body: 1.5;
  /* Tracking, or the nudging just migrates here from font-size. */
  --tr-tight: -0.01em; --tr-display: -0.02em; --tr-caps: 0.05em;

  /* Spacing: a scale, not one step multiplied in place. */
  --sp-1: 4px;  --sp-2: 8px;  --sp-3: 12px; --sp-4: 16px;
  --sp-5: 24px; --sp-6: 32px; --sp-7: 48px;

  --radius: 10px;
  /* Warm-toned shadows too, or they grey the paper. */
  --shadow-card: 0 1px 2px rgba(60, 44, 32, 0.05);
  --shadow-pop: 0 8px 28px rgba(60, 44, 32, 0.12);

  --dur-fast: 120ms; --dur-base: 200ms; --dur-loop: 1400ms;
  --ease: cubic-bezier(0.32, 0.72, 0, 1);

  /* Component dimensions. The spacing scale governs gaps and padding; an
     intrinsic size like a table row height is its own token, not a violation. */
  --row-h: 34px;
}
```

The spacing scale covers **gaps and padding**. A component's intrinsic dimension - a table row height, a control height, a sidebar width - will not always land on that ladder, and forcing it there makes rows cramped or loose. Give each such dimension its own named token instead, and use it everywhere that component appears; the rule is one source of truth per value, not that every value divides by four.

Honour the viewer's motion preference wherever you use those durations:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;   /* or an infinite loop still loops, just faster */
    transition-duration: 0.01ms !important;
    scroll-behavior: auto !important;
  }
}
```

Charts then resolve their colors from the contract rather than carrying literals:

```js
const token = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const palette = () => ["--c1","--c2","--c3","--c4","--c5","--c6","--c7","--c8"].map(token);
```

## Craft rules that carry most of the visual quality

- **Respect domain convention.** Finance shows negatives in parentheses, zero as a dash, and usually wants a unit toggle (units vs thousands vs millions); ops wants thresholds, staleness, and a clear "as of" time. Getting these wrong is what marks an app as generic to the people who live in the domain, regardless of how good the layout is.
- **Format at the edge, once.** Currency, percent, compact notation, and date labels belong in one shared formatter module, not re-derived per tile. Divergent rounding between two tiles showing the same measure is a bug users notice and lose trust over.
- **Every tile states what it is.** A title, plus a subtitle giving the definition or the window ("trailing 8 weeks", "excludes internal users"). A number whose definition is not on screen invites the wrong conclusion, and the person drawing it is rarely the person who built the model.
- **Reserve space for async content.** Give each tile its height before its query resolves, or the page reflows as tiles land and anything the user was reading jumps.
