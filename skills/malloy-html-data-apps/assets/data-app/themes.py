# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Write the dashboard kit's themes: kit/themes.css and kit/fonts/<face>.css.

Usage: python3 -I themes.py [kit dir]   (default: the kit beside it)

Each theme is a complete look: a paper, inks, one accent, chart colours, a band for a dark lead, good and bad, and a
display face for its headline figures. Light colours are chosen by hand from the subject; the dark ones are derived.
Every colour is then checked, and nudged until it passes: text 4.5:1 on its paper (headlines 7:1), marks 3:1, the
comparison 2.4:1, and the accent kept apart from bad. Run it after changing a theme; it prints what it nudged.
"""
import base64
import colorsys
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FACES = {  # display faces: file, family, licence, pinned sha256 (checked when fetched; see dash/fonts)
    "source-serif-4": ("source-serif-4_5.3.0_files_source-serif-4-latin-wght-normal.woff2", "Source Serif 4", "source-serif-4_5.3.0_LICENSE", "200 900", "normal"),
    "fraunces": ("fraunces_5.3.0_files_fraunces-latin-wght-normal.woff2", "Fraunces", "fraunces_5.3.0_LICENSE", "100 900", "normal"),
    "archivo": ("archivo_5.3.0_files_archivo-latin-wdth-normal.woff2", "Archivo", "archivo_5.3.0_LICENSE", "100 900", "62% 125%"),
}

# name: fits, display (face or None for Geist, 'mono' for Geist Mono), headline (display or sans), light colours
THEMES = {
    "paper": dict(fits="Any business subject; Credible's own look. The default.", display=None, headline="sans",
                  paper="#f8f7f4", surface="#ffffff", ink="#21201c", accent="#2e6a8e",
                  series=["#8aa36d", "#c27f6f", "#a7955e", "#8a7fa3", "#5e9a94"]),
    "ledger": dict(fits="Finance, revenue, pricing, costs, accounting, investment: money read like a statement.", display="source-serif-4", headline="display",
                   paper="#f3ecdd", surface="#fffcf4", ink="#1c1a16", accent="#1e3a5f",
                   series=["#b08a3e", "#6f8a5b", "#a35d4a", "#7a6f99", "#4f8a86"]),
    "clinic": dict(fits="Health, medicine, care, safety, science: careful and cool.", display=None, headline="sans",
                   paper="#edf3f3", surface="#ffffff", ink="#15211f", accent="#0e6f6a",
                   series=["#6f86b8", "#c48a5a", "#8f7fb8", "#b97a8f", "#7a9a5a"]),
    "transit": dict(fits="Operations, transport, logistics, mobility, energy: monitors read at a glance.", display="archivo", headline="display",
                    paper="#eef0ee", surface="#ffffff", ink="#141414", accent="#1f4fbf",
                    series=["#d07a3a", "#5a9a8f", "#8f6fb8", "#a59a5a", "#6e7f8f"]),
    "terminal": dict(fits="Software, engineering, AI, infrastructure, developer data.", display="mono", headline="sans",
                     paper="#fbfbfa", surface="#ffffff", ink="#111110", accent="#5b3fd6",
                     series=["#0f8a7e", "#c2650a", "#b0399a", "#3f7fd6", "#8a8a3a"]),
    "harvest": dict(fits="Food, agriculture, environment, climate, land and water.", display="fraunces", headline="display",
                    paper="#f0e8d6", surface="#fffaf0", ink="#211d14", accent="#2f6a52",
                    series=["#c08a3a", "#8a6a9a", "#b05a3a", "#5a7fa8", "#9a9a4a"]),
    "civic": dict(fits="Government, public data, elections, education, cities, justice.", display="source-serif-4", headline="display",
                  paper="#eff2f7", surface="#ffffff", ink="#131a2a", accent="#1f3a73",
                  series=["#c08a2e", "#6f8fbf", "#a35a5a", "#5a8a7a", "#8a7fb0"]),
    "bloom": dict(fits="Consumers, retail, media, entertainment, travel, people.", display="fraunces", headline="sans",
                  paper="#f8efeb", surface="#ffffff", ink="#221a1d", accent="#9b2c6b",
                  series=["#d08a3a", "#4f7fa8", "#7aa36a", "#8a6fb8", "#c27a8f"]),
    "stone": dict(fits="Internal tools, neutral reporting, anything that should recede.", display=None, headline="sans",
                  paper="#f2f1ef", surface="#ffffff", ink="#1e1e1c", accent="#3d3c38",
                  series=["#7a8fa3", "#b08b6e", "#8fa38a", "#a38fa8", "#6f9a9a"]),
}
LOOKS = {  # mast: paper | rule (a heavy ink rule under it) | bar (a thin accent bar over it) | band (a dark band)
    "paper": dict(mast="paper", rule="hair", figure=64),
    "ledger": dict(mast="rule", rule="ink", figure=70),
    "clinic": dict(mast="bar", rule="hair", figure=60),
    "transit": dict(mast="band", rule="ink", figure=84),
    "terminal": dict(mast="band", rule="hair", figure=56),
    "harvest": dict(mast="rule", rule="hair", figure=72),
    "civic": dict(mast="accent", rule="ink", figure=64),
    "bloom": dict(mast="paper", rule="hair", figure=76),
    "stone": dict(mast="paper", rule="hair", figure=60),
}
GOOD = {"light": "#3f7a3a", "dark": "#8fc07a"}
BAD = {"light": "#b1442f", "dark": "#ee8f7b"}
WATCH = {"light": "#9a6a10", "dark": "#e0b04f"}


def rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))


def hexc(c):
    return "#" + "".join(f"{max(0, min(255, round(v * 255))):02x}" for v in c)


def mix(a, b, t):
    """a toward b by t."""
    a, b = rgb(a), rgb(b)
    return hexc(tuple(x + (y - x) * t for x, y in zip(a, b)))


def lum(h):
    def ch(v):
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = (ch(v) for v in rgb(h))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a, b):
    la, lb = sorted((lum(a), lum(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def hue_gap(a, b):
    ha = colorsys.rgb_to_hls(*rgb(a))[0]
    hb = colorsys.rgb_to_hls(*rgb(b))[0]
    d = abs(ha - hb)
    return min(d, 1 - d) * 360


notes = []


def ensure(name, key, color, bg, need, toward):
    """Step `color` toward `toward` until it holds `need`:1 on `bg`."""
    c = color
    for i in range(40):
        if contrast(c, bg) >= need:
            break
        c = mix(c, toward, 0.06)
    if c != color:
        notes.append(f"{name}.{key}: {color} -> {c} for {need}:1")
    if contrast(c, bg) < need:
        sys.exit(f"{name}.{key}: cannot reach {need}:1 on {bg}")
    return c


def palette(name, t, mode):
    dark = mode == "dark"
    if not dark:
        paper, surface, ink = t["paper"], t["surface"], t["ink"]
        toward = ink
    else:
        paper = mix(t["ink"], "#000000", 0.25)
        surface = mix(paper, "#ffffff", 0.05)
        ink = mix(t["paper"], "#ffffff", 0.4)
        toward = "#ffffff"
    p = {"paper": paper, "surface": surface, "ink": ensure(name, "ink", ink, paper, 12, toward)}
    p["body"] = ensure(name, "body", mix(p["ink"], paper, 0.2 if not dark else 0.16), paper, 8, toward)
    p["muted"] = ensure(name, "muted", mix(p["ink"], paper, 0.45 if not dark else 0.38), paper, 4.6, toward)
    p["faint"] = ensure(name, "faint", mix(p["ink"], paper, 0.58 if not dark else 0.52), paper, 3.2, toward)
    p["rule"] = mix(paper, p["ink"], 0.09 if not dark else 0.12)
    p["rule-strong"] = mix(paper, p["ink"], 0.17 if not dark else 0.22)
    p["hover"] = mix(paper, p["ink"], 0.04 if not dark else 0.05)
    p["cmp"] = ensure(name, "cmp", mix(paper, p["ink"], 0.32 if not dark else 0.4), paper, 2.4, toward)
    p["cmp-bar"] = mix(paper, p["ink"], 0.1 if not dark else 0.14)
    accent = t["accent"] if not dark else mix(t["accent"], "#ffffff", 0.45)
    p["accent"] = ensure(name, "accent", accent, paper, 4.6, toward)
    p["good"] = ensure(name, "good", GOOD[mode], paper, 4.6, toward)
    p["bad"] = ensure(name, "bad", BAD[mode], paper, 4.6, toward)
    p["watch"] = ensure(name, "watch", WATCH[mode], paper, 4.6, toward)
    for i, s in enumerate(t["series"], start=2):
        s = s if not dark else mix(s, "#ffffff", 0.3)
        p[f"s{i}"] = ensure(name, f"s{i}", s, paper, 3.0 if i == 2 else 2.2, toward)
    # The band: a dark plane for a monitor's lead, in both modes.
    band = mix(t["ink"], t["accent"], 0.12) if not dark else mix(paper, p["accent"], 0.16)
    p["band"] = band
    p["band-ink"] = ensure(name, "band-ink", mix(t["paper"], "#ffffff", 0.3), band, 12, "#ffffff")
    p["band-muted"] = ensure(name, "band-muted", mix(band, "#ffffff", 0.62), band, 4.6, "#ffffff")
    p["band-rule"] = mix(band, "#ffffff", 0.14)
    p["band-cmp"] = ensure(name, "band-cmp", mix(band, "#ffffff", 0.42), band, 2.4, "#ffffff")
    p["band-accent"] = ensure(name, "band-accent", mix(t["accent"], "#ffffff", 0.5), band, 4.6, "#ffffff")
    p["band-good"] = ensure(name, "band-good", GOOD["dark"], band, 4.6, "#ffffff")
    p["band-bad"] = ensure(name, "band-bad", BAD["dark"], band, 4.6, "#ffffff")
    if hue_gap(p["accent"], p["bad"]) < 25 and name != "stone":
        sys.exit(f"{name} {mode}: the accent {p['accent']} is too close in hue to bad {p['bad']}")
    if hue_gap(p["accent"], p["good"]) < 20 and name != "stone":
        notes.append(f"{name} {mode}: accent near good in hue ({hue_gap(p['accent'], p['good']):.0f} deg); changes stay coloured text, the accent marks lines")
    return p


def block(sel, p, t, mode, indent=''):
    lines = [f"{sel} {{", f"  color-scheme: {mode};"]
    lines += [f"  --{k}: {v};" for k, v in p.items()]
    d = t["display"]
    fam = "var(--sans)" if d is None else "var(--mono)" if d == "mono" else f"\"{FACES[d][1]}\", Georgia, var(--sans)"
    lines += [f"  --display: {fam};", f"  --headline: {fam if t['headline'] == 'display' else 'var(--sans)'};",
              f"  --display-stretch: {'72%' if t['display'] == 'archivo' else '100%'};",
              f"  --display-weight: {500 if t['display'] in ('source-serif-4', 'fraunces') else 600};",
              f"  --display-face: \"{d if d in FACES else ''}\";"]
    lk = LOOKS[t["name"]]
    # The masthead's plane and inks: the page's own, a dark band, or the accent.
    if lk["mast"] in ("band", "accent"):
        bg = p["band"] if lk["mast"] == "band" else (t["accent"] if mode == "light" else p["band"])
        ink = ensure(t["name"], "mast-ink", "#ffffff", bg, 7, "#ffffff")
        lines += [f"  --mast-bg: {bg};", f"  --mast-ink: {ink};", f"  --mast-muted: {ensure(t['name'], 'mast-muted', mix(bg, '#ffffff', 0.68), bg, 4.6, '#ffffff')};", f"  --mast-rule: {mix(bg, '#ffffff', 0.18)};", "  --mast-line: transparent;"]
    else:
        lines += ["  --mast-bg: transparent;", f"  --mast-ink: {p['ink']};", f"  --mast-muted: {p['muted']};", f"  --mast-rule: {p['rule']};",
                  f"  --mast-line: {p['ink'] if lk['mast'] == 'rule' else p['rule']};"]
    lines += [f"  --mast-top: {p['accent'] if lk['mast'] == 'bar' else 'transparent'};",
              f"  --mast-line-w: {'2px' if lk['mast'] == 'rule' else '1px'};",
              f"  --section-rule: {p['ink'] if lk['rule'] == 'ink' else p['rule']};",
              f"  --figure-size: {lk['figure']}px;", "}"]
    return "\n".join(indent + l for l in lines)


def main(kit):
    for k, v in THEMES.items():
        v["name"] = k
    out = ["/* Copyright (c) Credible Data Inc. SPDX-License-Identifier: MIT */",
           "/* The kit's themes, written by dash/themes.py: edit there. Set dashboard({ theme }) or data-theme on <html>.",
           "   data-mode=\"dark\" or \"light\" fixes the mode; with neither, the reader's system setting decides. */", ""]
    for name, t in THEMES.items():
        light = palette(name, t, "light")
        dark = palette(name, t, "dark")
        root = ":root" if name == "paper" else f":root[data-theme=\"{name}\"]"
        out.append(f"/* {name}: {t['fits']} */")
        out.append(block(root, light, t, "light"))
        dsel = ":root:not([data-mode=\"light\"])" if name == "paper" else f":root[data-theme=\"{name}\"]:not([data-mode=\"light\"])"
        out.append("@media (prefers-color-scheme: dark) {\n" + block(dsel, dark, t, "dark", "  ") + "\n}")
        out.append(block(":root[data-mode=\"dark\"]" if name == "paper" else f":root[data-theme=\"{name}\"][data-mode=\"dark\"]", dark, t, "dark"))
        out.append("")
    open(os.path.join(kit, "themes.css"), "w").write("\n".join(out) + "\n")
    os.makedirs(os.path.join(kit, "fonts"), exist_ok=True)
    for face, (file, fam, lic, weight, stretch) in FACES.items():
        if not os.path.exists(os.path.join(HERE, "fonts", file)):
            continue  # the faces' sources live in the lab; without them only the colours are written
        b64 = base64.b64encode(open(os.path.join(HERE, "fonts", file), "rb").read()).decode()
        css = f"/* {fam}, SIL Open Font License 1.1: vendor/fonts/LICENSE-{face}.txt */\n@font-face {{ font-family: \"{fam}\"; font-style: normal; font-weight: {weight}; font-stretch: {stretch}; font-display: swap; src: url(data:font/woff2;base64,{b64}) format(\"woff2\"); }}\n"
        open(os.path.join(kit, "fonts", f"{face}.css"), "w").write(css)
        os.makedirs(os.path.join(kit, "vendor", "fonts"), exist_ok=True)
        open(os.path.join(kit, "vendor", "fonts", f"LICENSE-{face}.txt"), "w").write(open(os.path.join(HERE, "fonts", lic)).read())
    for n in notes:
        print("nudged", n)
    print(f"themes ok: {len(THEMES)} themes, light and dark, every text 4.5:1 and every mark checked")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "kit" if os.path.isdir(os.path.join(HERE, "kit")) else "kit2"))
