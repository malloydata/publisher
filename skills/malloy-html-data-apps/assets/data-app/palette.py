# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Check a data app's colour tokens: contrast on the page, categories a reader can tell apart (also with red-green
colour blindness), a sequential ramp that darkens step by step (brightens, on a dark page), and one meaning per colour.

Usage: python3 -I palette.py <styles.css> [more.css ...] [--theme NAME [--mode light|dark]]: a later file's tokens
override an earlier one's. With --theme, a file's :root[data-theme="NAME"] block counts too, and with --mode dark its
:root[data-theme="NAME"][data-mode="dark"] block after it: run it once per mode for an app on a theme.
"""
import re
import sys

MARK, TEXT = 3.0, 4.5  # WCAG contrast: marks and large shapes, then text
APART, APART_CVD = 18.0, 10.0  # least CIELAB distance between two categories, normally and simulated


COLOURS = re.compile(r"(?:bg|panel|band(?:-text)?|text(?:-strong|-muted)?|accent|c-bar|up|down|c[1-6]|seq-[0-5])")


def tokens(css, theme=None, mode="light"):
    """Every --token in the top-level :root blocks, later ones winning; a :root inside @media is not the page. With a
    theme, that theme's light block, then its dark block for mode dark, after the plain :root blocks."""
    css, found, depth, i = re.sub(r"/\*.*?\*/", "", css, flags=re.S), {}, 0, 0
    while i < len(css):
        if depth == 0 and css.startswith(":root", i) and (m := re.match(r":root\s*\{([^}]*)\}", css[i:])):
            found.update((k, v.strip().lower()) for k, v in re.findall(r"--([\w-]+)\s*:\s*([^;]+)", m.group(1)))
            i += m.end()
            continue
        depth += {"{": 1, "}": -1}.get(css[i], 0)
        i += 1
    if theme:
        for dark in ([False, True] if mode == "dark" else [False]):
            sel = re.escape(f':root[data-theme="{theme}"]') + (re.escape('[data-mode="dark"]') if dark else "")
            for m in re.finditer(r"(?m)^" + sel + r"\s*\{([^}]*)\}", css):
                found.update((k, v.strip().lower()) for k, v in re.findall(r"--([\w-]+)\s*:\s*([^;]+)", m.group(1)))
    return found


def rgb(h):
    return [int(h[i:i + 2], 16) / 255 for i in (1, 3, 5)]


def linear(c):
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def luminance(h):
    r, g, b = (linear(c) for c in rgb(h))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a, b):
    la, lb = sorted((luminance(a), luminance(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def lab(lin_rgb):
    r, g, b = lin_rgb
    xyz = (0.4124 * r + 0.3576 * g + 0.1805 * b, 0.2126 * r + 0.7152 * g + 0.0722 * b, 0.0193 * r + 0.1192 * g + 0.9505 * b)
    f = [(t / n) ** (1 / 3) if t / n > 0.008856 else 7.787 * t / n + 16 / 116 for t, n in zip(xyz, (0.9505, 1.0, 1.089))]
    return 116 * f[1] - 16, 500 * (f[0] - f[1]), 200 * (f[1] - f[2])


# Machado et al. (2009), severity 1: how a colour looks with no working red or green cones.
NAMES = {"protan": "protanopia", "deutan": "deuteranopia"}
CVD = {
    "protan": ((0.152286, 1.052583, -0.204868), (0.114503, 0.786281, 0.099216), (-0.003882, -0.048116, 1.051998)),
    "deutan": ((0.367322, 0.860646, -0.227968), (0.280085, 0.672501, 0.047413), (-0.011820, 0.042940, 0.968881)),
}


def seen(h, kind=None):
    lin = [linear(c) for c in rgb(h)]
    if kind:
        lin = [max(0.0, min(1.0, sum(m * c for m, c in zip(row, lin)))) for row in CVD[kind]]
    return lab(lin)


def distance(a, b, kind=None):
    return sum((x - y) ** 2 for x, y in zip(seen(a, kind), seen(b, kind))) ** 0.5


def main(paths):
    theme = paths[paths.index("--theme") + 1] if "--theme" in paths else None
    mode = paths[paths.index("--mode") + 1] if "--mode" in paths else "light"
    paths = [p for i, p in enumerate(paths) if p not in ("--theme", "--mode") and (i == 0 or paths[i - 1] not in ("--theme", "--mode"))]
    if not paths or mode not in ("light", "dark"):
        print(__doc__)
        return 2
    t = {}
    for path in paths:
        t.update(tokens(open(path, encoding="utf-8").read(), theme, mode))
    fails, notes = [], []
    for name in [k for k in t if COLOURS.fullmatch(k)]:
        if not re.fullmatch(r"#[0-9a-f]{6}", t[name]):
            fails.append(f"--{name} is {t.pop(name)}: write it as six-digit hex so it can be checked")
    if not any(COLOURS.fullmatch(k) for k in t):
        fails.append(f"no colour tokens found in a top-level :root block of {', '.join(paths)}")
    bg = t.get("bg", "#ffffff")
    for name in ("text-strong", "text", "text-muted", "accent", "up", "down"):  # the last three colour links and deltas
        for ground in ("bg", "panel"):  # the headline numbers and the footer sit on the panel
            if name in t and contrast(t[name], t.get(ground, bg)) < TEXT:
                fails.append(f"--{name} {t[name]} is {contrast(t[name], t.get(ground, bg)):.2f}:1 on --{ground}, under {TEXT}:1 for text")
    if "band" in t and contrast(t.get("band-text", "#ffffff"), t["band"]) < TEXT:  # the masthead and its text
        fails.append(f"--band-text on --band {t['band']} is {contrast(t.get('band-text', '#ffffff'), t['band']):.2f}:1, under {TEXT}:1 for text")
    cats = [f"c{i}" for i in range(1, 7) if f"c{i}" in t]
    for name in ["c-bar", *cats]:
        if name in t and contrast(t[name], bg) < MARK:
            fails.append(f"--{name} {t[name]} is {contrast(t[name], bg):.2f}:1 on --bg, under {MARK}:1 for a mark")
    named = cats
    for i, a in enumerate(named):
        for b in named[i + 1:]:
            d = distance(t[a], t[b])
            if d < APART:
                fails.append(f"--{a} and --{b} are {d:.0f} apart, under {APART:.0f}: too alike to tell apart")
            for kind in CVD:
                dk = distance(t[a], t[b], kind)
                if d >= APART and dk < APART_CVD:
                    notes.append(f"note: --{a} and --{b} are {dk:.0f} apart with {NAMES[kind]}; label them directly wherever both appear")
    if "up" in t and "down" in t:
        for kind in CVD:
            if distance(t["up"], t["down"], kind) < APART_CVD:
                fails.append(f"--up and --down look alike with {NAMES[kind]}: change one's lightness, not only its hue")
    # On a light page the ramp darkens step by step; on a dark page it brightens, so a high value always stands out.
    dark = luminance(bg) < 0.2
    ramp = [t.get(f"seq-{i}") for i in range(6)]
    if all(ramp):
        lights = [seen(c)[0] for c in ramp]
        for i in range(5):
            step = lights[i + 1] - lights[i] if dark else lights[i] - lights[i + 1]
            if step < 5:
                fails.append(f"--seq-{i + 1} {ramp[i + 1]} is not clearly {'brighter' if dark else 'darker'} than --seq-{i} {ramp[i]} (lightness {lights[i]:.0f} to {lights[i + 1]:.0f})")
    if "accent" in t:
        for n in cats[1:]:
            if distance(t["accent"], t[n]) < APART:
                fails.append(f"--{n} is the accent's colour: the accent may only be the lead category, --c1")
    for f in fails + notes:
        print(f)
    print("palette ok" if not fails else f"{len(fails)} problem(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
