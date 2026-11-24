# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""The data app themes: each a full set of colour tokens for a light page and a dark one.

Writes themes.css (one block per theme and mode, plus the system-preference fallback), then checks every theme in both
modes with palette.py beside it. Usage: python3 -I themes.py [out themes.css, default ./themes.css]. Run it after any
change to a theme here, never edit themes.css by hand.
"""
import subprocess
import sys
from pathlib import Path

# Page neutrals. A theme names one for light and one for dark; the accent and data colours are its own.
NEUTRALS = {
    "paper": dict(bg="#fbf8f2", surface="#fffdf9", **{"surface-2": "#f1ece2"}, panel="#f3eee4", nodata="#d9d3c7", border="#e4ddd0", **{"border-strong": "#c9bfae"},
                  band="#1d1a16", **{"band-text": "#f6f1e8"}, **{"text-strong": "#1d1a16", "text": "#3b362f", "text-muted": "#655d53", "text-soft": "#9a9184"}, grid="#ebe5da", **{"logo-ink": "#1a1a1a"}),
    "mist": dict(bg="#f6f8fa", surface="#ffffff", **{"surface-2": "#ebeff3"}, panel="#edf1f5", nodata="#cfd6dd", border="#dde3e9", **{"border-strong": "#bfc8d1"},
                 band="#111820", **{"band-text": "#eef3f8"}, **{"text-strong": "#111820", "text": "#2c3743", "text-muted": "#57636f", "text-soft": "#8b96a1"}, grid="#e2e8ee", **{"logo-ink": "#1a1a1a"}),
    "white": dict(bg="#ffffff", surface="#ffffff", **{"surface-2": "#f3f3f2"}, panel="#f5f5f4", nodata="#d6d6d3", border="#e6e6e3", **{"border-strong": "#c7c7c3"},
                  band="#161616", **{"band-text": "#f5f5f5"}, **{"text-strong": "#161616", "text": "#363636", "text-muted": "#626262", "text-soft": "#979797"}, grid="#ececea", **{"logo-ink": "#1a1a1a"}),
    "sand": dict(bg="#f8f1e6", surface="#fdf8f0", **{"surface-2": "#efe5d5"}, panel="#f1e7d7", nodata="#d8ccb8", border="#e4d8c4", **{"border-strong": "#c8b89e"},
                 band="#2a2017", **{"band-text": "#f8f0e4"}, **{"text-strong": "#221a12", "text": "#40352a", "text-muted": "#6a5c4c", "text-soft": "#9c8d7a"}, grid="#eadfcc", **{"logo-ink": "#1a1a1a"}),
    "ember-dark": dict(bg="#1a1714", surface="#211d19", **{"surface-2": "#2a2621"}, panel="#24201c", nodata="#3d3730", border="#36312b", **{"border-strong": "#4f4840"},
                       band="#0f0d0b", **{"band-text": "#f4efe8"}, **{"text-strong": "#f6f1ea", "text": "#ddd6cc", "text-muted": "#aca296", "text-soft": "#7f776c"}, grid="#2c2823", **{"logo-ink": "#ffffff"}),
    "night": dict(bg="#10151b", surface="#151c24", **{"surface-2": "#1c2530"}, panel="#18202a", nodata="#2e3a47", border="#26313e", **{"border-strong": "#3b4858"},
                  band="#0a0e13", **{"band-text": "#eef3f8"}, **{"text-strong": "#eef3f8", "text": "#cfd8e1", "text-muted": "#9aa7b4", "text-soft": "#6d7a88"}, grid="#1f2834", **{"logo-ink": "#ffffff"}),
    "carbon": dict(bg="#121212", surface="#181818", **{"surface-2": "#202020"}, panel="#1b1b1b", nodata="#333333", border="#2c2c2c", **{"border-strong": "#454545"},
                   band="#0a0a0a", **{"band-text": "#f2f2f2"}, **{"text-strong": "#f2f2f2", "text": "#d4d4d4", "text-muted": "#a3a3a3", "text-soft": "#767676"}, grid="#242424", **{"logo-ink": "#ffffff"}),
}

# Each theme: a line on when it suits a subject, its light and dark neutrals, then its colours in each mode.
# cats: c1 (the lead, the accent) to c6. seq: the ramp, low to high. up/down: bad and good news.
THEMES = {
    "ember": dict(fits="Heat, fire, energy, prices, cost of living: warm subjects where the lead is a rising number.",
        light=dict(neutral="paper", accent="#b0501c", soft="#f6e4d6", up="#b3261e", down="#1e6b52", bar="#7d8a96",
                   cats=["#b0501c", "#2f5d8a", "#3f7f5f", "#7a4f9a", "#1f7f8c", "#857b70"], seq=["#f7efe5", "#f0d6bd", "#e2ad84", "#cc834f", "#a85b22", "#6b3410"]),
        dark=dict(neutral="ember-dark", accent="#f0904f", soft="#3a2618", up="#ff8a7a", down="#5fcf9f", bar="#8f9aa5",
                  cats=["#f0904f", "#74abe0", "#6fc193", "#b892dd", "#5cc6d1", "#b1a79a"], seq=["#2a2019", "#4a2f1d", "#734222", "#a55a27", "#d67b39", "#f6b27a"])),
    "harbor": dict(fits="Water, shipping, weather, cities, transport: cool, civic subjects.",
        light=dict(neutral="mist", accent="#0f5f8c", soft="#dcebf4", up="#b3261e", down="#1d6b4f", bar="#7b8794",
                   cats=["#0f5f8c", "#c2571a", "#2a8466", "#6b4fa0", "#b23a5b", "#7a858f"], seq=["#edf3f8", "#cfe0ed", "#9fc3dd", "#6a9fc6", "#3b78a8", "#174a73"]),
        dark=dict(neutral="night", accent="#62b1e6", soft="#16314a", up="#ff8a7a", down="#5fcf9f", bar="#8b98a6",
                  cats=["#62b1e6", "#f2945a", "#5fcfa8", "#a98ee3", "#ee7d9d", "#a5afba"], seq=["#14212c", "#18364d", "#1f5072", "#2a6d99", "#3f90c2", "#7ec0e8"])),
    "meadow": dict(fits="Nature, farming, food, birds, land, health: living, growing subjects.",
        light=dict(neutral="paper", accent="#2f6f3e", soft="#dfeedf", up="#b3261e", down="#285f9e", bar="#7d857a",
                   cats=["#2f6f3e", "#b5651d", "#3d5f9e", "#9a3d6b", "#8a7414", "#7a8278"], seq=["#f0f5ec", "#d3e6cc", "#a8cf9d", "#76b06d", "#478c45", "#235c2a"]),
        dark=dict(neutral="ember-dark", accent="#7dcb8b", soft="#1e3524", up="#ff8a7a", down="#7fb2f0", bar="#98a294",
                  cats=["#7dcb8b", "#ea9d5b", "#82a8e8", "#e389b6", "#d8c35a", "#aab3a6"], seq=["#16211a", "#1d3524", "#25502f", "#2f6c3b", "#3f8c4b", "#73c07e"])),
    "dusk": dict(fits="Space, night, sleep, technology, science, culture: reflective or abstract subjects.",
        light=dict(neutral="mist", accent="#4b3fa0", soft="#e5e2f5", up="#b3261e", down="#1d6b4f", bar="#80849a",
                   cats=["#4b3fa0", "#c26c17", "#17817f", "#b33a6a", "#5e7d2a", "#7f8396"], seq=["#f2f0fa", "#ddd8f1", "#bdb3e3", "#9688cf", "#6e5db5", "#43358a"]),
        dark=dict(neutral="night", accent="#a99cff", soft="#28244d", up="#ff8a7a", down="#5fcf9f", bar="#9599ae",
                  cats=["#a99cff", "#f0a64e", "#56cfcf", "#f07aa8", "#a7c95d", "#acb0c2"], seq=["#19172a", "#25204a", "#352c6c", "#48398f", "#6550b5", "#a090f2"])),
    "coral": dict(fits="Oceans, beaches, tourism, summer, people and places: bright, outward subjects.",
        light=dict(neutral="white", accent="#c2413a", soft="#f8e1df", up="#b3261e", down="#1d6b4f", bar="#84888c",
                   cats=["#c2413a", "#157a83", "#6a5aa8", "#a87412", "#2f7d3c", "#83878b"], seq=["#eef7f7", "#cce8e9", "#98cfd2", "#5fb0b5", "#2d8a91", "#125e65"]),
        dark=dict(neutral="carbon", accent="#ff7c70", soft="#3d1f1c", up="#ff8a7a", down="#5fcf9f", bar="#9a9ea2",
                  cats=["#ff7c70", "#4cc4cc", "#aa9bf0", "#e6b84b", "#71c880", "#b2b6ba"], seq=["#102020", "#123534", "#164c4c", "#1d6767", "#2a898a", "#5ec2c3"])),
    "ink": dict(fits="Politics, elections, courts, news, history: serious subjects that want a newspaper's restraint.",
        light=dict(neutral="white", accent="#c8102e", soft="#f9dfe3", up="#c8102e", down="#1f5f99", bar="#6b6b6b",
                   cats=["#c8102e", "#262626", "#1f5f99", "#a36a00", "#6b6b6b", "#2f7d4f"], seq=["#f4f4f4", "#d9d9d9", "#b3b3b3", "#808080", "#4d4d4d", "#1a1a1a"]),
        dark=dict(neutral="carbon", accent="#ff5266", soft="#3d1419", up="#ff5266", down="#6aaae8", bar="#8f8f8f",
                  cats=["#ff5266", "#e8e8e8", "#6aaae8", "#e3a43f", "#9c9c9c", "#6fc792"], seq=["#1c1c1c", "#333333", "#4d4d4d", "#737373", "#a6a6a6", "#e6e6e6"])),
    "desert": dict(fits="Drought, mining, the West, archaeology, deserts and dry land: earthy, sun-baked subjects.",
        light=dict(neutral="sand", accent="#8c5a00", soft="#f0dfbe", up="#a8341e", down="#2c6a4a", bar="#857a6b",
                   cats=["#8c5a00", "#4e7a5c", "#3e6fa3", "#a8432a", "#6d4c8a", "#82786b"], seq=["#f6ecdc", "#ead2ad", "#d9b07a", "#c08a4a", "#94612a", "#5e3a14"]),
        dark=dict(neutral="ember-dark", accent="#e7b24a", soft="#3a2c14", up="#ff8a6e", down="#6fcf9a", bar="#a59a8a",
                  cats=["#e7b24a", "#8cc4a0", "#80b1e8", "#f08b6b", "#b59be2", "#b8ae9f"], seq=["#231d15", "#3a2f1d", "#594425", "#7d5d2e", "#a87d3b", "#e3b460"])),
    "orchard": dict(fits="Wine, fruit, fashion, arts, retail, consumer life: rich, cultivated subjects.",
        light=dict(neutral="paper", accent="#8e2f6b", soft="#f2dfea", up="#b3261e", down="#1d6b4f", bar="#8a8590",
                   cats=["#8e2f6b", "#2c7a6e", "#c0701a", "#3f5fa0", "#6a8a2a", "#86818c"], seq=["#f8eef4", "#efd2e3", "#dfa8c8", "#c779a8", "#a14d86", "#6d2659"]),
        dark=dict(neutral="ember-dark", accent="#f07dc2", soft="#3b1d31", up="#ff8a7a", down="#5fcf9f", bar="#a39eaa",
                  cats=["#f07dc2", "#5fcfbf", "#f0a55a", "#8aa8ec", "#b9d16a", "#b6b1bb"], seq=["#22161e", "#3d1f33", "#5b2a4b", "#7d3866", "#a74e8a", "#e68cc5"])),
    "lagoon": dict(fits="Climate, coasts, public health, ecology, energy transition: hopeful, green-blue subjects.",
        light=dict(neutral="mist", accent="#0d7a6f", soft="#d8eeeb", up="#b3261e", down="#285f9e", bar="#7f8b8a",
                   cats=["#0d7a6f", "#cf5530", "#3d5ca8", "#a4740d", "#8a3f86", "#7d8988"], seq=["#edf7f5", "#c9e8e2", "#93cfc4", "#59ae9f", "#2a8a7b", "#0f5c51"]),
        dark=dict(neutral="night", accent="#52d1be", soft="#143630", up="#ff8a7a", down="#7fb2f0", bar="#95a3a2",
                  cats=["#52d1be", "#ff8b63", "#8aa6f0", "#e6bf50", "#da8bd5", "#aab6b5"], seq=["#0f1d1b", "#12322e", "#164a43", "#1c665c", "#2a8a7b", "#62c9b6"])),
    "graphite": dict(fits="Technology, finance, markets, infrastructure, data about data: precise, modern subjects.",
        light=dict(neutral="mist", accent="#2457d6", soft="#dfe7fa", up="#b3261e", down="#1d6b4f", bar="#6e7781",
                   cats=["#2457d6", "#d24a12", "#0e8a5d", "#9b3fbf", "#a98000", "#6c757f"], seq=["#eef2fc", "#d0dcf7", "#a3baef", "#6f92e2", "#3e68cf", "#1f3f99"]),
        dark=dict(neutral="night", accent="#7da5ff", soft="#1a2b55", up="#ff8a7a", down="#5fcf9f", bar="#8f98a3",
                  cats=["#7da5ff", "#ff905c", "#47d59c", "#d48ef3", "#f0c44c", "#a3abb4"], seq=["#121a2e", "#17284d", "#1e3a72", "#2a5099", "#3f6ccc", "#89abff"])),
}


def tokens(mode):
    n = NEUTRALS[mode["neutral"]]
    out = dict(n)
    out.update({"accent": mode["accent"], "accent-soft": mode["soft"], "up": mode["up"], "down": mode["down"], "c-bar": mode["bar"]})
    out.update({f"c{i + 1}": c for i, c in enumerate(mode["cats"])})
    out.update({f"seq-{i}": c for i, c in enumerate(mode["seq"])})
    return out


def block(selector, toks, scheme):
    body = "\n".join(f"  --{k}: {v};" for k, v in toks.items())
    return f"{selector} {{\n  color-scheme: {scheme};\n{body}\n}}\n"


def main(out):
    palette = str(Path(__file__).with_name("palette.py"))
    css = ["/* Themes for the data app: set data-theme on <html>; data-mode=\"dark\" or \"light\" fixes the mode, and with",
           "   neither the reader's system setting decides. Generated by themes.py: edit there, not here. */\n"]
    failed = 0
    for name, th in THEMES.items():
        light, dark = tokens(th["light"]), tokens(th["dark"])
        css.append(f"/* {name}: {th['fits']} */")
        css.append(block(f':root[data-theme="{name}"]', light, "light"))
        css.append(block(f':root[data-theme="{name}"][data-mode="dark"]', dark, "dark"))
        css.append("@media (prefers-color-scheme: dark) {\n" + block(f'  :root[data-theme="{name}"]:not([data-mode="light"])', dark, "dark").replace("\n  --", "\n    --") + "}\n")
    Path(out).write_text("\n".join(css))
    # Check the file as written: each theme's blocks, read the way an app reads them.
    for name in THEMES:
        for mode in ("light", "dark"):
            r = subprocess.run([sys.executable, "-I", palette, out, "--theme", name, "--mode", mode], capture_output=True, text=True)
            lines = [l for l in r.stdout.splitlines() if not l.startswith("note:")]
            print(f"{name:9} {mode:5} {lines[-1] if lines else '?'}")
            for l in lines[:-1]:
                print("    " + l)
            failed += r.returncode != 0
    print(f"wrote {out}; {failed} theme-mode(s) failing")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "themes.css"))
