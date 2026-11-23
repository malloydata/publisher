# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Write public/fonts.css: pinned, checksummed open-licence fonts as base64 @font-face rules.

Usage: python3 -I fonts.py <public dir> <face> [<face> ...]
Faces: figtree, source-serif-4, inter, fraunces. Standard library only.
"""
import base64
import hashlib
import os
import sys
import urllib.request

CDN = "https://cdn.jsdelivr.net/npm/@fontsource-variable/"
FACES = {
    "figtree": {
        "family": "Figtree Variable",
        "license": ("figtree@5.3.0/LICENSE", "ee23e6c84000126692e112ff067b456470493349f993ceaa8b4d3766dd6fad5d"),
        "files": [("figtree@5.3.0/files/figtree-latin-wght-normal.woff2", "4ba7d3d096695818fe0686be4f1e82c6b05134e18a22260336130335027462dd", "normal", "300 900")],
    },
    "source-serif-4": {
        "family": "Source Serif 4 Variable",
        "license": ("source-serif-4@5.3.1/LICENSE", "18aabf190848725e2576eefb5c29ba06aac1029d02132252a7f312eac2e50cf3"),
        "files": [
            ("source-serif-4@5.3.1/files/source-serif-4-latin-wght-normal.woff2", "382475f38242c52d42a3e5ff8dc69b5d73f10d0d21e4550b5fe232984688779a", "normal", "200 900"),
            ("source-serif-4@5.3.1/files/source-serif-4-latin-wght-italic.woff2", "b4f56ddc9873487f1c9a3ad5bbce31982fe89b951f57e4604f62b4c8a540fb19", "italic", "200 900"),
        ],
    },
    "inter": {
        "family": "Inter Variable",
        "license": ("inter@5.3.0/LICENSE", "3b0a5fca3d17942cde889069889dedbbbd075e9b599968c82a95f4d944e9b345"),
        "files": [("inter@5.3.0/files/inter-latin-wght-normal.woff2", "3100e775e8616cd2611beecfa23a4263d7037586789b43f035236a2e6fbd4c62", "normal", "100 900")],
    },
    "fraunces": {
        "family": "Fraunces Variable",
        "license": ("fraunces@5.3.0/LICENSE", "cd3384cafac6f2bddc3955273958a2e029f97027c8037ac539ef5744a77b579e"),
        "files": [("fraunces@5.3.0/files/fraunces-latin-wght-normal.woff2", "7f9d191d999336d3b9790afa72e1358e50a13b06d4f289341e92a311967a80f9", "normal", "100 900")],
    },
}


def fetch(path, sha):
    with urllib.request.urlopen(CDN + path, timeout=60) as r:
        data = r.read()
    got = hashlib.sha256(data).hexdigest()
    if got != sha:
        sys.exit(f"{path}: sha256 {got} is not the pinned {sha}; nothing written")
    return data


def main(public, names):
    unknown = [n for n in names if n not in FACES]
    if unknown:
        sys.exit(f"unknown face {unknown}; choose from {sorted(FACES)}")
    rules, licenses = [], {}
    for name in names:
        face = FACES[name]
        for path, sha, style, weight in face["files"]:
            b64 = base64.b64encode(fetch(path, sha)).decode()
            rules.append(
                f'@font-face {{ font-family: "{face["family"]}"; font-style: {style}; font-weight: {weight}; '
                f'font-display: swap; src: url(data:font/woff2;base64,{b64}) format("woff2"); }}'
            )
        lic_path, lic_sha = face["license"]
        licenses[name] = fetch(lic_path, lic_sha)
    head = "/* Open-licence fonts (SIL OFL 1.1), from Fontsource on jsdelivr, pinned and checked by sha256.\n"
    head += "   Licenses: vendor/fonts/. Faces: " + ", ".join(FACES[n]["family"] for n in names) + ". */\n"
    os.makedirs(f"{public}/vendor/fonts", exist_ok=True)
    with open(f"{public}/fonts.css", "w") as f:
        f.write(head + "\n".join(rules) + "\n")
    for name, text in licenses.items():
        with open(f"{public}/vendor/fonts/{name}-OFL.txt", "wb") as f:
            f.write(text)
    print(f"wrote {public}/fonts.css ({os.path.getsize(public + '/fonts.css') // 1024} KB): {', '.join(names)}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
