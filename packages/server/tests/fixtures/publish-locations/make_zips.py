# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Write the zip fixtures in zips/ from the package folders beside this file.

Each archive holds the package's files at its root (publisher.json,
model.malloy, models/...), the way a control plane packs a package. Entries
are sorted and carry a fixed timestamp, so a rebuild writes the same bytes.
sales-1.0.0-repacked.zip holds the same files as sales-1.0.0.zip with
another timestamp: the archives differ, their contents do not.

Run from anywhere: python3 make_zips.py
"""

import os
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "zips")

FIRST = (2026, 1, 1, 0, 0, 0)
REPACKED = (2026, 2, 2, 12, 30, 0)

ARCHIVES = [
    ("sales-1.0.0", "sales-1.0.0.zip", FIRST),
    ("sales-1.0.0", "sales-1.0.0-repacked.zip", REPACKED),
    ("sales-1.0.0-changed", "sales-1.0.0-changed.zip", FIRST),
    ("sales-unversioned", "sales-unversioned.zip", FIRST),
    ("sales-unversioned-changed", "sales-unversioned-changed.zip", FIRST),
]


def files_under(root):
    out = []
    for dirpath, _dirs, names in os.walk(root):
        for name in names:
            absolute = os.path.join(dirpath, name)
            out.append(os.path.relpath(absolute, root).replace(os.sep, "/"))
    return sorted(out)


def main():
    os.makedirs(OUT, exist_ok=True)
    for folder, archive, stamp in ARCHIVES:
        source = os.path.join(HERE, folder)
        with zipfile.ZipFile(os.path.join(OUT, archive), "w") as zf:
            for relative in files_under(source):
                info = zipfile.ZipInfo(relative, date_time=stamp)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                with open(os.path.join(source, relative), "rb") as f:
                    zf.writestr(info, f.read())
        print(f"wrote zips/{archive}")


if __name__ == "__main__":
    main()
