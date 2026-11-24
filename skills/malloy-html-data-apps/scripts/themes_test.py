# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""The kit's generated files match their generator, with no network: themes.css is what themes.py writes, and every
inlined font and its licence is the file whose sha256 themes.py pins."""
import base64
import hashlib
import io
import os
import re
import unittest
from contextlib import redirect_stdout

import themes

KIT = themes.KIT


def sha(data):
    return hashlib.sha256(data).hexdigest()


def inlined(css_path):
    """Every base64 font in a stylesheet, decoded."""
    text = open(css_path).read()
    return [base64.b64decode(b) for b in re.findall(r"base64,([A-Za-z0-9+/=]+)", text)]


class ThemesTest(unittest.TestCase):
    def test_themes_css_is_what_the_script_writes(self):
        with redirect_stdout(io.StringIO()):
            expected = themes.themes_css()
        with open(os.path.join(KIT, "themes.css")) as f:
            self.assertEqual(f.read(), expected, "kit/themes.css is stale: run python3 -I scripts/themes.py")

    def test_every_display_face_is_the_pinned_file(self):
        for face, (_, _, _, _, woff_sha, lic_sha) in themes.FACES.items():
            with self.subTest(face=face):
                self.assertEqual([sha(d) for d in inlined(os.path.join(KIT, "fonts", f"{face}.css"))], [woff_sha])
                with open(os.path.join(KIT, "vendor", "fonts", f"LICENSE-{face}.txt"), "rb") as f:
                    self.assertEqual(sha(f.read()), lic_sha)

    def test_the_interface_faces_are_the_pinned_files(self):
        pins = [v[2] for v in themes.SANS.values()]
        self.assertEqual([sha(d) for d in inlined(os.path.join(KIT, "fonts.css"))], pins)
        for face, (_, _, _, lic_sha) in themes.SANS.items():
            with self.subTest(face=face), open(os.path.join(KIT, "vendor", "fonts", f"LICENSE-{face}.txt"), "rb") as f:
                self.assertEqual(sha(f.read()), lic_sha)


if __name__ == "__main__":
    unittest.main()
