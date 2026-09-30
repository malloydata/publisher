# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Run every scenario in turn, writing each one's output to $VAL_OUT/<name>.txt.

Usage: VAL_OUT=/tmp/llm-val/results-real python3 run_all.py [name ...]
"""

import os
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
OUT = pathlib.Path(os.environ.get("VAL_OUT", "/tmp/llm-val/results"))
OUT.mkdir(parents=True, exist_ok=True)
ALL = ["core", "scoring", "settings", "enrich", "values", "privacy", "failures", "prefix", "parity", "storefront"]

for name in sys.argv[1:] or ALL:
    t0 = time.time()
    with open(OUT / f"{name}.txt", "w") as fh:
        code = subprocess.run([sys.executable, str(HERE / f"scenario_{name}.py")], stdout=fh,
                              stderr=subprocess.STDOUT, env={**os.environ, "VAL_OUT": str(OUT)}).returncode
    print(f"done {name}: exit {code} in {time.time() - t0:.0f}s", flush=True)
print("ALLDONE", flush=True)
