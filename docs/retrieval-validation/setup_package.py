#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Build the scratch package the validation runs against.

Copies the ecommerce sample, tags four dimensions `#(index)` so their values
can be searched, and adds two gated sources: one behind `#(authorize)` and one
behind `#(access_filter)`. Their gate text is chosen to be easy to grep for in
captured provider traffic (`analyst`, `ROLE`, `COUNTRY`, `secret_tenant`).

Usage: setup_package.py <ecommerce-sample-dir> <out-dir>
"""

import pathlib
import re
import shutil
import sys

src, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
if out.exists():
    shutil.rmtree(out)
shutil.copytree(src, out)

model = out / "ecommerce.malloy"
text = model.read_text()
for field in ("traffic_source", "brand", "category", "gender"):
    pat = re.compile(rf"^(\s*)(public: {field})$", re.M)
    assert pat.search(text), field
    text = pat.sub(rf"\1#(index)\n\1\2", text, count=1)
model.write_text(text)

(out / "restricted.malloy").write_text('''##! experimental.givens
##! experimental.access_modifiers

given:
  ROLE :: string
  COUNTRY :: string

#(doc) Restricted customer accounts, for the analyst role only.
#(authorize) 'analyst' = $ROLE
source: restricted_users is duckdb.table('data/users.parquet') extend {
  dimension:
    #(index)
    #(doc) Restricted acquisition channel
    restricted_channel is traffic_source
}

#(doc) Customer accounts scoped to one country per caller.
#(access_filter) country = $COUNTRY
source: country_users is duckdb.table('data/users.parquet') extend {
  dimension:
    #(index)
    #(doc) Scoped acquisition channel
    scoped_channel is traffic_source
}
''')
print(f"wrote {out}")
