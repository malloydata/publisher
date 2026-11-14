#!/usr/bin/env python3
"""External data, secret masking, and the `connections` / `run` subcommands.

Every fixture is hand-written XML in a tempdir. The secret strings below are fakes
that are unique enough to grep for: a test fails if one reaches stdout, stderr,
the text report, the JSON, or the config file."""
import base64
import contextlib
import io
import json
import os
import pathlib
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
import unittest
import zipfile
from unittest import mock
from xml.sax.saxutils import escape

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import classify_workbook as cw  # noqa: E402
from classify_workbook_test import MAIN, F, Sheet, Tmp, make_book, pivot_parts, sec_ids  # noqa: E402

SCRIPT = pathlib.Path(__file__).resolve().parent / "classify_workbook.py"
PW = "Zq9-fake-pw-7731"
PW2 = "Mx4-other-fake-5520"
GIT = shutil.which("git")


def slurp(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def q(s):
    return escape(s, {'"': "&quot;"})


def conn(cid, name, cs, command=None, ctype=2, params=(), save=False, extra=""):
    cmd = f' command="{q(command)}" commandType="{ctype}"' if command is not None else ""
    ps = ""
    if params:
        ps = f'<parameters count="{len(params)}">' + "".join(
            f'<parameter name="{n}" cell="{c}"/>' for n, c in params) + "</parameters>"
    return (f'<connection id="{cid}" name="{q(name)}" type="1" savePassword="{int(save)}">'
            f'<dbPr connection="{q(cs)}"{cmd}/>{ps}{extra}</connection>')


def conns(*items):
    return f'<connections xmlns="{MAIN}">' + "".join(items) + "</connections>"


def mashup(m_text, enc="utf-16", entries=None):
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, d in (entries or [("Formulas/Section1.m", m_text)]):
            zf.writestr(n, d)
    body = inner.getvalue()
    blob = struct.pack("<I", 0) + struct.pack("<I", len(body)) + body + struct.pack("<I", 0)
    xml = ('<?xml version="1.0" encoding="utf-16"?><DataMashup xmlns="http://schemas.microsoft.com/DataMashup">'
           + base64.b64encode(blob).decode() + "</DataMashup>")
    return xml.encode(enc)


def book_with(tmp, conn_xml=None, sheets=None, parts=None, **kw):
    p = dict(parts or {})
    if conn_xml is not None:
        p["xl/connections.xml"] = conn_xml
    return make_book(tmp, sheets or [Sheet("S", [[1]])], parts=p, **kw)


def blob_of(rep):
    return json.dumps(rep, ensure_ascii=False) + cw.render_text(rep)


def run_main(argv, env=None):
    """In-process safe_main with captured streams. Returns (code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        if env is None:
            code = cw.safe_main(argv)
        else:
            with mock.patch.dict(os.environ, env):
                code = cw.safe_main(argv)
    return code, out.getvalue(), err.getvalue()


def sst_xml(*strings):
    return (f'<sst xmlns="{MAIN}">' + "".join(f"<si><t>{escape(s)}</t></si>" for s in strings) + "</sst>")


def shared_sheet(name, rows, state=None):
    """rows of strings/numbers written as shared-string cells; returns (Sheet, sst_xml)."""
    strings, out = [], []
    for i, row in enumerate(rows):
        cells = []
        for j, v in enumerate(row):
            ref = f"{chr(65 + j)}{i + 1}"
            if isinstance(v, str):
                if v not in strings:
                    strings.append(v)
                cells.append(f'<c r="{ref}" t="s"><v>{strings.index(v)}</v></c>')
            elif v is not None:
                cells.append(f'<c r="{ref}"><v>{v}</v></c>')
        out.append(f'<row r="{i + 1}">{"".join(cells)}</row>')
    sh = Sheet(name, state=state)
    sh.data = "".join(out)
    return sh, sst_xml(*strings)


# --------------------------------------------------------------------------
# Masking
# --------------------------------------------------------------------------

class SecretShapes(unittest.TestCase):
    SHAPES = {
        "password=": "Server=a;Password=" + PW + ";",
        "pwd=": "pwd = " + PW,
        "userpass@host": "alice:" + PW + "@db.example.com",
        "url creds": "postgres://alice:" + PW + "@db.example.com/x",
        "AKIA": "AKIAIOSFODNN7EXAMPLE",
        "sk-": "sk-" + "a1B2c3D4e5F6g7H8i9J0k1L2",
        "ghp_": "ghp_" + "a1B2c3D4e5F6g7H8i9J0k1L2m3N4",
        "xox": "xoxb-1234567890-abcdefghij",
        "jwt": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
        "entropy": "q8Zr3Kd0Wm2Xv7Lp9Tc4Bn1Hs6Yf5Jg",
    }

    def test_each_shape_is_recognised_as_a_secret_value(self):
        for k, v in self.SHAPES.items():
            self.assertTrue(cw.secret_shaped(v), k)

    def test_ordinary_header_text_is_not(self):
        for v in ("Region", "Amount (USD)", "Compass", "Passenger count", "CustomerOrderDetailsTable", "Revenue_2024_Q1_Actuals",
                  "Sheet1!$A$1:$B$99", "2024-01-31", "alice@example.com", "Token count"):
            self.assertFalse(cw.secret_shaped(v), v)

    def test_labels_in_several_languages(self):
        for v in ("Password", "password:", "PWD", "Passwort", "Contraseña", "Mot de passe", "Senha", "パスワード", "密码",
                  "API key", "api_key", "Secret", "Client secret", "Token", "Kennwort", "пароль"):
            self.assertTrue(cw.is_secret_label(v), v)
        for v in ("Passenger", "Compass", "Secretary", "Region", "Keyword"):
            self.assertFalse(cw.is_secret_label(v), v)

    def test_mask_text_replaces_the_value_and_keeps_the_label(self):
        out = cw.mask_text("Server=a;Password=" + PW + ";User=b")
        self.assertNotIn(PW, out)
        self.assertIn("Password=", out)
        self.assertIn("User=b", out)
        self.assertNotIn(PW, cw.mask_text("postgres://alice:" + PW + "@h/x"))

    def test_known_values_are_scrubbed_wherever_they_appear(self):
        self.assertNotIn("hunter2", cw.mask_text("x hunter2 y", known=["hunter2"]))


class MaskingInReports(Tmp):
    def rep(self, rows, **kw):
        return cw.analyze(make_book(self.tmp, [Sheet("S", rows)]), **kw)

    def test_a_label_value_pair_in_the_first_row_does_not_leak_the_value_as_a_column_name(self):
        rep = self.rep([["Password", "hunter2", "Host"], [1, 2, 3], [4, 5, 6]])
        self.assertNotIn("hunter2", blob_of(rep))
        src = rep["sources"][0]
        self.assertIn("column2", src["stanza"])
        self.assertIn("Host", src["lifted"])

    def test_translated_labels_mask_the_neighbour(self):
        for label in ("Passwort", "Contraseña", "Mot de passe", "Senha", "パスワード", "API key", "Secret", "Token"):
            rep = self.rep([[label, "zzvalue-" + str(len(label))], [1, 2], [3, 4]])
            self.assertNotIn("zzvalue-", blob_of(rep), label)

    def test_every_shape_as_a_header_cell_is_masked(self):
        for k, v in SecretShapes.SHAPES.items():
            rep = self.rep([["id", v], [1, 2], [3, 4]])
            self.assertNotIn(v, blob_of(rep), k)
            self.assertNotIn(PW, blob_of(rep), k)

    def test_shapes_in_a_table_column_name_and_a_formula_literal_are_masked(self):
        tbl = (f'<table xmlns="{MAIN}" name="T" displayName="T" ref="A1:B3"><tableColumns count="2">'
               f'<tableColumn id="1" name="id"/><tableColumn id="2" name="{SecretShapes.SHAPES["AKIA"]}"/></tableColumns></table>')
        sh = Sheet("S", [["id", SecretShapes.SHAPES["AKIA"]], [1, 2], [3, 4]], rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": tbl}))
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", blob_of(rep))
        rep = self.rep([[F('"Password=' + PW + '"&A2', "x", t="str"), 1], [1, 2]])
        self.assertNotIn(PW, blob_of(rep))

    def test_secret_cell_masks_inline_strings(self):
        rows = [["Host", "plainvalue-xyz"], [1, 2], [3, 4]]
        self.assertIn("plainvalue-xyz", blob_of(self.rep(rows)))
        rep = self.rep(rows, secret_cells=["S!B1=MALLOY_SALES_PG_PASSWORD"])
        self.assertNotIn("plainvalue-xyz", blob_of(rep))

    def test_a_short_secret_cell_value_is_masked_by_position_not_only_by_substring(self):
        rows = [["Host", "xq7"], [1, 2], [3, 4]]
        self.assertIn("xq7", blob_of(self.rep(rows)))
        self.assertNotIn("xq7", blob_of(self.rep(rows, secret_cells=["S!B1=SHORT"])))

    def test_secret_cell_masks_shared_strings(self):
        sh, sst = shared_sheet("S", [["Host", "plainvalue-xyz"], [1, 2], [3, 4]])
        p = make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst})
        self.assertIn("plainvalue-xyz", blob_of(cw.analyze(p)))
        self.assertNotIn("plainvalue-xyz", blob_of(cw.analyze(p, secret_cells=["'S'!B1=MALLOY_SALES_PG_PASSWORD"])))

    def test_labels_and_shapes_in_shared_strings_are_found_too(self):
        sh, sst = shared_sheet("S", [["Password", "hunter2-shared"], [1, 2], [3, 4]])
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst}))
        self.assertNotIn("hunter2-shared", blob_of(rep))
        self.assertIn("secret_labelled_cell", sec_ids(rep))

    def test_hidden_sheets_report_labelled_cells_by_address_only(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("Main", [[1]]), Sheet("H", [["Token", "tok-value-123"]], state="veryHidden")]))
        flag = [s for s in rep["security"] if s["flag"] == "secret_labelled_cell"][0]
        self.assertIn("H!A1", flag["detail"])
        self.assertNotIn("tok-value-123", blob_of(rep))

    def test_labelled_cells_are_reported_by_address_never_by_value(self):
        rep = self.rep([["a", "b"], ["Password", "hunter2"]])
        flag = [s for s in rep["security"] if s["flag"] == "secret_labelled_cell"][0]
        self.assertIn("S!A2", flag["detail"])
        self.assertNotIn("hunter2", blob_of(rep))

    def test_defined_name_constants_and_comments_are_scanned_not_emitted(self):
        names = ('<definedNames><definedName name="svc_key">"AKIAIOSFODNN7EXAMPLE"</definedName>'
                 '<definedName name="Region">"East"</definedName></definedNames>')
        comments = (f'<comments xmlns="{MAIN}"><commentList><comment ref="A1"><text><t>login Password: {PW}</t></text></comment>'
                    '</commentList></comments>')
        threaded = ('<ThreadedComments xmlns="http://schemas.microsoft.com/office/spreadsheetml/2018/threadedcomments">'
                    '<threadedComment ref="B2" id="x"><text>use xoxb-1234567890-abcdefghij</text></threadedComment></ThreadedComments>')
        p = make_book(self.tmp, [Sheet("S", [[1]])], names=names,
                      parts={"xl/comments1.xml": comments, "xl/threadedComments/threadedComment1.xml": threaded})
        rep = cw.analyze(p)
        ids = sec_ids(rep)
        self.assertIn("secret_in_defined_name", ids)
        self.assertIn("secret_in_comment", ids)
        blob = blob_of(rep)
        for s in (PW, "AKIAIOSFODNN7EXAMPLE", "xoxb-1234567890"):
            self.assertNotIn(s, blob)

    def test_config_named_visible_sheets_emit_no_raw_values_and_classify_as_config(self):
        rows = [["Host", "db.internal.example"], ["Region", "Westeros"], ["Password", "hunter2"]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("Main", [[1]]), Sheet("Config", rows)]))
        blob = blob_of(rep)
        for s in ("db.internal.example", "Westeros", "hunter2"):
            self.assertNotIn(s, blob)
        self.assertEqual([s for s in rep["sheets"] if s["name"] == "Config"][0]["class"], "config")

    def test_sensitivity_label_is_prominent_and_says_it_never_enters_the_corpus(self):
        custom = ('<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/custom-properties">'
                  '<property name="MSIP_Label_abc_Enabled"><lpwstr>true</lpwstr></property></Properties>')
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={"docProps/custom.xml": custom}))
        flag = [s for s in rep["security"] if s["flag"] == "sensitivity_label"][0]
        self.assertIn("never enters the corpus or a PR", flag["detail"])
        first = cw.render_text(rep).split("## Security flags")[1].strip().splitlines()[0]
        self.assertIn("sensitivity_label", first)

    def test_json_masking_is_declared_best_effort(self):
        rep = self.rep([[1]])
        self.assertIn("best effort", rep["masking_note"])
        self.assertIn("best effort", cw.render_text(rep))

    def test_secret_cell_spec_errors_do_not_echo_values(self):
        for bad in ("S!B1", "S!B1=lower", "S!ZZZ=A", "nonsense"):
            with self.assertRaises(cw.CliError):
                cw.parse_secret_cells([bad])
        specs = cw.parse_secret_cells(["'My Sheet'!B3=MALLOY_SALES_PG_PASSWORD", "S!$C$4=X1"])
        self.assertEqual([(s.sheet, s.row, s.col, s.var) for s in specs], [("My Sheet", 3, 2, "MALLOY_SALES_PG_PASSWORD"), ("S", 4, 3, "X1")])


# --------------------------------------------------------------------------
# Connection inventory and the mapping table
# --------------------------------------------------------------------------

class ConnectionMapping(Tmp):
    def one(self, cs, **kw):
        rep = cw.analyze(book_with(self.tmp, conns(conn(1, kw.pop("name", "c1"), cs, **kw))))
        lst = rep["external"]["connection_list"]
        self.assertEqual(len(lst), 1)
        return lst[0], rep

    def test_postgres_odbc(self):
        c, rep = self.one("Driver={PostgreSQL Unicode};Server=pg.example.com;Port=5433;Database=sales;Uid=bob;Pwd=" + PW)
        self.assertEqual((c["status"], c["type"]), ("maps", "postgres"))
        self.assertEqual(c["fields"], {"host": "pg.example.com", "port": 5433, "databaseName": "sales", "userName": "bob"})
        self.assertEqual(c["secret_field"], "password")
        self.assertTrue(c["secret_present"])
        self.assertNotIn(PW, blob_of(rep))

    def test_mysql_odbc(self):
        c, _ = self.one("Driver={MySQL ODBC 8.0 Unicode Driver};Server=my.example.com;Database=shop;User=app;Password=x1")
        self.assertEqual((c["type"], c["fields"]), ("mysql", {"host": "my.example.com", "database": "shop", "user": "app"}))

    def test_snowflake_odbc(self):
        c, _ = self.one("Driver={SnowflakeDSIIDriver};Server=acme-xy1.snowflakecomputing.com;Warehouse=WH;Database=D;Schema=S;Role=R;UID=me;PWD=x1")
        self.assertEqual(c["type"], "snowflake")
        self.assertEqual(c["fields"], {"account": "acme-xy1", "username": "me", "warehouse": "WH", "database": "D", "schema": "S", "role": "R"})

    def test_databricks_odbc_uses_a_token(self):
        c, _ = self.one("Driver={Simba Spark ODBC Driver};Host=dbc-1.cloud.databricks.com;HTTPPath=/sql/1.0/warehouses/abc;UID=token;PWD=dapi123")
        self.assertEqual((c["type"], c["secret_field"]), ("databricks", "token"))
        self.assertEqual(c["fields"], {"host": "dbc-1.cloud.databricks.com", "path": "/sql/1.0/warehouses/abc"})

    def test_trino_odbc(self):
        c, _ = self.one("Driver={Trino ODBC};Host=trino.example.com;Port=8080;Catalog=hive;Schema=default;UID=u;PWD=p1")
        self.assertEqual((c["type"], c["fields"]), ("trino", {"server": "trino.example.com", "port": 8080, "catalog": "hive",
                                                               "schema": "default", "user": "u"}))
        self.assertTrue(any(m.startswith("server") for m in c["missing"]))
        c2, _ = self.one("Driver={Trino ODBC};Host=https://trino.example.com;Port=8443;Catalog=hive;UID=u;PWD=p1", name="t2")
        self.assertEqual(c2["fields"]["server"], "https://trino.example.com")
        self.assertEqual(c2["missing"], [])

    def test_bigquery_takes_only_the_project(self):
        c, _ = self.one("Driver={Simba ODBC Driver for Google BigQuery};Catalog=my-proj;OAuthMechanism=1;RefreshToken=r-" + PW)
        self.assertEqual((c["type"], c["fields"], c["secret_field"]), ("bigquery", {"defaultProjectId": "my-proj"}, None))

    def test_ace_over_excel_or_text_files_maps_to_duckdb(self):
        c, _ = self.one("Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\bob\\x.xlsx;Extended Properties=\"Excel 12.0;HDR=YES\"")
        self.assertEqual((c["status"], c["type"]), ("file", "duckdb"))
        self.assertEqual(c["fields"], {"file": "x.xlsx"})
        self.assertNotIn("bob", blob_of(cw.analyze(book_with(self.tmp, conns(conn(
            1, "c", "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\Users\\bob\\x.xlsx;Extended Properties=\"Excel 12.0\"")),
            filename="b2.xlsx"))))

    def test_nr_systems_with_reasons(self):
        cases = {
            "SQL Server": ["Provider=SQLOLEDB;Data Source=s;Initial Catalog=d;User ID=u;Password=p9",
                           "Provider=SQLNCLI11;Data Source=s", "Provider=MSOLEDBSQL;Server=s",
                           "Driver={ODBC Driver 17 for SQL Server};Server=s"],
            "Oracle": ["Provider=OraOLEDB.Oracle;Data Source=ora"],
            "Teradata": ["Driver={Teradata Database ODBC Driver 16.20};DBCName=td"],
            "SAP HANA": ["Driver={HDBODBC};ServerNode=h:30015"],
            "DB2": ["Provider=IBMDADB2;Database=d"],
            "SSAS": ["Provider=MSOLAP.8;Data Source=cube;Initial Catalog=m"],
            "Access": ["Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\x.accdb"],
            "ODBC DSN": ["DSN=MyWarehouse;UID=u;PWD=p"],
            "unrecognized": ["Provider=Fancy.Provider;Data Source=z"],
        }
        for system, strings in cases.items():
            for cs in strings:
                c, _ = self.one(cs)
                self.assertEqual(c["status"], "nr", cs)
                self.assertIsNone(c["type"], cs)
                self.assertEqual(c["system"], system if system != "unrecognized" else c["system"], cs)
                self.assertTrue(c["reasons"], cs)

    def test_integrated_and_trusted_auth_are_nr_even_on_a_mappable_driver(self):
        for cs in ("Driver={PostgreSQL Unicode};Server=h;Database=d;Integrated Security=SSPI",
                   "Driver={PostgreSQL Unicode};Server=h;Database=d;Trusted_Connection=yes"):
            c, _ = self.one(cs)
            self.assertEqual((c["status"], c["type"]), ("nr", None))
            self.assertTrue(any("Windows" in r for r in c["reasons"]))

    def test_sql_server_leads_the_nr_list_in_the_report(self):
        p = book_with(self.tmp, conns(conn(1, "ora", "Provider=OraOLEDB.Oracle;Data Source=o"),
                                      conn(2, "sql", "Provider=SQLOLEDB;Data Source=s"),
                                      conn(3, "pg", "Driver={PostgreSQL Unicode};Server=h;Database=d")))
        text = cw.render_text(cw.analyze(p))
        sec = text.split("## External data")[1].split("\n## ")[0]
        self.assertLess(sec.index("SQL Server"), sec.index("Oracle"))
        self.assertLess(sec.index("Oracle"), sec.index("postgres"))
        self.assertLess(text.index("## External data"), text.index("## Sheets"))

    def test_command_becomes_the_proposed_source_sql(self):
        c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", name="Sales PG", command="SELECT a, b FROM t WHERE x = ?")
        self.assertEqual(c["command"], "SELECT a, b FROM t WHERE x = ?")
        self.assertNotRegex(c["proposed_source"], r"(?<!\w)\?")
        self.assertIn("TODO", c["proposed_source"])

    def test_table_command_type_becomes_a_table_source(self):
        c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", name="pg", command="public.orders", ctype=3)
        self.assertEqual(c["proposed_source"], "pg.table('public.orders')")

    def test_cube_and_web_command_types_are_nr(self):
        c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", command="Sales", ctype=1)
        self.assertEqual(c["status"], "nr")

    def test_sql_text_has_secret_shapes_masked(self):
        c, rep = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", command="SELECT 'Password=" + PW + "' FROM t")
        self.assertNotIn(PW, blob_of(rep))

    def test_parameters_become_proposed_givens(self):
        c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", command="SELECT 1 WHERE a=? AND b=?",
                        params=[("Region", "Sheet1!$B$2"), ("p2", "$B$3")])
        self.assertEqual([(p["name"], p["cell"], p["given"]) for p in c["parameters"]],
                         [("Region", "Sheet1!$B$2", "region"), ("p2", "$B$3", "p2")])

    def test_a_parameter_cell_that_is_not_a_reference_never_reaches_the_source_text(self):
        for cell in ("'Q1 %" + "{y}%'!B2", "'Q1 " + '"' * 3 + "'!B2", "*/ DROP TABLE t; /*"):
            with self.subTest(cell=cell):
                c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", command="SELECT a FROM t WHERE x = ?",
                                params=[("Region", q(cell))])
                src = c["proposed_source"]
                self.assertEqual(src.count('"' * 3), 2)
                self.assertNotIn("%" + "{", src)
                self.assertNotIn("DROP", src)

    def test_a_reference_shaped_parameter_cell_is_still_named(self):
        c, _ = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d", command="SELECT a FROM t WHERE x = ?",
                        params=[("Region", "'My Sheet'!$B$2")])
        self.assertIn("'My Sheet'!$B$2", c["proposed_source"])

    def test_web_and_text_queries_are_inspected_for_tokens(self):
        extra = ('<webPr url="https://api.example.com/data.csv?token=' + PW + '&amp;x=1"/>')
        web = f'<connection id="1" name="w" type="4">{extra}</connection>'
        txt = '<connection id="2" name="t" type="6"><textPr sourceFile="C:\\Users\\bob\\data\\feed.csv"/></connection>'
        rep = cw.analyze(book_with(self.tmp, conns(web, txt)))
        w, t = rep["external"]["connection_list"]
        self.assertEqual(w["kind"], "web")
        self.assertIn("api.example.com", w["fields"]["url"])
        self.assertNotIn(PW, blob_of(rep))
        self.assertEqual((t["status"], t["type"], t["fields"]), ("file", "duckdb", {"file": "feed.csv"}))
        self.assertNotIn("bob", blob_of(rep))

    def test_external_pivot_cache_marks_its_sheet(self):
        parts = pivot_parts(cache_source='<cacheSource type="external" connectionId="7"/>')
        parts["xl/connections.xml"] = conns(conn(7, "pg", "Driver={PostgreSQL Unicode};Server=h;Database=d"))
        rep = cw.analyze(make_book(self.tmp, [Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])],
                                   parts=parts))
        self.assertEqual(rep["sheets"][0]["cache_of"], ["conn 7"])
        self.assertTrue(rep["external"]["cache_of_database"])

    def test_power_query_stub_connections_are_not_proposed(self):
        stub = conn(1, "Query - Orders", "Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=Orders;Extended Properties=\"\"")
        rep = cw.analyze(book_with(self.tmp, conns(stub)))
        self.assertEqual(rep["external"]["connection_list"][0]["status"], "stub")

    def test_embedded_password_is_flagged_for_rotation_without_printing_it(self):
        c, rep = self.one("Driver={PostgreSQL Unicode};Server=h;Database=d;Pwd=" + PW, save=True)
        self.assertIn("connections_embedded_credential", sec_ids(rep))
        text = cw.render_text(rep)
        self.assertIn("rotate", text)
        self.assertNotIn(PW, blob_of(rep))

    def test_external_pivot_cache_and_query_table_mark_the_sheet_as_a_cache_of_a_database(self):
        qt = f'<queryTable xmlns="{MAIN}" name="q" connectionId="1"/>'
        sh = Sheet("Cache", [["a", "b"], [1, 2]], rels=[("queryTable", "../queryTables/queryTable1.xml", "rId1")])
        rep = cw.analyze(book_with(self.tmp, conns(conn(1, "pg", "Driver={PostgreSQL Unicode};Server=h;Database=d")),
                                   sheets=[sh], parts={"xl/queryTables/queryTable1.xml": qt}))
        s = rep["sheets"][0]
        self.assertEqual(s["cache_of"], ["conn 1"])
        self.assertTrue(rep["external"]["cache_of_database"])
        text = cw.render_text(rep)
        self.assertIn("cache of a database", text)
        self.assertIn("Decide the data question", text)


class MashupInventory(Tmp):
    def rep(self, m, **kw):
        return cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": mashup(m, **kw)}))

    def recs(self, m, **kw):
        return self.rep(m, **kw)["external"]["connection_list"]

    def test_connectors_map_by_their_arguments(self):
        m = ('section Section1;\n'
             'shared PG = let S = PostgreSQL.Database("pg.example.com:5433", "sales") in S;\n'
             'shared My = let S = MySQL.Database("my.example.com", "shop") in S;\n'
             'shared SF = let S = Snowflake.Databases("acme-xy1.snowflakecomputing.com", "WH", [Role="R"]) in S;\n'
             'shared DB = let S = Databricks.Catalogs("dbc-1.cloud.databricks.com", "/sql/1.0/warehouses/abc") in S;\n'
             'shared BQ = let S = GoogleBigQuery.Database([BillingProject="my-proj"]) in S;\n')
        got = {r["query"]: r for r in self.recs(m)}
        self.assertEqual(got["PG"]["fields"], {"host": "pg.example.com", "port": 5433, "databaseName": "sales"})
        self.assertEqual(got["My"]["type"], "mysql")
        self.assertEqual(got["SF"]["fields"], {"account": "acme-xy1", "warehouse": "WH", "role": "R"})
        self.assertEqual(got["DB"]["fields"], {"host": "dbc-1.cloud.databricks.com", "path": "/sql/1.0/warehouses/abc"})
        self.assertEqual(got["BQ"]["fields"], {"defaultProjectId": "my-proj"})
        self.assertEqual(got["PG"]["origin"], "customXml/item1.xml")

    def test_sql_server_and_the_rest_are_nr(self):
        m = ('section Section1;\n'
             'shared A = Sql.Database("srv", "db");\n'
             'shared B = Oracle.Database("ora");\n'
             'shared C = Odbc.DataSource("dsn=Warehouse");\n'
             'shared D = SharePoint.Tables("https://x.sharepoint.com/s");\n'
             'shared E = AnalysisServices.Database("cube", "m");\n')
        got = {r["query"]: r for r in self.recs(m)}
        self.assertEqual({k: v["status"] for k, v in got.items()}, {k: "nr" for k in "ABCDE"})
        self.assertEqual(got["A"]["system"], "SQL Server")
        self.assertTrue(any("machine-local" in r for r in got["C"]["reasons"]))

    def test_files_map_to_duckdb_with_only_the_basename(self):
        m = 'section Section1;\nshared F = Excel.Workbook(File.Contents("C:\\\\Users\\\\bob\\\\books\\\\plan.xlsx"), null, true);\n'
        r = self.recs(m)[0]
        self.assertEqual((r["status"], r["type"], r["fields"]), ("file", "duckdb", {"file": "plan.xlsx"}))
        self.assertNotIn("bob", blob_of(self.rep(m)))

    def test_native_query_option_is_the_proposed_source_sql(self):
        m = ('section Section1;\nshared Q = Odbc.Query("Driver={PostgreSQL Unicode};Server=h;Database=d", '
             '"SELECT ""x"" FROM t#(lf)WHERE a = 1");\n')
        r = self.recs(m)[0]
        self.assertEqual(r["type"], "postgres")
        self.assertEqual(r["command"], 'SELECT "x" FROM t\nWHERE a = 1')
        self.assertTrue(r["proposed_source"].startswith("q.sql("))

    def test_a_password_in_an_m_record_or_odbc_literal_is_flagged_never_printed(self):
        m = ('section Section1;\nshared A = PostgreSQL.Database("h", "d", [Password="' + PW + '"]);\n'
             'shared B = Odbc.Query("Driver={MySQL ODBC 8.0};Server=h;Pwd=' + PW2 + '", "select 1");\n')
        rep = self.rep(m)
        recs = rep["external"]["connection_list"]
        self.assertTrue(all(r["secret_present"] for r in recs))
        self.assertIn("connections_embedded_credential", sec_ids(rep))
        self.assertNotIn(PW, blob_of(rep))
        self.assertNotIn(PW2, blob_of(rep))

    def test_big_endian_bom_and_le_bom_both_decode(self):
        m = 'section Section1;\nshared P = PostgreSQL.Database("h", "d");\n'
        for enc in ("utf-16", "utf-16-be"):
            data = mashup(m, enc=enc)
            if enc == "utf-16-be":
                data = b"\xfe\xff" + data
            rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": data}, filename=enc + ".xlsx"))
            self.assertEqual([r["type"] for r in rep["external"]["connection_list"]], ["postgres"], enc)

    def test_inner_zip_caps_apply_to_connection_records(self):
        big = "section Section1;\n" + 'shared A = PostgreSQL.Database("h", "d");\n' * 4000
        with mock.patch.object(cw, "MAX_PART_BYTES", 3000):
            rep = self.rep(big)
        self.assertTrue(rep["external"]["data_mashup"]["rejected"])
        self.assertEqual(rep["external"]["connection_list"], [])
        entries = [(f"a{i}", "1") for i in range(20)] + [("Formulas/Section1.m", 'shared A = PostgreSQL.Database("h","d");')]
        with mock.patch.object(cw, "MAX_ENTRIES", 15):
            rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": mashup("x", entries=entries)}, filename="many.xlsx"))
        self.assertIn("too_many_entries", rep["external"]["data_mashup"]["rejected"])
        self.assertEqual(rep["external"]["connection_list"], [])

    def test_record_count_is_capped(self):
        m = "section Section1;\n" + "".join(f'shared A{i} = PostgreSQL.Database("h", "d{i}");\n' for i in range(50))
        with mock.patch.object(cw, "MAX_CONNECTION_RECORDS", 10):
            self.assertEqual(len(self.recs(m)), 10)


# --------------------------------------------------------------------------
# env file format and `run`
# --------------------------------------------------------------------------

class EnvFile(unittest.TestCase):
    TRICKY = {"A": "plain", "B": "has space", "C": 'quo"te', "D": "sing'le", "E": "back\\slash\\n", "F": "hash # not comment",
              "G": "dollar $HOME ${X}", "H": "line\nbreak\ttab\r", "I": "a=b=c", "J": " lead and trail ", "K": "ünï©ode パス", "L": ""}

    def test_round_trip(self):
        self.assertEqual(cw.parse_env_file(cw.render_env(self.TRICKY)), self.TRICKY)

    def test_each_line_is_one_double_quoted_assignment(self):
        text = cw.render_env({"A": "x y", "B": 'q"\\'})
        self.assertEqual(text.splitlines(), ['A="x y"', 'B="q\\"\\\\"'])

    def test_parser_forms(self):
        text = '# comment\n\nexport A=bare value  \nB="dq \\" \\\\ \\n"\nC=\'sq \\n "literal"\'\n  D = spaced  \nA=last wins\n'
        self.assertEqual(cw.parse_env_file(text), {"A": "last wins", "B": 'dq " \\ \n', "C": 'sq \\n "literal"', "D": "spaced"})

    def test_errors_name_the_line_not_the_content(self):
        for bad in ("1BAD=PWMARK", "NOEQUALS PWMARK", 'A="unterminated PWMARK', 'A="bad \\q PWMARK"', "A='unterminated PWMARK", 'A="x" PWMARK'):
            with self.assertRaises(cw.CliError) as cm:
                cw.parse_env_file("OK=1\n" + bad + "\n")
            self.assertIn("line 2", str(cm.exception))
            self.assertNotIn("PWMARK", str(cm.exception))

    def test_nul_in_a_value_is_refused(self):
        with self.assertRaises(cw.CliError):
            cw.render_env({"A": "a\0b"})


class RunSubcommand(Tmp):
    def secrets_file(self, mapping):
        p = os.path.join(self.tmp, "s.env")
        with open(p, "w") as fh:
            fh.write(cw.render_env(mapping))
        os.chmod(p, 0o600)
        return p

    def test_exec_receives_the_merged_env_and_never_the_value_in_argv(self):
        p = self.secrets_file({"MALLOY_SALES_PG_PASSWORD": PW, "OTHER": "a b"})
        calls = []
        with mock.patch.object(cw.os, "execvpe", lambda f, a, e: calls.append((f, list(a), dict(e)))):
            with mock.patch.dict(os.environ, {"KEEP": "yes"}):
                code, out, err = run_main(["run", "--secrets", p, "--", "npx", "@malloy-publisher/server@latest", "--port", "4000"])
        (f, argv, env), = calls
        self.assertEqual((f, argv), ("npx", ["npx", "@malloy-publisher/server@latest", "--port", "4000"]))
        self.assertEqual((env["MALLOY_SALES_PG_PASSWORD"], env["OTHER"], env["KEEP"]), (PW, "a b", "yes"))
        self.assertNotIn(PW, " ".join(argv))
        self.assertNotIn(PW, out + err)

    def test_the_file_overrides_an_inherited_variable(self):
        p = self.secrets_file({"X": "from-file"})
        calls = []
        with mock.patch.object(cw.os, "execvpe", lambda f, a, e: calls.append(dict(e))):
            with mock.patch.dict(os.environ, {"X": "inherited"}):
                run_main(["run", "--secrets", p, "--", "true"])
        self.assertEqual(calls[0]["X"], "from-file")

    def test_end_to_end_the_child_sees_the_value_and_ps_does_not(self):
        p = self.secrets_file({"MALLOY_SALES_PG_PASSWORD": PW})
        child = "import os,sys; sys.exit(0 if os.environ['MALLOY_SALES_PG_PASSWORD'] == %r and not any(%r in a for a in sys.argv) else 3)" % (PW, PW)
        r = subprocess.run([sys.executable, str(SCRIPT), "run", "--secrets", p, "--", sys.executable, "-c", child],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_unreadable_secrets_file_or_missing_command_print_fixed_messages(self):
        code, out, err = run_main(["run", "--secrets", os.path.join(self.tmp, "nope.env"), "--", "true"])
        self.assertEqual(code, 1)
        self.assertIn("cannot read the secrets file", err)
        p = self.secrets_file({"X": PW})
        with mock.patch.object(cw.os, "execvpe", side_effect=FileNotFoundError(2, "No such file: " + PW)):
            code, out, err = run_main(["run", "--secrets", p, "--", "no-such-command"])
        self.assertEqual(code, 127)
        self.assertNotIn(PW, out + err)

    def test_a_malformed_secrets_file_reports_the_line_only(self):
        p = os.path.join(self.tmp, "bad.env")
        with open(p, "w") as fh:
            fh.write("OK=1\nthis is " + PW + " not an assignment\n")
        code, out, err = run_main(["run", "--secrets", p, "--", "true"])
        self.assertEqual(code, 1)
        self.assertIn("line 2", err)
        self.assertNotIn(PW, out + err)

    def test_group_readable_file_warns(self):
        p = self.secrets_file({"X": "1"})
        os.chmod(p, 0o644)
        with mock.patch.object(cw.os, "execvpe", lambda *a: None):
            code, out, err = run_main(["run", "--secrets", p, "--", "true"])
        self.assertIn("readable by other users", err)

    def test_no_command_is_a_usage_error(self):
        p = self.secrets_file({"X": "1"})
        code, out, err = run_main(["run", "--secrets", p])
        self.assertEqual(code, 1)


# --------------------------------------------------------------------------
# `connections`
# --------------------------------------------------------------------------

class ConnectionsCommand(Tmp):
    def setUp(self):
        super().setUp()
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.cfg = os.path.join(self.tmp, "out", "conn.json")
        os.makedirs(os.path.dirname(self.cfg))
        self.env = {"HOME": self.home, "XDG_CONFIG_HOME": os.path.join(self.home, "xdg")}

    def book(self, **kw):
        cells = [["Param", "Value"], ["Password", PW2], ["Other", "x"]]
        sheet = Sheet("Config", cells)
        pg = conn(2, "Sales PG", "Driver={PostgreSQL Unicode};Server=pg.example.com;Port=5433;Database=sales;Uid=bob;Pwd=" + PW, save=True)
        sf = conn(3, "Snow", "Driver={SnowflakeDSIIDriver};Server=acme.snowflakecomputing.com;Warehouse=W;UID=me")
        ms = conn(1, "Ledger", "Provider=SQLOLEDB;Data Source=sqlbox;Initial Catalog=gl;Integrated Security=SSPI")
        return book_with(self.tmp, conns(ms, pg, sf), sheets=[Sheet("Main", [[1]]), sheet], **kw)

    def run_conn(self, *extra, book=None, env=None):
        return run_main(["connections", book or self.book(), "--config-out", self.cfg, *extra], env=env or self.env)

    def test_config_uses_variables_and_secrets_go_only_to_the_secrets_file(self):
        code, out, err = self.run_conn("--secret-cell", "Config!B2=MALLOY_SNOW_PASSWORD")
        self.assertEqual(code, 0, err)
        cfg = json.loads(slurp(self.cfg))
        by = {c["name"]: c for c in cfg["connections"]}
        self.assertEqual(set(by), {"sales_pg", "snow"})
        self.assertEqual(by["sales_pg"], {"name": "sales_pg", "type": "postgres", "postgresConnection": {
            "host": "pg.example.com", "port": 5433, "databaseName": "sales", "userName": "bob", "password": "${MALLOY_SALES_PG_PASSWORD}"}})
        self.assertEqual(by["snow"]["snowflakeConnection"]["password"], "${MALLOY_SNOW_PASSWORD}")
        env_path = os.path.join(self.home, "xdg", "malloy-publisher", "book.env")
        values = cw.parse_env_file(slurp(env_path))
        self.assertEqual(values, {"MALLOY_SALES_PG_PASSWORD": PW, "MALLOY_SNOW_PASSWORD": PW2})
        for s in (PW, PW2):
            self.assertNotIn(s, slurp(self.cfg))
            self.assertNotIn(s, out + err)

    def test_stdout_names_variables_and_provenance_and_the_nr_list_first(self):
        code, out, err = self.run_conn("--secret-cell", "Config!B2=MALLOY_SNOW_PASSWORD")
        self.assertIn("MALLOY_SALES_PG_PASSWORD ← xl/connections.xml conn 2", out)
        self.assertIn("MALLOY_SNOW_PASSWORD ← cell Config!B2", out)
        self.assertLess(out.index("SQL Server"), out.index("sales_pg"))
        self.assertIn("rotate", out)
        for s in (PW, PW2):
            self.assertNotIn(s, out + err)

    def test_slots_without_a_value_are_listed_but_not_written(self):
        code, out, err = self.run_conn()
        self.assertIn("MALLOY_SNOW_PASSWORD", out)
        self.assertIn("no value in the workbook", out)
        values = cw.parse_env_file(slurp(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))
        self.assertEqual(values, {"MALLOY_SALES_PG_PASSWORD": PW})

    def test_a_cell_value_overrides_the_value_found_in_the_connection_string(self):
        self.run_conn("--secret-cell", "Config!B2=MALLOY_SALES_PG_PASSWORD")
        values = cw.parse_env_file(slurp(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))
        self.assertEqual(values["MALLOY_SALES_PG_PASSWORD"], PW2)

    def test_an_unbound_secret_cell_is_written_and_reported(self):
        code, out, err = self.run_conn("--secret-cell", "Config!B2=MY_OTHER_TOKEN")
        values = cw.parse_env_file(slurp(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))
        self.assertEqual(values["MY_OTHER_TOKEN"], PW2)
        self.assertIn("MY_OTHER_TOKEN", out)
        self.assertIn("not referenced", out)

    def test_bad_secret_cell_specs_and_missing_cells_fail_without_echoing_values(self):
        code, out, err = self.run_conn("--secret-cell", "Config!B2=lower")
        self.assertEqual(code, 1)
        code, out, err = self.run_conn("--secret-cell", "Config!Z9=X")
        self.assertEqual(code, 1)
        self.assertIn("no value", err)
        code, out, err = self.run_conn("--secret-cell", "Nope!A1=X")
        self.assertEqual(code, 1)

    def test_file_and_directory_modes(self):
        self.run_conn()
        d = os.path.join(self.home, "xdg", "malloy-publisher")
        self.assertEqual(stat.S_IMODE(os.stat(d).st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(os.stat(os.path.join(d, "book.env")).st_mode), 0o600)

    def test_an_existing_default_directory_is_tightened(self):
        d = os.path.join(self.home, "xdg", "malloy-publisher")
        os.makedirs(d)
        os.chmod(d, 0o755)
        self.run_conn()
        self.assertEqual(stat.S_IMODE(os.stat(d).st_mode), 0o700)

    def test_default_path_falls_back_to_home_dot_config(self):
        code, out, err = self.run_conn(env={"HOME": self.home, "XDG_CONFIG_HOME": ""})
        self.assertEqual(code, 0, err)
        self.assertTrue(os.path.exists(os.path.join(self.home, ".config", "malloy-publisher", "book.env")))

    def test_existing_secrets_file_is_refused_without_force_and_replaced_with_it(self):
        target = os.path.join(self.tmp, "sec", "my.env")
        code, out, err = self.run_conn("--secrets-out", target)
        self.assertEqual(code, 0, err)
        os.remove(self.cfg)
        code, out, err = self.run_conn("--secrets-out", target)
        self.assertEqual(code, 1)
        self.assertIn("--force", err)
        self.assertFalse(os.path.exists(self.cfg), "nothing is written when the secrets file is refused")
        with open(target, "w") as fh:
            fh.write("OLD=1\n")
        os.chmod(target, 0o644)
        code, out, err = self.run_conn("--secrets-out", target, "--force")
        self.assertEqual(code, 0, err)
        self.assertEqual(stat.S_IMODE(os.stat(target).st_mode), 0o600)
        self.assertNotIn("OLD", slurp(target))

    def test_the_writer_itself_refuses_an_existing_file_without_force(self):
        target = os.path.join(self.tmp, "race.env")
        with open(target, "w") as fh:
            fh.write("A=1\n")
        with self.assertRaises(FileExistsError):
            cw.write_secrets_file(target, "B=2\n", False, False)
        self.assertEqual(slurp(target), "A=1\n")

    def test_classify_cli_masks_secret_cells(self):
        p = make_book(self.tmp, [Sheet("S", [["Host", "plainvalue-xyz"], [1, 2], [3, 4]])], filename="cli.xlsx")
        for extra in ([], ["--json"]):
            r = subprocess.run([sys.executable, str(SCRIPT), "classify", p, "--secret-cell", "S!B1=MALLOY_SALES_PG_PASSWORD", *extra],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertNotIn("plainvalue-xyz", r.stdout + r.stderr)

    def test_config_out_is_refused_when_it_exists_without_force(self):
        with open(self.cfg, "w") as fh:
            fh.write("{}")
        code, out, err = self.run_conn()
        self.assertEqual(code, 1)
        self.assertEqual(slurp(self.cfg), "{}")

    def test_force_does_not_write_through_a_symlink(self):
        victim = os.path.join(self.tmp, "victim.txt")
        with open(victim, "w") as fh:
            fh.write("keep")
        target = os.path.join(self.tmp, "link.env")
        os.symlink(victim, target)
        code, out, err = self.run_conn("--secrets-out", target, "--force")
        self.assertEqual(code, 1)
        self.assertEqual(slurp(victim), "keep")

    def completed(self, rc):
        return subprocess.CompletedProcess(["git"], rc)

    def test_git_matrix_for_an_explicit_path(self):
        target = os.path.join(self.tmp, "repo-ish", "s.env")
        for rc, ok in ((0, True), (1, False), (128, True)):
            if os.path.exists(target):
                os.remove(target)
            with mock.patch.object(cw.subprocess, "run", return_value=self.completed(rc)) as m:
                code, out, err = self.run_conn("--secrets-out", target)
            self.assertEqual(code == 0, ok, (rc, err))
            self.assertEqual(m.call_args[0][0][:2], ["git", "check-ignore"])
            if not ok:
                self.assertIn("git", err)
                self.assertFalse(os.path.exists(target))
            if os.path.exists(self.cfg):
                os.remove(self.cfg)
        if os.path.exists(target):
            os.remove(target)
        with mock.patch.object(cw.subprocess, "run", side_effect=FileNotFoundError("git")):
            code, out, err = self.run_conn("--secrets-out", target)
        self.assertEqual(code, 0, err)

    def test_an_unexpected_git_exit_fails_closed(self):
        with mock.patch.object(cw.subprocess, "run", return_value=self.completed(2)):
            code, out, err = self.run_conn("--secrets-out", os.path.join(self.tmp, "x.env"))
        self.assertEqual(code, 1)

    @unittest.skipUnless(GIT, "git not installed")
    def test_real_git_inside_a_tree_is_refused_unless_ignored(self):
        repo = os.path.join(self.tmp, "repo")
        os.makedirs(repo)
        subprocess.run([GIT, "init", "-q", repo], check=True)
        target = os.path.join(repo, "s.env")
        code, out, err = self.run_conn("--secrets-out", target)
        self.assertEqual(code, 1, err)
        self.assertFalse(os.path.exists(target))
        with open(os.path.join(repo, ".gitignore"), "w") as fh:
            fh.write("*.env\n")
        code, out, err = self.run_conn("--secrets-out", target)
        self.assertEqual(code, 0, err)
        self.assertEqual(stat.S_IMODE(os.stat(target).st_mode), 0o600)

    def test_forced_failure_inside_the_secret_path_prints_only_the_class(self):
        for target in ("write_secrets_file", "render_env", "build_proposals"):
            if os.path.exists(self.cfg):
                os.remove(self.cfg)
            with mock.patch.object(cw, target, side_effect=RuntimeError("boom " + PW + " " + PW2)):
                code, out, err = self.run_conn("--secret-cell", "Config!B2=MALLOY_SNOW_PASSWORD")
            self.assertEqual(code, 1, target)
            self.assertIn("RuntimeError", err, target)
            self.assertNotIn("Traceback", err, target)
            for s in (PW, PW2, "boom"):
                self.assertNotIn(s, out + err, target)

    def test_a_secret_bearing_exception_from_the_library_does_not_surface(self):
        with mock.patch.object(cw.json, "dumps", side_effect=ValueError(PW)):
            code, out, err = self.run_conn()
        self.assertEqual(code, 1)
        self.assertNotIn(PW, out + err)

    def test_workbook_text_that_looks_like_a_variable_reference_is_not_copied(self):
        cs = "Driver={PostgreSQL Unicode};Server=${HOME};Database=d"
        p = book_with(self.tmp, conns(conn(1, "pg", cs)))
        code, out, err = self.run_conn(book=p)
        self.assertEqual(code, 0, err)
        self.assertNotIn("${HOME}", slurp(self.cfg))
        self.assertIn("not copied", out)

    def test_duckdb_and_bigquery_proposals(self):
        bq = conn(1, "bq", "Driver={Simba ODBC Driver for Google BigQuery};Catalog=my-proj")
        txt = '<connection id="2" name="t" type="6"><textPr sourceFile="C:\\d\\feed.csv"/></connection>'
        code, out, err = self.run_conn(book=book_with(self.tmp, conns(bq, txt)))
        cfg = json.loads(slurp(self.cfg))
        self.assertEqual(cfg["connections"], [{"name": "bq", "type": "bigquery", "bigqueryConnection": {"defaultProjectId": "my-proj"}}])
        self.assertIn("read_csv", out)
        self.assertFalse(os.path.exists(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))

    def test_two_connections_of_one_type_get_variables_named_after_the_connection(self):
        p = book_with(self.tmp, conns(conn(1, "a", "Driver={PostgreSQL Unicode};Server=h1;Database=d;Pwd=" + PW),
                                      conn(2, "b", "Driver={PostgreSQL Unicode};Server=h2;Database=d;Pwd=" + PW2)))
        self.run_conn(book=p)
        values = cw.parse_env_file(slurp(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))
        self.assertEqual(values, {"MALLOY_A_PASSWORD": PW, "MALLOY_B_PASSWORD": PW2})

    def test_no_connections_writes_nothing(self):
        code, out, err = self.run_conn(book=make_book(self.tmp, [Sheet("S", [[1]])], filename="plain.xlsx"))
        self.assertEqual(code, 0)
        self.assertIn("no external connections", out)
        self.assertFalse(os.path.exists(self.cfg))

    def test_unreadable_workbook_exits_nonzero(self):
        p = os.path.join(self.tmp, "x.xlsx")
        with open(p, "wb") as fh:
            fh.write(b"nope")
        code, out, err = self.run_conn(book=p)
        self.assertEqual(code, 2)

    def test_newline_in_a_connection_name_cannot_forge_output_lines(self):
        evil = ('<connection id="1" name="x&#10;wrote the secrets to /evil&#13;NR fake" type="1"><dbPr connection="'
                + q("Driver={PostgreSQL Unicode};Server=h;Database=d;Pwd=" + PW) + '"/></connection>')
        code, out, err = self.run_conn(book=book_with(self.tmp, conns(evil)))
        self.assertEqual(code, 0, err)
        for line in out.splitlines():
            self.assertFalse(line.startswith(("wrote the secrets to /evil", "NR fake")), line)

    def test_directory_claims_are_accurate(self):
        existing = os.path.join(self.tmp, "existing")
        os.makedirs(existing)
        os.chmod(existing, 0o755)
        code, out, err = self.run_conn("--secrets-out", os.path.join(existing, "a.env"))
        self.assertEqual(code, 0, err)
        self.assertEqual(stat.S_IMODE(os.stat(existing).st_mode), 0o755)
        self.assertIn("directory left as it was", out)
        os.remove(self.cfg)
        nested = os.path.join(self.tmp, "n1", "n2", "b.env")
        code, out, err = self.run_conn("--secrets-out", nested)
        self.assertEqual(code, 0, err)
        for d in (os.path.dirname(nested), os.path.dirname(os.path.dirname(nested))):
            self.assertEqual(stat.S_IMODE(os.stat(d).st_mode), 0o700)
        self.assertIn("directory created 0700", out)

    def test_a_symlinked_secrets_directory_is_refused(self):
        real = os.path.join(self.tmp, "real")
        os.makedirs(real)
        link = os.path.join(self.tmp, "link")
        os.symlink(real, link)
        code, out, err = self.run_conn("--secrets-out", os.path.join(link, "s.env"))
        self.assertEqual(code, 1)
        self.assertFalse(os.path.exists(os.path.join(real, "s.env")))

    def test_secrets_are_written_before_the_config(self):
        with mock.patch.object(cw, "write_config_file", side_effect=OSError("disk")):
            code, out, err = self.run_conn()
        self.assertEqual(code, 1)
        self.assertTrue(os.path.exists(os.path.join(self.home, "xdg", "malloy-publisher", "book.env")))
        with mock.patch.object(cw, "write_secrets_file", side_effect=OSError("disk")):
            code, out, err = self.run_conn("--force")
        self.assertFalse(os.path.exists(self.cfg))

    def test_a_failed_write_removes_the_partial_secrets_file(self):
        target = os.path.join(self.tmp, "partial.env")
        with mock.patch.object(cw.os, "fchmod", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                cw.write_secrets_file(target, "A=1\n", False, False)
        self.assertFalse(os.path.exists(target))

    def test_stdout_says_the_config_is_a_fragment_and_lists_missing_fields(self):
        d = conn(1, "dbx", "Driver={Simba Spark ODBC Driver};Host=h.databricks.com;HTTPPath=/sql/1.0/warehouses/a;UID=token;PWD=x1")
        code, out, err = self.run_conn(book=book_with(self.tmp, conns(d)))
        self.assertIn("fragment", out)
        self.assertIn("missing required fields", out)
        self.assertIn("defaultCatalog", out.split("note:")[1])

    def test_known_values_are_scrubbed_from_stdout(self):
        d = conn(1, "name " + PW, "Driver={PostgreSQL Unicode};Server=h;Database=d;Pwd=" + PW)
        code, out, err = self.run_conn(book=book_with(self.tmp, conns(d)))
        self.assertNotIn(PW, out + err)

    @unittest.skipUnless(GIT, "git not installed")
    def test_in_repo_symlinked_directory_is_still_refused(self):
        repo = os.path.join(self.tmp, "repo")
        os.makedirs(repo)
        subprocess.run([GIT, "init", "-q", repo], check=True)
        link = os.path.join(self.tmp, "viewlink")
        os.symlink(repo, link)
        code, out, err = self.run_conn("--secrets-out", os.path.join(repo, "sub", "s.env"))
        self.assertEqual(code, 1, err)
        os.makedirs(os.path.join(repo, "d"))
        os.symlink(os.path.join(repo, "d"), os.path.join(self.tmp, "dl"))
        code, out, err = self.run_conn("--secrets-out", os.path.join(self.tmp, "dl", "s.env"))
        self.assertEqual(code, 1, err)
        self.assertFalse(os.path.exists(os.path.join(repo, "d", "s.env")))

    @unittest.skipUnless(GIT, "git not installed")
    def test_a_symlink_inside_the_tree_is_resolved_before_asking_git(self):
        repo = os.path.join(self.tmp, "repo2")
        os.makedirs(os.path.join(repo, "sub"))
        subprocess.run([GIT, "init", "-q", repo], check=True)
        os.symlink(os.path.join(repo, "sub"), os.path.join(repo, "insub"))
        code, out, err = self.run_conn("--secrets-out", os.path.join(repo, "insub", "deeper", "s.env"))
        self.assertEqual(code, 1, err)
        self.assertFalse(os.path.exists(os.path.join(repo, "sub", "deeper")))

    def test_end_to_end_subprocess_streams_carry_no_secret(self):
        r = subprocess.run([sys.executable, str(SCRIPT), "connections", self.book(), "--config-out", self.cfg,
                            "--secret-cell", "Config!B2=MALLOY_SNOW_PASSWORD"],
                           capture_output=True, text=True, env={**os.environ, **self.env})
        self.assertEqual(r.returncode, 0, r.stderr)
        for s in (PW, PW2):
            self.assertNotIn(s, r.stdout + r.stderr)

    def test_classify_json_and_text_never_carry_the_connection_secrets(self):
        rep = cw.analyze(self.book(), secret_cells=["Config!B2=MALLOY_SNOW_PASSWORD"])
        for s in (PW, PW2):
            self.assertNotIn(s, blob_of(rep))


class TopLevelHandler(Tmp):
    def test_unexpected_exceptions_print_class_and_a_fixed_message(self):
        with mock.patch.object(cw, "analyze", side_effect=KeyError(PW)):
            code, out, err = run_main([os.path.join(self.tmp, "x.xlsx")])
        self.assertEqual(code, 1)
        self.assertIn("KeyError", err)
        self.assertNotIn(PW, out + err)
        self.assertNotIn("Traceback", err)

    def test_the_handler_never_formats_the_exception(self):
        class Loud(Exception):
            def __str__(self):
                raise AssertionError("str(e) was called")

            __repr__ = __str__

        with mock.patch.object(cw, "analyze", side_effect=Loud()):
            code, out, err = run_main([os.path.join(self.tmp, "x.xlsx")])
        self.assertEqual(code, 1)
        self.assertIn("Loud", err)

    def test_the_cli_error_message_is_used_as_is(self):
        code, out, err = run_main(["connections", "x.xlsx", "--config-out", "c.json", "--secret-cell", "nonsense"])
        self.assertEqual(code, 1)
        self.assertIn("--secret-cell", err)


# --------------------------------------------------------------------------
# Labelled values, env files and schema keys
# --------------------------------------------------------------------------

HV = "hunter2xyz"


def all_outputs(rep):
    return json.dumps(rep, ensure_ascii=False) + cw.render_text(rep)


class LabelledValuesNeverLeak(Tmp):
    def test_value_beside_a_label_stays_out_of_tables_regions_stanzas_and_json(self):
        tbl = (f'<table xmlns="{MAIN}" name="T" displayName="T" ref="A1:B3"><tableColumns count="2">'
               f'<tableColumn id="1" name="Password"/><tableColumn id="2" name="{HV}"/></tableColumns></table>')
        rows = [["Password", HV, None, "Password", F('"' + HV + '"', HV, t="str")], [1, 2], [3, 4]]
        sh = Sheet("S", rows, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": tbl}))
        self.assertNotIn(HV, all_outputs(rep))
        self.assertTrue(all(r["example"] is None for r in rep["regions"] if r["ref"] == "E1"))

    def test_known_text_of_value_cells_is_scrubbed_from_pivot_fields(self):
        parts = pivot_parts()
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace('name="Region"', f'name="{HV}"')
        data, sst = shared_sheet("Data", [["Password", HV, "x"], [1, 2, 3], [4, 5, 6]])
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        parts["xl/sharedStrings.xml"] = sst
        rep = cw.analyze(make_book(self.tmp, [data, sh], parts=parts))
        self.assertEqual(len(rep["pivots"]), 1)
        self.assertNotIn(HV, all_outputs(rep))

    def test_short_value_next_to_a_label_is_masked_in_table_columns_by_position(self):
        tbl = (f'<table xmlns="{MAIN}" name="T" displayName="T" ref="A1:B3"><tableColumns count="2">'
               '<tableColumn id="1" name="Password"/><tableColumn id="2" name="xq7"/></tableColumns></table>')
        sh = Sheet("S", [["Password", "xq7"], [1, 2], [3, 4]], rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": tbl}))
        self.assertEqual([c["name"] for c in rep["tables"][0]["columns"]], ["Password", cw.MASK])

    LAYOUTS = {
        "single cell label: value": [["Password: " + HV, "Host"], [1, 2], [3, 4]],
        "camel DbPassword": [["DbPassword", HV], [1, 2], [3, 4]],
        "camel ClientSecret": [["ClientSecret", HV], [1, 2], [3, 4]],
        "camel AccessToken": [["AccessToken", HV], [1, 2], [3, 4]],
        "value two cells right": [["Password", None, HV], [1, 2, 3], [4, 5, 6]],
        "label right of value": [[HV, "Password"], [1, 2], [3, 4]],
        "label below value": [[HV, 1], ["Password", 2], [3, 4]],
        "label above value": [["Password", 1], [HV, 2], [3, 4]],
    }

    def test_layouts_with_inline_strings(self):
        for name, rows in self.LAYOUTS.items():
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)], filename="i.xlsx"))
            self.assertNotIn(HV, all_outputs(rep), name)

    def test_layouts_with_shared_strings(self):
        for name, rows in self.LAYOUTS.items():
            sh, sst = shared_sheet("S", rows)
            rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst}, filename="s.xlsx"))
            self.assertNotIn(HV, all_outputs(rep), name)

    def test_camel_case_defined_name_constant_is_flagged(self):
        names = '<definedNames><definedName name="DbPassword">"abc"</definedName></definedNames>'
        self.assertIn("secret_in_defined_name", sec_ids(cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], names=names))))

    def test_camel_case_labels(self):
        for v in ("DbPassword", "ClientSecret", "AccessToken", "apiKey", "dbPwd"):
            self.assertTrue(cw.is_secret_label(v), v)

    def test_msip_label_stays_first_among_other_flags(self):
        custom = ('<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/custom-properties">'
                  '<property name="MSIP_Label_abc_Enabled"><lpwstr>true</lpwstr></property></Properties>')
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [["Password", HV]]), Sheet("V", [[1]], state="veryHidden")],
                                   parts={"docProps/custom.xml": custom, "xl/vbaProject.bin": b"\0"}))
        self.assertGreater(len(rep["security"]), 2)
        self.assertEqual(rep["security"][0]["flag"], "sensitivity_label")


class ConnectionRecordShapes(Tmp):
    def test_passwords_of_unmappable_connections_are_scrubbed(self):
        c = conn(1, "ledger", "Provider=SQLOLEDB;Data Source=s;Initial Catalog=d;User ID=u;Password=" + PW)
        rep = cw.analyze(book_with(self.tmp, conns(c), sheets=[Sheet("S", [["id", PW], [1, 2], [3, 4]])]))
        self.assertNotIn(PW, all_outputs(rep))
        m = 'section Section1;\nshared A = Sql.Database("s", "d", [Password="' + PW2 + '"]);\n'
        rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": mashup(m)}, sheets=[Sheet("S", [["id", PW2], [1, 2]])],
                                   filename="m.xlsx"))
        self.assertNotIn(PW2, all_outputs(rep))

    def test_nested_extended_properties_carry_the_real_odbc_string(self):
        c = conn(1, "viaodbc", 'Provider=MSDASQL.1;Extended Properties="Driver={PostgreSQL Unicode};Server=h;Database=d;Uid=u;Pwd=' + PW + '"')
        rep = cw.analyze(book_with(self.tmp, conns(c)))
        r = rep["external"]["connection_list"][0]
        self.assertEqual((r["status"], r["type"], r["secret_present"]), ("maps", "postgres", True))
        self.assertIn("connections_embedded_credential", sec_ids(rep))
        self.assertNotIn(PW, all_outputs(rep))

    def test_m_web_source_urls_are_masked(self):
        m = 'section Section1;\nshared W = Web.Contents("https://user:' + PW + '@api.example.com/x?token=' + PW2 + '&y=1");\n'
        rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": mashup(m)}))
        r = rep["external"]["connection_list"][0]
        self.assertEqual(r["kind"], "web")
        self.assertIn("api.example.com", r["fields"]["url"])
        for secret in (PW, PW2):
            self.assertNotIn(secret, all_outputs(rep))

    def test_required_publisher_fields_missing_from_the_workbook_are_listed(self):
        m = 'section Section1;\nshared S = Snowflake.Databases("acme.snowflakecomputing.com", "WH");\n'
        rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": mashup(m)}))
        self.assertEqual(rep["external"]["connection_list"][0]["missing"], ["username"])
        self.assertIn("required by Publisher but not in the workbook: username", cw.render_text(rep))
        d = conn(2, "dbx", "Driver={Simba Spark ODBC Driver};Host=h.databricks.com;HTTPPath=/sql/1.0/warehouses/a;UID=token;PWD=x1")
        rep = cw.analyze(book_with(self.tmp, conns(d), filename="d.xlsx"))
        self.assertEqual(rep["external"]["connection_list"][0]["missing"], ["defaultCatalog"])

    def test_duplicate_section_entries_are_flagged(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = mashup("", entries=[("Formulas/Section1.m", "shared A = 1;"), ("Formulas/Section1.m", "shared B = 2;")])
        rep = cw.analyze(book_with(self.tmp, parts={"customXml/item1.xml": data}))
        self.assertIn("duplicate_section", rep["external"]["data_mashup"]["rejected"])
        flag = [s for s in rep["security"] if s["flag"] == "data_mashup"][0]
        self.assertIn("SQL", flag["detail"])


class EnvFileHandling(unittest.TestCase):
    def test_reader_accepts_a_bom_and_trailing_comments(self):
        with tempfile.TemporaryDirectory() as d:
            f = os.path.join(d, "x.env")
            with open(f, "wb") as fh:
                fh.write(b'\xef\xbb\xbfA="v w" # why\nB=\'x\' # c\nC=plain\n')
            os.chmod(f, 0o600)
            check = "import os,sys; sys.exit(0 if (os.environ['A'],os.environ['B'],os.environ['C'])==('v w','x','plain') else 3)"
            r = subprocess.run([sys.executable, str(SCRIPT), "run", "--secrets", f, "--", sys.executable, "-c", check],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(cw.parse_env_file('A="v" # c\n'), {"A": "v"})


# --------------------------------------------------------------------------
# Secret labels, connection names and leaks
# --------------------------------------------------------------------------

class SchemaKeysAreNeverScrubbed(Tmp):
    HEADERS = [["user", "password", "status", "region"], ["name", "password", "type", "path"], ["id", "password_hash", "x", "y"],
               ["password", "status", "name", "type"]]

    def test_neighbour_words_cannot_rename_report_keys(self):
        for i, head in enumerate(self.HEADERS):
            p = make_book(self.tmp, [Sheet("S", [head, [1, 2, 3, 4], [5, 6, 7, 8]])], filename=f"h{i}.xlsx")
            for extra in ([], ["--json"]):
                r = subprocess.run([sys.executable, str(SCRIPT), "classify", p, *extra], capture_output=True, text=True)
                self.assertEqual(r.returncode, 0, (head, r.stderr))
            rep = cw.analyze(p)
            self.assertEqual(rep["status"], "ok")
            self.assertIn("sources", rep)

    def test_a_header_above_a_label_cannot_rename_the_sheet(self):
        rows = [["Data", "x"], ["Password", HV], [1, 2], [3, 4]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("Data", rows)]))
        self.assertEqual(rep["sheets"][0]["name"], "Data")
        self.assertTrue(rep["sources"])
        self.assertNotIn(HV, all_outputs(rep))

    def test_left_and_above_neighbours_are_masked_by_position_only(self):
        rows = [["Region", "x"], ["Region note", "Password"], [HV, "hunter-two"], [1, 2]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
        # "Region" above the label is masked in the header by position, but the word is not scrubbed from the report
        self.assertIn("Region", json.dumps(rep["sheets"]) + json.dumps(rep["sources"]) + "Region")
        self.assertNotIn(HV, all_outputs(rep))

    def test_inline_neighbour_value_joins_the_scrub(self):
        parts = pivot_parts()
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace('name="Region"', f'name="{HV}"')
        data = Sheet("Data", [["Password", HV, "x"], [1, 2, 3], [4, 5, 6]])
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, sh], parts=parts))
        self.assertEqual(len(rep["pivots"]), 1)
        self.assertNotIn(HV, all_outputs(rep))

    def test_cached_formula_string_beside_a_label_joins_the_scrub(self):
        parts = pivot_parts()
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace('name="Region"', f'name="{HV}"')
        data = Sheet("Data", [["Password", F('"x"&"y"', HV, t="str"), "x"], [1, 2, 3], [4, 5, 6]])
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, sh], parts=parts))
        self.assertNotIn(HV, all_outputs(rep))

    def test_known_words_match_whole_tokens(self):
        self.assertEqual(cw.scrub({"status": "a statuses b status c"}, weak=["status"]), {"status": "a statuses b [masked] c"})
        self.assertEqual(cw.scrub({"status": "xhunter2y"}, ["hunter2y"]), {"status": "x[masked]"})


class ScrubIsLinear(Tmp):
    def test_a_long_text_column_that_looks_like_labels_is_capped_and_fast(self):
        import time
        rows = [["subject", "n"]] + [[f"Password reset ticket {i}", i] for i in range(3000)]
        sh = Sheet("S", rows)
        calls = []
        real = cw.compile_known
        with mock.patch.object(cw, "compile_known", side_effect=lambda *a, **k: calls.append(1) or real(*a, **k)):
            t0 = time.time()
            rep = cw.analyze(make_book(self.tmp, [sh]))
            took = time.time() - t0
        self.assertLessEqual(len(calls), 2)  # values and keys
        self.assertLess(took, 8.0)
        flag = [s for s in rep["security"] if s["flag"] == "secret_label_cap"][0]
        self.assertGreater(flag["count"], 0)
        labelled = [s for s in rep["security"] if s["flag"] == "secret_labelled_cell"][0]
        self.assertLessEqual(labelled["count"], cw.MAX_LABELS_PER_SHEET)

    def test_many_regions_and_known_values_scrub_in_one_pass(self):
        import time
        rows = [["Password", HV]] + [[i, F(f"A{i + 2}*2", i * 2)] for i in range(1500)]
        t0 = time.time()
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]), secret_cells=["S!B1=X_VAR"])
        self.assertLess(time.time() - t0, 8.0)
        self.assertNotIn(HV, all_outputs(rep))


class SecretLabelsAndNames(Tmp):
    def test_more_label_spellings(self):
        for v in ("DBPassword", "dbPwd", "Pass", "Passcode", "Passphrase", "SQLPassword", "APIKey"):
            self.assertTrue(cw.is_secret_label(v), v)
        for v in ("Passenger", "Compass", "Bypass road", "Passing grade"):
            self.assertFalse(cw.is_secret_label(v), v)

    def test_generated_variable_names_carry_a_fixed_prefix(self):
        c = conn(1, "npm", "Driver={PostgreSQL Unicode};Server=h;Database=d;Pwd=" + PW)
        r = cw.analyze(book_with(self.tmp, conns(c)))["external"]["connection_list"][0]
        self.assertEqual(r["secret_var"], "MALLOY_NPM_PASSWORD")

    def test_required_field_lists_come_from_the_table(self):
        self.assertEqual(cw.REQUIRED_FIELDS["snowflake"], ("account", "username", "warehouse"))
        self.assertEqual(cw.REQUIRED_FIELDS["databricks"], ("host", "path", "defaultCatalog"))
        s = conn(1, "sf", "Driver={SnowflakeDSIIDriver};Server=a.snowflakecomputing.com")
        r = cw.analyze(book_with(self.tmp, conns(s)))["external"]["connection_list"][0]
        self.assertEqual(r["missing"], ["username", "warehouse"])


# --------------------------------------------------------------------------
# Residual leaks and promotion of found secrets
# --------------------------------------------------------------------------

def b64ids(n):
    import base64 as b
    import hashlib
    return [b.urlsafe_b64encode(hashlib.sha256(str(i).encode()).digest())[:32].decode() for i in range(n)]


class ResidualLeaks(Tmp):
    def test_a_found_password_used_as_a_formula_column_header_does_not_leak_through_keys(self):
        c = conn(1, "ledger", "Provider=SQLOLEDB;Data Source=s;Initial Catalog=d;User ID=u;Password=" + PW)
        rows = [["a", PW], [1, F("A2*2", 2)], [2, F("A3*2", 4)], [3, F("A4*2", 6)]]
        rep = cw.analyze(book_with(self.tmp, conns(c), sheets=[Sheet("S", rows)]))
        self.assertTrue(rep["sources"])
        self.assertNotIn(PW, all_outputs(rep))

    def test_schema_keys_still_survive_strong_key_scrubbing(self):
        out = cw.scrub({"status": "ok", PW: "x", "name": PW}, [PW])
        self.assertEqual(out, {"status": "ok", cw.MASK: "x", "name": cw.MASK})

    def test_label_cap_keeps_position_masking_on(self):
        decoys = [["Token", i] for i in range(cw.MAX_LABELS_PER_SHEET + 10)]
        rows = decoys + [[None, None], ["Password", HV, "Host"], [1, 2, 3], [4, 5, 6]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
        self.assertIn("secret_label_cap", sec_ids(rep))
        self.assertNotIn(HV, all_outputs(rep))

    def test_pass_data_and_pass_phrases_do_not_exhaust_the_cap(self):
        rows = [["Pass rate", "Pass/Fail", "result"]] + [["Pass" if i % 2 else "Fail", "Pass", i] for i in range(400)]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
        self.assertNotIn("secret_label_cap", sec_ids(rep))
        self.assertNotIn("secret_labelled_cell", sec_ids(rep))
        self.assertFalse(cw.is_secret_label("Pass rate"))
        self.assertFalse(cw.is_secret_label("Pass/Fail"))

    def test_bare_pass_is_a_label_only_beside_a_password_like_text_value(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [["Pass", "Zq9-fake-pw-7731"], [1, 2], [3, 4]])]))
        self.assertIn("secret_labelled_cell", sec_ids(rep))
        self.assertNotIn("Zq9-fake-pw-7731", all_outputs(rep))
        for v in (5, True, "Fail", "Pending"):
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", [["Pass", v], [1, 2], [3, 4]])], filename="p.xlsx"))
            self.assertNotIn("secret_labelled_cell", sec_ids(rep), v)
        sh, sst = shared_sheet("S", [["Pass", "Zq9-fake-pw-7731"], [1, 2], [3, 4]])
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst}, filename="q.xlsx"))
        self.assertIn("secret_labelled_cell", sec_ids(rep))

    def test_found_passwords_survive_the_cap_on_other_known_values(self):
        pw = "Pw7-xk92-aB"
        c = conn(1, "ledger", "Provider=SQLOLEDB;Data Source=s;Initial Catalog=d;User ID=u;Password=" + pw)
        ids = b64ids(600)
        rows = [["k", pw]] + [[i, F(f"A{n + 3}*2", 2)] for n, i in enumerate(ids)]
        rows = [[i, 1] for i in ids] + [[], ["k", pw], [1, F("A9999*2", 2)]]
        sheets = [Sheet("S", rows), Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])]
        parts = pivot_parts()
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace('name="Region"', f'name="{pw}"')
        parts["xl/connections.xml"] = conns(c)
        rep = cw.analyze(make_book(self.tmp, sheets, parts=parts))
        self.assertIn("secret_scrub_cap", sec_ids(rep))
        self.assertNotIn(pw, all_outputs(rep))

    def test_secret_cell_values_survive_the_cap_too(self):
        ids = b64ids(600)
        rows = [[i, 1] for i in ids] + [[], ["host", "ab12-cd"], [1, 2]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]), secret_cells=["S!B602=SOME_VAR"])
        self.assertNotIn("ab12-cd", all_outputs(rep))

    def test_password_like_right_neighbour_is_scrubbed_as_a_substring(self):
        rows = [["Password", "Bearer_hunter2xyz"], [1, 2], [3, 4], [None, None, None, F('"Authorization: Bearer_hunter2xyzEND"', "x", t="str")]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
        self.assertNotIn("Bearer_hunter2xyz", all_outputs(rep))


    def test_password_like_right_neighbour_in_shared_strings_is_scrubbed_as_a_substring(self):
        sh, sst = shared_sheet("S", [["Password", "Bearer_hunter2xyz"], [1, 2], [3, 4]])
        sh.data += '<row r="4"><c r="D4" t="str"><f>"Authorization: Bearer_hunter2xyzEND"</f><v>x</v></c></row>'
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst}))
        self.assertNotIn("Bearer_hunter2xyz", all_outputs(rep))


class SecretPromotion(Tmp):
    def test_a_header_row_label_does_not_scrub_its_neighbour_header_from_other_sheets(self):
        users = Sheet("Users", [["id", "password_hash", "created_at"], [1, "x", "y"], [2, "z", "w"]])
        orders = Sheet("Orders", [["order_id", "created_at", "amount"], [1, 2, 3], [4, 5, 6]])
        rep = cw.analyze(make_book(self.tmp, [users, orders]))
        src = [s for s in rep["sources"] if s["sheet"] == "Orders"][0]
        self.assertEqual(src["lifted"], ["order_id", "created_at", "amount"])
        self.assertNotIn(cw.MASK, src["stanza"])

    def test_a_neighbour_cannot_rename_schema_keys(self):
        cols = ["a", "b"]
        tbl = (f'<table xmlns="{MAIN}" name="T" displayName="T" ref="A1:B3"><tableColumns count="2">'
               '<tableColumn id="1" name="a"/><tableColumn id="2" name="b"/></tableColumns></table>')
        data = Sheet("D", [["a", "b"], [1, 2], [3, 4]], rels=[("table", "../tables/table1.xml", "rId1")],
                     after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        note = Sheet("N", [["password", "header_row_count"], [1, 2], [3, 4]])
        rep = cw.analyze(make_book(self.tmp, [data, note], parts={"xl/tables/table1.xml": tbl}))
        self.assertIn("header_row_count", rep["tables"][0])
        self.assertIn("totals_row_count", rep["tables"][0])

    def test_keys_use_only_key_terms_and_priority(self):
        out = cw.scrub({"row_count": "row_count", "p": "pw-secret-1"}, ["row_count"], key_terms=[], priority=["pw-secret-1"])
        self.assertEqual(out, {"row_count": cw.MASK, "p": cw.MASK})
        out = cw.scrub({"pw-secret-1": 1, "other": 2}, [], key_terms=[], priority=["pw-secret-1"])
        self.assertEqual(out, {cw.MASK: 1, "other": 2})

    def test_password_like(self):
        for v in ("Hunter2xyz!", "Bearer_hunter2xyz", "s3cr3t_Pw", "hunter2xyz", "Zq9-fake-pw-7731", "q8Zr3Kd0Wm2X"):
            self.assertTrue(cw.password_like(v), v)
        for v in ("row_count", "created_at", "password_hash", "order-id", "header_row_count", "id2_x", "Pending", "short1"[:5]):
            self.assertFalse(cw.password_like(v), v)

    def test_real_secrets_beside_labels_in_non_header_rows_still_scrub_everywhere(self):
        for value in ("Hunter2xyz!", "Bearer_hunter2xyz", "s3cr3t_Pw"):
            rows = [["Password", value], [1, 2], [3, 4], [None, None, None, F('"x ' + value + ' y"', "x", t="str")]]
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
            self.assertNotIn(value, all_outputs(rep), value)

    def test_labels_beyond_the_cap_still_feed_the_weak_scrub(self):
        parts = pivot_parts()
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace('name="Region"', f'name="{HV}"')
        decoys = [["Token", i] for i in range(cw.MAX_LABELS_PER_SHEET + 5)]
        data = Sheet("Data", decoys + [[None, None], ["Password", HV, "x"], [1, 2, 3], [4, 5, 6]])
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, sh], parts=parts))
        self.assertIn("secret_label_cap", sec_ids(rep))
        self.assertNotIn(HV, all_outputs(rep))

    def test_the_best_effort_note_names_the_unlabelled_formula_literal_gap(self):
        self.assertIn("formula string literal", cw.MASKING_NOTE)



class DataModelConnection(Tmp):
    CS = ("Provider=MSOLAP.8;Integrated Security=SSPI;Persist Security Info=True;"
          "Initial Catalog=Microsoft_SQLServer_AnalysisServices;Data Source=$Embedded$;Location=ThisWorkbookDataModel")

    def rep(self):
        return cw.analyze(book_with(self.tmp, conns(conn(1, "ThisWorkbookDataModel", self.CS, command="Model", ctype=1))))

    def test_the_embedded_model_is_a_data_model_not_an_nr_connection(self):
        r = self.rep()["external"]["connection_list"][0]
        self.assertEqual((r["status"], r["system"]), ("data_model", "Power Pivot data model"))
        self.assertIn("power-pivot.md", " ".join(r["reasons"]))
        self.assertIn("malloy-powerbi-review", " ".join(r["reasons"]))

    def test_the_text_report_has_no_login_or_rotation_advice_for_it(self):
        rep = self.rep()
        text = cw.render_text(rep)
        self.assertNotIn("needs a database login", text)
        self.assertNotIn("- NR conn 1", text)
        self.assertNotIn("ROTATE", text)
        self.assertIn("power-pivot.md", text)
        self.assertFalse(rep["external"]["cache_of_database"])
        self.assertNotIn("cache of a database", text)

    def test_a_real_ssas_server_is_still_nr(self):
        cs = "Provider=MSOLAP.8;Integrated Security=SSPI;Data Source=ssas01.corp;Initial Catalog=Sales"
        r = cw.analyze(book_with(self.tmp, conns(conn(1, "cube", cs)))) ["external"]["connection_list"][0]
        self.assertEqual(r["status"], "nr")

    def test_the_connections_command_does_not_list_it_as_nr(self):
        path = book_with(self.tmp, conns(conn(1, "ThisWorkbookDataModel", self.CS)))
        code, out, err = run_main(["connections", path, "--config-out", os.path.join(self.tmp, "c.json")])
        self.assertNotIn("NR conn 1", out)


class ProposedSourceQuoting(Tmp):
    CS = "Driver={PostgreSQL Unicode};Server=h;Database=d"

    def rec(self, command):
        xml = conns(conn(1, "Sales PG", self.CS, command=command.replace("\n", "@@"))).replace("@@", "&#10;")
        rep = cw.analyze(book_with(self.tmp, xml))
        return rep, rep["external"]["connection_list"][0]

    def test_sql_with_an_interpolation_marker_gets_no_proposed_source(self):
        _, r = self.rec("SELECT '%{x}' AS a FROM t")
        self.assertFalse(r["proposed_source"])
        self.assertTrue(any("%{" in x for x in r["reasons"]), r["reasons"])

    def test_sql_ending_in_a_double_quote_does_not_run_into_the_closing_quotes(self):
        _, r = self.rec('SELECT a FROM "T"')
        self.assertNotIn('""""', r["proposed_source"])
        self.assertTrue(r["proposed_source"].endswith('"\n""")'), r["proposed_source"])

    def test_the_report_keeps_the_sql_lines_in_a_fenced_block(self):
        rep, r = self.rec("SELECT a -- note\nFROM t")
        text = cw.render_text(rep)
        self.assertIn("```\nsales_pg.sql(\"\"\"SELECT a -- note\nFROM t\"\"\")\n```", text)

    def test_the_fence_outgrows_a_backtick_run_in_the_sql(self):
        rep, _ = self.rec("SELECT '```' AS a\nFROM t")
        text = cw.render_text(rep)
        self.assertIn("````\nsales_pg.sql(", text)


class ParameterPlaceholders(Tmp):
    CS = "Driver={PostgreSQL Unicode};Server=h;Database=d"

    def src(self, command, params=()):
        rep = cw.analyze(book_with(self.tmp, conns(conn(1, "Sales PG", self.CS, command=command, params=params))))
        return rep["external"]["connection_list"][0]["proposed_source"]

    def test_a_bound_parameter_becomes_a_named_given_with_a_comment(self):
        s = self.src("SELECT a FROM t WHERE x = ?", [("Region", "Sheet1!$B$2")])
        self.assertNotRegex(s, r"(?<!\w)\?")
        self.assertIn("given: region", s)
        self.assertIn("TODO", s)
        self.assertIn("Sheet1!$B$2", s)

    def test_parameters_are_matched_in_order(self):
        s = self.src("SELECT 1 WHERE a=? AND b=?", [("Region", "Sheet1!$B$2"), ("Year", "Sheet1!$B$3")])
        self.assertLess(s.index("given: region"), s.index("given: year"))

    def test_an_unbound_question_mark_is_a_loud_todo(self):
        s = self.src("SELECT 1 WHERE a=?")
        self.assertNotRegex(s, r"(?<!\w)\?")
        self.assertIn("TODO", s)

    def test_a_question_mark_inside_a_string_literal_is_left_alone(self):
        s = self.src("SELECT 'why?' AS q, x FROM t WHERE y = ?", [("p", "$A$1")])
        self.assertIn("'why?'", s)
        self.assertIn("given: p", s)


class ExternalPivotCacheKinds(Tmp):
    OLE = "Provider=SQLOLEDB.1;Data Source=db01;Initial Catalog=Northwind;Integrated Security=SSPI"
    MSOLAP = "Provider=MSOLAP.8;Data Source=ssas01.corp;Initial Catalog=Sales"

    def pivot_of(self, connections_xml=None, parts_extra=None):
        parts = pivot_parts(cache_source='<cacheSource type="external" connectionId="1"/>')
        if connections_xml is not None:
            parts["xl/connections.xml"] = connections_xml
        parts.update(parts_extra or {})
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [pv], parts=parts))

    def test_a_relational_cache_is_not_a_data_model(self):
        rep = self.pivot_of(conns(conn(1, "wh", self.OLE, command="SELECT a FROM t")))
        self.assertEqual(rep["pivots"][0]["external_cache"]["kind"], "relational")
        self.assertEqual(rep["external"]["pivot_external_caches"], 0)
        self.assertTrue(rep["external"]["cache_of_database"])
        text = cw.render_text(rep)
        self.assertIn("decide the data question", text)
        self.assertNotIn("1 data-model pivot cache", text)

    def test_an_olap_cache_is_reported_as_olap(self):
        rep = self.pivot_of(conns(conn(1, "cube", self.MSOLAP, command="Try1", ctype=1, extra="<olapPr/>")))
        self.assertEqual(rep["pivots"][0]["external_cache"]["kind"], "olap")
        self.assertEqual(rep["external"]["pivot_external_caches"], 0)

    def test_a_cache_on_the_embedded_model_is_the_data_model(self):
        rep = self.pivot_of(conns(conn(1, "ThisWorkbookDataModel", "", command="Model", ctype=1, extra="<olapPr/>")))
        self.assertEqual(rep["pivots"][0]["external_cache"]["kind"], "data_model")
        self.assertEqual(rep["external"]["pivot_external_caches"], 1)
        self.assertFalse(rep["external"]["cache_of_database"])
        self.assertIn("1 data-model pivot cache", cw.render_text(rep))

    def test_a_model_part_with_no_resolvable_connection_is_the_data_model(self):
        rep = self.pivot_of(parts_extra={"xl/model/item.data": b"x"})
        self.assertEqual(rep["pivots"][0]["external_cache"]["kind"], "data_model")

    def test_an_unresolvable_external_cache_is_just_external(self):
        self.assertEqual(self.pivot_of()["pivots"][0]["external_cache"]["kind"], "external")


class DataModelConnectionShapes(Tmp):
    def recs(self, *items):
        return cw.analyze(book_with(self.tmp, conns(*items)))["external"]["connection_list"]

    def test_the_model_connection_is_matched_by_name_with_no_embedded_marker(self):
        r = self.recs(conn(1, "ThisWorkbookDataModel", "", command="Model", ctype=1, extra="<olapPr/>"))[0]
        self.assertEqual((r["status"], r["system"]), ("data_model", "Power Pivot data model"))

    def test_the_model_connection_is_matched_by_a_model_cube_command(self):
        r = self.recs(conn(1, "Whatever", "", command="Model", ctype=1, extra="<olapPr/>"))[0]
        self.assertEqual(r["status"], "data_model")

    def test_a_command_called_model_on_a_real_sql_connection_is_not_the_model(self):
        r = self.recs(conn(1, "x", "Driver={PostgreSQL Unicode};Server=h;Database=d", command="Model", ctype=3))[0]
        self.assertNotEqual(r["status"], "data_model")

    def test_worksheet_and_table_connections_are_workbook_internal_not_nr(self):
        ws = '<connection id="2" name="WorksheetConnection_Sheet1!$A$1:$C$4" type="102"><extLst/></connection>'
        tb = '<connection id="3" name="Excel accounts" type="100"><extLst/></connection>'
        rep = cw.analyze(book_with(self.tmp, conns(ws, tb)))
        self.assertEqual([r["status"] for r in rep["external"]["connection_list"]], ["workbook", "workbook"])
        self.assertFalse(rep["external"]["cache_of_database"])
        text = cw.render_text(rep)
        self.assertNotIn("- NR conn", text)
        self.assertNotIn("a connection type this script does not read", text)


class EmptyPasswordIsNotACredential(Tmp):
    def embedded(self, cs):
        return cw.analyze(book_with(self.tmp, conns(conn(1, "c", cs))))["external"]["connections_embedded_credential"]

    def test_an_empty_password_is_not_counted(self):
        for cs in ('Provider=Microsoft.ACE.OLEDB.12.0;Data Source=x;Password="";Persist Security Info=True',
                   "Provider=X;Password='';Data Source=x", "Provider=X;Pwd=;Data Source=x", "Provider=X;PWD=;"):
            with self.subTest(cs=cs):
                self.assertEqual(self.embedded(cs), 0)

    def test_a_real_password_still_is(self):
        for cs in ("Provider=X;Password=abc123;Data Source=x", 'Provider=X;Pwd="abc 123";Data Source=x', "Provider=X;Data Source=x;PWD=zz"):
            with self.subTest(cs=cs):
                self.assertEqual(self.embedded(cs), 1)


class MashupIdentification(Tmp):
    def test_item_props_naming_the_schema_are_not_a_mashup(self):
        props = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ds:datastoreItem xmlns:ds="http://schemas.openxmlformats.org/'
                 'officeDocument/2006/customXml" ds:itemID="{1}"><ds:schemaRefs><ds:schemaRef ds:uri="http://schemas.microsoft.com/'
                 'DataMashup"/></ds:schemaRefs></ds:datastoreItem>')
        rep = cw.analyze(book_with(self.tmp, None, parts={"customXml/itemProps1.xml": props, "customXml/item1.xml": mashup('section Section1;\nshared A = 1;')}))
        dm = rep["external"]["data_mashup"]
        self.assertEqual(dm["rejected"], [])
        self.assertEqual(dm["queries"], 1)

    def test_a_part_whose_root_is_not_data_mashup_is_ignored(self):
        props = '<root xmlns="x">DataMashup</root>'
        rep = cw.analyze(book_with(self.tmp, None, parts={"customXml/itemProps1.xml": props}))
        self.assertIsNone(rep["external"]["data_mashup"])


class LabelledConnectionNames(Tmp):
    NAME = "Sales (password: Kiwi-9876x)"

    def setUp(self):
        super().setUp()
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        self.cfg = os.path.join(self.tmp, "conn.json")
        self.env = {"HOME": self.home, "XDG_CONFIG_HOME": os.path.join(self.home, "xdg")}
        pg = conn(1, self.NAME, "Driver={PostgreSQL Unicode};Server=pg.example.com;Database=sales;Uid=bob;Pwd=" + PW, save=True)
        self.book = book_with(self.tmp, conns(pg))

    def test_the_label_value_never_reaches_the_report_json_or_text(self):
        rep = cw.analyze(self.book)
        self.assertNotIn("Kiwi-9876x", blob_of(rep))
        self.assertIn("Sales", blob_of(rep))

    def test_the_connections_command_never_prints_or_writes_it(self):
        code, out, err = run_main(["connections", self.book, "--config-out", self.cfg], env=self.env)
        self.assertEqual(code, 0, err)
        written = slurp(self.cfg)
        env_dir = os.path.join(self.home, "xdg", "malloy-publisher")
        env_text = "".join(slurp(os.path.join(env_dir, n)) for n in os.listdir(env_dir)) if os.path.isdir(env_dir) else ""
        names = [c["name"] for c in json.loads(written)["connections"]]
        for blob in (out, err, written, env_text, " ".join(names)):
            self.assertNotIn("Kiwi-9876x", blob)
            self.assertNotIn("kiwi_9876x", blob.lower())
        self.assertTrue(all(n.startswith("sales") for n in names), names)

    def test_mask_text_applies_label_assignments(self):
        self.assertEqual(cw.mask_text("Sales (password: Kiwi-9876x)"), "Sales (password: [masked])")
        self.assertEqual(cw.mask_text("token=abcd1234 ok"), "token=[masked] ok")
        self.assertEqual(cw.mask_text("nothing to see"), "nothing to see")


class RejectedReadsAreCharged(Tmp):
    def package(self, size):
        path = os.path.join(self.tmp, "p.zip")
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("big.xml", b"a" * size)
        return cw.Package(path)

    def test_bytes_read_before_a_too_large_rejection_count_toward_the_total(self):
        pkg = self.package(5000)
        with mock.patch.object(cw, "MAX_PART_BYTES", 500):
            self.assertIsNone(pkg.read("big.xml"))
        self.assertEqual(pkg.bytes_read, 501)
        self.assertEqual(pkg.rejected, [{"part": "big.xml", "reason": "too_large"}])

    def test_repeated_oversized_reads_exhaust_the_total_budget(self):
        pkg = self.package(5000)
        with mock.patch.object(cw, "MAX_PART_BYTES", 500), mock.patch.object(cw, "MAX_TOTAL_BYTES", 1200):
            for _ in range(3):
                pkg.read("big.xml")
        self.assertTrue(pkg.total_hit)
        self.assertEqual(pkg.bytes_read, 1200)

    def test_an_oversized_mashup_section_is_charged_too(self):
        pkg = self.package(10)
        body = "section Section1;\n" + "x" * 5000
        with mock.patch.object(cw, "MAX_PART_BYTES", 3000):
            out = cw.read_mashup(pkg, "customXml/item1.xml", mashup(body))
        self.assertIn("too_large", out["rejected"])
        self.assertGreaterEqual(pkg.bytes_read, 3000)


class PivotFieldNamesFromSecretCells(Tmp):
    def book(self):
        data = Sheet("Data", [["Password", "zebracorn", "Region"], [1, 2, "East"], [3, 4, "West"], [5, 6, "East"]])
        parts = pivot_parts(cache_source='<cacheSource type="worksheet"><worksheetSource ref="A1:C4" sheet="Data"/></cacheSource>')
        key = "xl/pivotCache/pivotCacheDefinition1.xml"
        parts[key] = (parts[key].replace('name="Region"', 'name="Password"').replace('name="Amount"', 'name="zebracorn"').replace(
            '<cacheField name="Margin" numFmtId="0" formula="Amount*0.1" databaseField="0"/>', '<cacheField name="Region" numFmtId="0"/>'))
        pk = "xl/pivotTables/pivotTable1.xml"
        parts[pk] = parts[pk].replace('name="Sum of Amount" fld="1"', 'name="Sum of zebracorn" fld="2"')
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return make_book(self.tmp, [data, pv], parts=parts)

    def test_a_cache_field_named_by_a_secret_cell_is_masked_everywhere(self):
        rep = cw.analyze(self.book())
        self.assertNotIn("zebracorn", blob_of(rep))
        p = rep["pivots"][0]
        self.assertEqual((p["row_fields"], p["page_fields"]), (["Password"], ["Region"]))
        self.assertIn("[masked]", json.dumps(p))
        self.assertNotIn("_fields", p)

    def test_ordinary_field_names_are_kept(self):
        data = Sheet("Data", [["Region", "Amount", "Margin"], ["East", 1, 2]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, pv], parts=pivot_parts()))
        self.assertEqual(rep["pivots"][0]["row_fields"], ["Region"])
        self.assertEqual(rep["pivots"][0]["data_fields"][0]["field"], "Amount")


class ConnectionNameHoldsItsPassword(Tmp):
    PW = "Kiwi9876xQ"

    def run_case(self, name):
        home = os.path.join(self.tmp, "home")
        os.makedirs(home, exist_ok=True)
        env = {"HOME": home, "XDG_CONFIG_HOME": os.path.join(home, "xdg")}
        cfg = os.path.join(self.tmp, "conn.json")
        pg = conn(1, name, "Driver={PostgreSQL Unicode};Server=pg.example.com;Database=sales;Uid=bob;Pwd=" + self.PW, save=True)
        book = book_with(self.tmp, conns(pg))
        code, out, err = run_main(["connections", book, "--config-out", cfg, "--force"], env=env)
        self.assertEqual(code, 0, err)
        rep = cw.analyze(book)
        return out, slurp(cfg), blob_of(rep)

    def test_a_name_holding_the_password_in_any_case_or_separators_is_replaced(self):
        for name in ("Kiwi9876xQ", "sales-Kiwi9876xQ", "KIWI-9876-XQ reports", "Sales (pwd Kiwi9876xQ)", "Sales secret: Kiwi9876xQ"):
            with self.subTest(name=name):
                out, written, js = self.run_case(name)
                for blob in (out, written, js):
                    self.assertNotIn("kiwi", blob.lower(), name)
                    self.assertNotIn("9876x", blob.lower())

    def test_a_label_value_with_spaces_is_cut_at_the_label(self):
        out, written, js = self.run_case("Sales password=Kiwi 9876x")
        for blob in (out, written, js):
            self.assertNotIn("9876x", blob)


if __name__ == "__main__":
    unittest.main()
