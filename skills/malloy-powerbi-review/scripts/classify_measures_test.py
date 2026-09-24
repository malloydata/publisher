#!/usr/bin/env python3
"""The lexing and typing mistakes that put wrong counts in this skill twice.

Every case below reproduces a defect that shipped. They are all the same shape:
the router matched raw text where it needed to parse, so a measure was filed
under a heading that changed the size of a migration estimate. None of them
errored - a wrong number that looks like a number survives every check except
this one."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import classify_measures as cm  # noqa: E402


def measure(name, dax, table="T", hidden=False):
    return {"table": table, "name": name, "dax": dax, "hidden": hidden,
            "displayFolder": ""}


NO_FLAGS = {"bidirectional": set(), "many_to_many": set(), "inactive": set()}


class FunctionLexing(unittest.TestCase):
    def test_a_column_named_calculated_is_not_a_CALCULATE_call(self):
        # `SUM('Operation'[CPUTime (calculated)])` inflated the CALCULATE count
        # from 50 measures to 55 and the occurrence count well past that.
        fns = cm.function_names("SUM ( 'Operation'[CPUTime (calculated)] )")
        self.assertNotIn("CALCULATE", fns)
        self.assertIn("SUM", fns)

    def test_CALCULATETABLE_is_not_CALCULATE(self):
        fns = cm.function_names("CALCULATETABLE ( GROUPBY ( T ), ALLSELECTED ( T ) )")
        self.assertNotIn("CALCULATE", fns)
        self.assertIn("CALCULATETABLE", fns)

    def test_a_quoted_table_name_contributes_no_functions(self):
        fns = cm.function_names("MAX ( 'Top N Selector'[Value] )")
        self.assertEqual(set(fns), {"MAX"})

    def test_an_escaped_bracket_does_not_truncate_the_identifier(self):
        # `]]` is a literal `]`. Stopping at the first one read the column as
        # `Status`, so the measure typed as numeric and left the report layer.
        self.assertEqual(cm.column_refs("SELECTEDVALUE ( T[Status]] Label] )"),
                         {("T", "Status] Label")})

    def test_an_escaped_bracket_still_hides_a_function_lookalike(self):
        fns = cm.function_names('CALCULATE ( SUM ( T[Amt]] calculated] ), T[c] = "v" )')
        self.assertEqual(sorted(fns), ["CALCULATE", "SUM"])

    def test_occurrences_are_counted_not_just_measures(self):
        fns = cm.function_names("CALCULATE ( CALCULATE ( SUM ( T[a] ) ) )")
        self.assertEqual(fns["CALCULATE"], 2)


class CommentStripping(unittest.TestCase):
    def test_dax_double_dash_is_a_comment(self):
        # Real models carry superseded measures commented out. Missing `--`
        # read a commented-out sentence's quoted "0" as a live string literal,
        # and filed a plain numeric card measure as report-layer.
        live = cm.strip_comments('-- shows a "0" instead of blank\nSUM(T[a])')
        self.assertNotIn('"0"', live)
        self.assertIn("SUM(T[a])", live)

    def test_double_slash_is_also_a_comment(self):
        self.assertNotIn("QueryEnd", cm.strip_comments('// was "QueryEnd"\nSUM(T[a])'))

    def test_offsets_are_preserved_so_positions_stay_valid(self):
        dax = '-- note\nSUM(T[a])'
        self.assertEqual(len(cm.strip_comments(dax)), len(dax))


class LabelTyping(unittest.TestCase):
    """Step 1 types the return value. Looking for a quote character misses the
    skill's own canonical report-layer example."""

    def test_selectedvalue_over_a_text_column_is_a_label_with_no_literal(self):
        m = measure("Selected page", "SELECTEDVALUE('Current page'[Current page])")
        coltypes = {("Current page", "Current page"): "string"}
        self.assertTrue(cm.returns_string(m, coltypes, {}))

    def test_a_passthrough_over_a_numeric_column_is_not_a_label(self):
        # MAX('Top N Selector'[Value]) sits in a ranking measure beside several
        # text columns. Scanning every column in the body instead of the ones
        # MAX is called on made 8 numeric ranking measures read as labels.
        m = measure("Rank", "IF ( ISINSCOPE ( Op[EventText] ), "
                            "RANKX ( T, [X] ) <= MAX ( 'Top N Selector'[Value] ) )")
        coltypes = {("Op", "EventText"): "string", ("Top N Selector", "Value"): "int64"}
        self.assertFalse(cm.returns_string(m, coltypes, {}))

    def test_a_literal_compared_against_is_not_a_label(self):
        m = measure("Shipped", 'CALCULATE ( [Total], T[Status] = "Shipped" )')
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_an_IN_set_of_literals_is_a_comparison(self):
        # Only the FIRST member of `IN ({"a","b"})` follows a `{`; the second
        # follows a comma, which read as a value position.
        m = measure("SE", 'CALCULATE ( [X], T[Op] IN ({"VertiPaqSEQueryEnd", "DirectQueryEnd"}) )')
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_a_literal_in_a_value_position_is_a_label(self):
        m = measure("Caption", 'IF ( ISFILTERED ( T[a] ), "Drill through", "Pick one" )')
        self.assertTrue(cm.returns_string(m, {}, {}))

    def test_double_ampersand_is_logical_and_not_concatenation(self):
        m = measure("Both", "IF ( ISFILTERED ( T[a] ) && HASONEFILTER ( T[a] ), 1, 0 )")
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_single_ampersand_is_concatenation(self):
        m = measure("Title", "MIN ( T[a] ) & MIN ( T[b] )")
        self.assertTrue(cm.returns_string(m, {}, {}))

    def test_arithmetic_on_two_label_measures_is_still_a_number(self):
        # `[A] - [B]` is numeric whatever A and B are. Propagating label-ness
        # through the subtraction filed three real measures as report-layer.
        label = measure("Status", 'SELECTEDVALUE ( T[Status] )')
        diff = measure("Delta", "[Status] - [Status]")
        by_name = {m["name"]: m for m in (label, diff)}
        coltypes = {("T", "Status"): "string"}
        self.assertTrue(cm.returns_string(label, coltypes, by_name))
        self.assertFalse(cm.returns_string(diff, coltypes, by_name))

    def test_plus_zero_is_the_coerce_to_number_idiom(self):
        m = measure("Flag", "SELECTEDVALUE ( T[IsOn] ) + 0")
        self.assertFalse(cm.returns_string(m, {("T", "IsOn"): "string"}, {}))

    def test_an_ampersand_inside_a_column_name_is_not_concatenation(self):
        # `'Invoices'[Taxes & Commercial Fees]` read as string concatenation and
        # skipped a plain SUMX revenue measure.
        m = measure("Net", "SUMX ( 'Invoices', 'Invoices'[Taxes & Commercial Fees] )")
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_a_label_propagates_to_its_callers(self):
        page = measure("Selected page", "SELECTEDVALUE('P'[Page])", table="P")
        button = measure("Button", "[Selected page]", table="P")
        by_name = {"Selected page": page}
        self.assertTrue(cm.returns_string(button, {("P", "Page"): "string"}, by_name))


class Routing(unittest.TestCase):
    def test_calculate_without_keepfilters_routes_to_the_divergent_recipe(self):
        m = measure("Bikes", 'CALCULATE ( [Total], P[Category] = "Bikes" )')
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertIn("FC1", routes)

    def test_keepfilters_is_not_divergent(self):
        m = measure("Bikes", 'CALCULATE ( [Total], KEEPFILTERS ( P[Category] = "Bikes" ) )')
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertNotIn("FC1", routes)

    def test_allselected_is_its_own_recipe_not_untranslatable(self):
        m = measure("Pct", "DIVIDE ( [Total], CALCULATE ( [Total], ALLSELECTED () ) )")
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertIn("FC3", routes)

    def test_rankx_over_allselected_with_a_slicer_is_the_top_n_shape(self):
        m = measure("Rank", "RANKX ( CALCULATETABLE ( GROUPBY ( T ), ALLSELECTED ( T ) ), [X] ) "
                            "<= MAX ( 'Top N Selector'[Value] )")
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertIn("FC6", routes)

    def test_crossfilter_is_caught_though_relationships_tmdl_is_clean(self):
        # CROSSFILTER turns a relationship bidirectional for one measure, so it
        # leaves no trace in relationships.tmdl at all.
        m = measure("Hits", "CALCULATE ( [Q], CROSSFILTER ( A[k], B[k], BOTH ) )")
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertIn("S3", routes)

    def test_bidirectional_flag_routes_a_measure_whose_dax_says_nothing(self):
        m = measure("Plain", "SUM ( ExecutionMetrics[cpu] )")
        flags = dict(NO_FLAGS, bidirectional={"ExecutionMetrics"})
        routes, _ = cm.local_routes(m, {}, flags)
        self.assertIn("S3", routes)

    def test_an_inactive_relationship_alone_does_not_route_a_measure(self):
        # It is inert until USERELATIONSHIP activates it. Routing on proximity put
        # 97 of one real model's 298 recipe measures on S1 that did not belong.
        flags = dict(NO_FLAGS, inactive={"Sales"})
        m = measure("Total", "SUM ( Sales[Amount] )", table="Sales")
        results, _ = cm.classify([m], {}, flags)
        self.assertEqual(results[("Sales", "Total")]["routes"], ["DIRECT"])

    def test_userelationship_on_the_same_model_still_routes(self):
        flags = dict(NO_FLAGS, inactive={"Sales"})
        m = measure("By due date",
                    "CALCULATE ( SUM ( Sales[Amount] ), USERELATIONSHIP ( Sales[DueKey], "
                    "'Date'[Key] ) )", table="Sales")
        results, _ = cm.classify([m], {}, flags)
        self.assertIn("S1", results[("Sales", "By due date")]["routes"])

    def test_a_plain_aggregate_falls_through_to_direct(self):
        m = measure("Total", "SUM ( T[amount] )")
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertEqual(routes, [])

    def test_userelationship_does_not_stall(self):
        # It neither widens nor narrows, which used to leave it unrouted.
        m = measure("By Due", "CALCULATE ( SUM ( S[amt] ), USERELATIONSHIP ( S[d], D[k] ) )")
        routes, _ = cm.local_routes(m, {}, NO_FLAGS)
        self.assertIn("S1", routes)


class Propagation(unittest.TestCase):
    def test_a_direct_wrapper_around_a_divergent_leaf_is_divergent(self):
        leaf = measure("Query - failure", 'CALCULATE ( [Q], Op[StatusCode] <> 0 )')
        wrapper = measure("Query failure(card)", "[Query - failure]+0")
        results, _ = cm.classify([leaf, wrapper], {}, NO_FLAGS)
        self.assertIn("FC1", results[("T", "Query failure(card)")]["routes"])

    def test_a_cycle_does_not_hang(self):
        a = measure("A", "[B]")
        b = measure("B", "[A]")
        results, _ = cm.classify([a, b], {}, NO_FLAGS)
        self.assertEqual(results[("T", "A")]["routes"], ["DIRECT"])
        self.assertEqual(results[("T", "B")]["routes"], ["DIRECT"])


class TmdlParsing(unittest.TestCase):
    """The parser everything else is downstream of. A body it cuts short is a
    measure routed on half its DAX, and it reports no error when it does."""

    def parse(self, body, filename="Sales.tmdl"):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
            return cm.parse_table_file(path)

    def test_a_fenced_body_runs_to_the_closing_fence(self):
        table, measures, _ = self.parse(
            "table Sales\n"
            "\tmeasure 'Total Sales' = ```\n"
            "\t\t\tCALCULATE (\n"
            "\t\t\t    SUM ( Sales[Amount] )\n"
            "\t\t\t)\n"
            "\t\t\t```\n"
            "\t\tformatString: 0.00\n"
        )
        self.assertEqual(table, "Sales")
        self.assertEqual(len(measures), 1)
        self.assertEqual(measures[0]["name"], "Total Sales")
        self.assertIn("CALCULATE", cm.function_names(measures[0]["dax"]))
        self.assertNotIn("```", measures[0]["dax"])
        self.assertNotIn("formatString", measures[0]["dax"])

    def test_an_unfenced_body_survives_a_blank_line_inside_it(self):
        # Real exports put a blank line mid-expression. Ending the body there
        # dropped the ALLSELECTED half of the measure and routed it direct.
        _, measures, _ = self.parse(
            "table Sales\n"
            "\tmeasure 'Pct of visible' =\n"
            "\t\t\tDIVIDE (\n"
            "\n"
            "\t\t\t    [Total Sales],\n"
            "\t\t\t    CALCULATE ( [Total Sales], ALLSELECTED () )\n"
            "\t\t\t)\n"
            "\t\tlineageTag: abc\n"
            "\t\tisHidden\n"
        )
        self.assertEqual(len(measures), 1)
        self.assertIn("ALLSELECTED", cm.function_names(measures[0]["dax"]))
        self.assertTrue(measures[0]["hidden"])

    def test_an_inline_first_line_keeps_its_continuation(self):
        _, measures, _ = self.parse(
            "table Sales\n"
            "\tmeasure Margin = DIVIDE (\n"
            "\t\t\t    [Profit],\n"
            "\t\t\t    [Total Sales]\n"
            "\t\t\t)\n"
            "\t\tdisplayFolder: KPIs\n"
        )
        self.assertEqual(len(measures), 1)
        self.assertEqual(measures[0]["displayFolder"], "KPIs")
        self.assertEqual(cm.measure_refs(measures[0]["dax"]),
                         {"Profit", "Total Sales"})

    def test_column_datatypes_are_read_for_the_label_test(self):
        _, _, columns = self.parse(
            "table Sales\n"
            "\tcolumn 'Order Status'\n"
            "\t\tdataType: string\n"
            "\t\tsummarizeBy: none\n"
            "\n"
            "\tcolumn Amount\n"
            "\t\tdataType: double\n"
        )
        self.assertEqual(columns, {"Order Status": "string", "Amount": "double"})

    def test_a_calculation_item_is_parsed_like_a_measure(self):
        # A calculation group lives in tables/*.tmdl but declares `calculationItem`,
        # not `measure`. Matching only `measure` read a 7-group model as having none.
        table, measures, _ = self.parse(
            "table 'Z04CG1 - Time Intelligence'\n"
            "\tisHidden\n"
            "\n"
            "\tcalculationGroup\n"
            "\t\tprecedence: 4\n"
            "\n"
            "\t\tcalculationItem Daily =\n"
            "\t\t\t\tSELECTEDMEASURE()\n"
            "\n"
            "\t\tcalculationItem MTD = ```\n"
            "\t\t\t\tCALCULATE ( SELECTEDMEASURE (), DATESMTD ('Date'[Date]) )\n"
            "\t\t\t\t```\n",
            filename="Z04CG1 - Time Intelligence.tmdl",
        )
        self.assertEqual(table, "Z04CG1 - Time Intelligence")
        self.assertEqual([m["name"] for m in measures], ["Daily", "MTD"])
        self.assertTrue(all(m["kind"] == "calculation_item" for m in measures))

    def test_a_calculation_item_routes_to_the_stopgap(self):
        item = {"table": "CG", "name": "Constant", "kind": "calculation_item",
                "dax": "1", "hidden": False, "displayFolder": ""}
        results, _ = cm.classify([item], {}, NO_FLAGS)
        # No SELECTEDMEASURE in the body, but it is still a calculation group.
        self.assertIn("S5", results[("CG", "Constant")]["routes"])

    def test_the_declared_table_name_beats_the_filename(self):
        table, measures, _ = self.parse(
            "table 'Top N Selector'\n\tmeasure X = 1\n",
            filename="Top N Selector.tmdl",
        )
        self.assertEqual(table, "Top N Selector")
        self.assertEqual(measures[0]["table"], "Top N Selector")


class FunctionsFile(unittest.TestCase):
    """`definition/functions.tmdl` holds user-defined DAX. The loader never opened
    it, so a measure calling a helper was routed on a body it could not see."""

    def parse(self, body):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "functions.tmdl")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
            return cm.parse_functions_file(path)

    def test_a_fenced_function_body_is_read(self):
        fns = self.parse(
            "/// A doc comment\n"
            "function 'DaxLib.Format.Percent' = ```\n"
            "\t\t(value: DOUBLE) =>\n"
            "\t\tCALCULATE ( SUM ( Sales[Amount] ), ALLSELECTED () )\n"
            "\t\t```\n"
        )
        self.assertEqual([f["name"] for f in fns], ["DaxLib.Format.Percent"])
        self.assertEqual(fns[0]["kind"], "function")
        self.assertIn("ALLSELECTED", cm.function_names(fns[0]["dax"]))

    def test_an_unfenced_function_body_is_read(self):
        fns = self.parse(
            "function Helper =\n"
            "\t\t(x: INT64) =>\n"
            "\t\tLASTNONBLANK ( 'Date'[Date], 1 )\n"
        )
        self.assertEqual(len(fns), 1)
        self.assertIn("LASTNONBLANK", cm.function_names(fns[0]["dax"]))

    def test_a_missing_functions_file_is_not_an_error(self):
        self.assertEqual(cm.parse_functions_file("/nonexistent/functions.tmdl"), [])


class Relationships(unittest.TestCase):
    def test_absent_properties_read_as_the_default_not_as_missing(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "relationships.tmdl")
            with open(p, "w") as fh:
                fh.write("relationship abc\n\tfromColumn: A.k\n\ttoColumn: B.k\n")
            flags, note = cm.parse_relationships(p)
        self.assertEqual(flags["bidirectional"], set())
        self.assertEqual(note, "")

    def test_bothdirections_names_both_end_tables(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "relationships.tmdl")
            with open(p, "w") as fh:
                fh.write("relationship abc\n\tcrossFilteringBehavior: bothDirections\n"
                         "\tfromColumn: EventText.XmlaRequestId\n"
                         "\ttoColumn: ExecutionMetrics.XmlaRequestId\n")
            flags, _ = cm.parse_relationships(p)
        self.assertEqual(flags["bidirectional"], {"EventText", "ExecutionMetrics"})

    def test_a_missing_file_says_so_rather_than_reporting_a_clean_model(self):
        flags, note = cm.parse_relationships("/nonexistent/relationships.tmdl")
        self.assertTrue(note)
        self.assertEqual(flags["bidirectional"], set())


if __name__ == "__main__":
    unittest.main()
