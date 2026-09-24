#!/usr/bin/env python3
"""The lexing and typing mistakes that put wrong counts in this skill twice.

Every case below reproduces a defect that shipped. They are all the same shape:
the router matched raw text where it needed to parse, so a measure was filed
under a heading that changed the size of a migration estimate. None of them
errored - a wrong number that looks like a number survives every check except
this one."""
import ast
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import classify_measures as cm  # noqa: E402


def measure(name, dax, table="T", hidden=False, kind="measure"):
    return {"table": table, "name": name, "dax": dax, "hidden": hidden,
            "kind": kind, "displayFolder": ""}


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


class ReturnTyping(unittest.TestCase):
    """DAX types on the RETURN. Typing the whole body instead let a VAR that the
    RETURN only *compared against* decide the measure's type, which produced
    three classes of false report-layer finding at once."""

    def test_a_var_holding_a_label_for_a_comparison_is_not_the_return_type(self):
        m = measure("Filtered rows",
                    'VAR _state = "IsFiltered"\n'
                    'VAR _flag = SELECTEDVALUE ( T[Mode] )\n'
                    'RETURN COUNTROWS ( FILTER ( T, _flag = _state ) )')
        self.assertFalse(cm.returns_string(m, {("T", "Mode"): "string"}, {}))

    def test_a_numeric_outermost_call_ends_the_question(self):
        # `SUMX(VALUES(T[TextCol]), …)` typed as a label off the iterated table,
        # not off its value: three of one Microsoft model's seven report-layer
        # measures were this shape, each carrying `formatString: #,0`.
        m = measure("Column Count",
                    'SUMX ( VALUES ( P[TableName] ), CALCULATE ( MAX ( P[ObjectCount] ) ) )')
        self.assertFalse(cm.returns_string(
            m, {("P", "TableName"): "string", ("P", "ObjectCount"): "int64"}, {}))

    def test_a_var_the_return_does_reach_still_counts(self):
        m = measure("Caption",
                    'VAR _t = FORMAT ( [X], "0.0%" )\n'
                    'RETURN IF ( ISFILTERED ( T[a] ), _t, BLANK () )')
        self.assertTrue(cm.returns_string(m, {}, {}))

    def test_a_var_the_return_never_reaches_is_dropped(self):
        m = measure("Ratio",
                    'VAR _unused = FORMAT ( [X], "0.0%" )\n'
                    'VAR _n = COUNTROWS ( T )\n'
                    'RETURN DIVIDE ( _n, 100 )')
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_a_var_name_that_also_spells_a_column_is_not_substituted(self):
        # Substituting on the raw text rewrote `T[Total]` into `T[(…)]`.
        expr = cm.return_expr('VAR Total = 1\nRETURN SUM ( T[Total] )')
        self.assertIn("T[Total]", expr)

    def test_a_body_with_no_return_is_typed_whole(self):
        m = measure("Title", 'SELECTEDVALUE ( T[Page] )')
        self.assertTrue(cm.returns_string(m, {("T", "Page"): "string"}, {}))

    def test_calculate_is_unwrapped_rather_than_read_as_numeric(self):
        # CALCULATE returns its first argument's type, so treating it as numeric
        # would skip every label measure wrapped in one.
        m = measure("Mode", 'CALCULATE ( SELECTEDVALUE ( T[Mode] ) )')
        self.assertTrue(cm.returns_string(m, {("T", "Mode"): "string"}, {}))

    def test_a_self_referencing_var_does_not_hang(self):
        # Illegal DAX, but a parse of a truncated file produces it.
        cm.return_expr("VAR a = b\nVAR b = a\nRETURN a")


class ValuePositionTyping(unittest.TestCase):
    """Four defects a 50-model corpus run turned up, all the same shape: a
    string that is not a value, or a value position the typing never found."""

    def test_a_column_name_argument_is_not_a_value(self):
        # `ADDCOLUMNS(t, "InTop10", …)` names a column. Reading it as a value
        # filed 15 ordinary numeric measures across the corpus as labels -
        # streak counters, monthly revenue averages, a sales rank. The outer
        # call here is MINX, which is not numeric by itself, so nothing earlier
        # in the typing rescues it.
        m = measure("Streak start",
                    'MINX ( FILTER ( ADDCOLUMNS ( ALL ( D[Year] ), "InTop10", '
                    'IF ( [Rank] <= 10, 1, 0 ) ), [InTop10] = 1 ), [Year] )')
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_an_iterator_table_argument_is_not_the_return_type(self):
        # `AVERAGEX(KEEPFILTERS(VALUES(T[GradeCode])), DIVIDE(…))` names a text
        # column to say which table to walk, not what comes out.
        m = measure("Participation rate",
                    'IF ( [Total] = 0, 0, AVERAGEX ( KEEPFILTERS ( VALUES ( '
                    'G[GradeCode] ) ), DIVIDE ( [Part], [Total] ) ) )')
        self.assertFalse(cm.returns_string(m, {("G", "GradeCode"): "string"}, {}))

    def test_a_filter_argument_is_not_the_return_type_either(self):
        m = measure("Longest gap",
                    "CALCULATE ( MAX ( C[Work Days] ), "
                    "TREATAS ( VALUES ( L[List ID] ), C[List ID] ) )")
        self.assertFalse(cm.returns_string(
            m, {("L", "List ID"): "string", ("C", "Work Days"): "int64"}, {}))

    def test_both_branches_numeric_makes_the_measure_numeric(self):
        # The condition can be full of string work; only the branches type it.
        m = measure("In range",
                    'IF ( FORMAT ( MIN ( O[Start] ), "hh:mm:ss" ) >= [From], 1, 2 )')
        self.assertFalse(cm.returns_string(m, {}, {}))

    def test_a_comparison_inside_an_argument_does_not_make_it_a_number(self):
        # Scanning the whole expression for an operator, rather than its top
        # level, made every SVG sparkline in a Microsoft model numeric.
        m = measure("Sparkline",
                    '"<svg>" & IF ( [Max] > 0, "#649398", "#D9655D" ) & "</svg>"')
        self.assertTrue(cm.returns_string(m, {}, {}))

    def test_a_comment_marker_inside_a_string_literal_is_not_a_comment(self):
        # The `//` in an SVG's namespace URL blanked the rest of the line and
        # left the literal unterminated, desynchronising every `"` after it.
        live = cm.strip_comments("\"<svg xmlns='http://www.w3.org/2000/svg'>\" & X")
        self.assertIn("2000/svg", live)
        self.assertTrue(live.rstrip().endswith("& X"))

    def test_a_real_comment_outside_a_literal_is_still_stripped(self):
        live = cm.strip_comments('SUM ( T[a] ) // was "QueryEnd"\n-- and this')
        self.assertNotIn("QueryEnd", live)
        self.assertNotIn("and this", live)
        self.assertIn("SUM ( T[a] )", live)

    def test_an_escaped_quote_does_not_end_the_literal_early(self):
        live = cm.strip_comments('"say ""hi"" // not a comment" & X')
        self.assertIn("not a comment", live)

    def test_a_measure_named_var_does_not_truncate_the_binding(self):
        # `[Var EBITDA vs Budget %]` read as a VAR declaration and cut the
        # binding before it in half.
        expr = cm.return_expr('VAR Pct = [Var EBITDA vs Budget %]\n'
                              'RETURN IF ( ISBLANK ( Pct ), 0, Pct )')
        self.assertIn("Var EBITDA vs Budget %", expr)


class KeywordPrecededRefs(unittest.TestCase):
    def test_a_measure_ref_after_a_dax_keyword_stays_in_the_graph(self):
        # `AND [Gross]` read as a column of a table named `AND`, so the edge
        # vanished and divergence stopped propagating across it.
        self.assertEqual(cm.measure_refs("IF ( [Net] > 0 AND [Gross] > 0, 1, 0 )"),
                         {"Net", "Gross"})

    def test_else_or_and_then_are_all_the_same_shape(self):
        self.assertEqual(cm.measure_refs("SWITCH ( TRUE (), x, [A], [B] )"), {"A", "B"})
        self.assertIn("C", cm.measure_refs("IF ( p, 1 ) ELSE [C]"))

    def test_an_adjacent_table_qualifier_is_still_a_column(self):
        self.assertEqual(cm.measure_refs("SUM ( Sales[Amount] )"), set())
        self.assertEqual(cm.measure_refs("SUM ( 'My Sales'[Amount] )"), set())

    def test_divergence_propagates_across_a_keyword_preceded_ref(self):
        leaf = measure("Shipped", 'CALCULATE ( [Total], T[Status] = "Shipped" )')
        caller = measure("Both", "IF ( [Total] > 0 AND [Shipped] > 0, [Shipped], 0 )")
        results, _ = cm.classify([leaf, caller], {}, NO_FLAGS)
        self.assertIn("FC1", results[("T", "Both")]["routes"])


class CalculatedColumns(unittest.TestCase):
    """`column X = <DAX>` matched nothing, so the DAX was never routed and the
    block's dataType never reached the column types the label test reads."""

    def parse(self, body, filename="Sales.tmdl"):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
            return cm.parse_table_file(path)

    def test_a_calculated_column_is_read_as_a_definition(self):
        _, defs, _ = self.parse(
            "table Sales\n"
            "\tcolumn 'Year Month' = FORMAT ( Sales[Date], \"YYYY-MM\" )\n"
            "\t\tdataType: string\n"
            "\t\tlineageTag: abc\n"
        )
        self.assertEqual([(d["kind"], d["name"]) for d in defs],
                         [("calculated_column", "Year Month")])
        self.assertIn("FORMAT", cm.function_names(defs[0]["dax"]))

    def test_its_datatype_reaches_the_column_types(self):
        _, _, cols = self.parse(
            "table Sales\n"
            "\tcolumn Bucket = IF ( Sales[Amt] > 100, \"Big\", \"Small\" )\n"
            "\t\tdataType: string\n"
            "\n"
            "\tcolumn Amt\n"
            "\t\tdataType: double\n"
        )
        self.assertEqual(cols, {"Bucket": "string", "Amt": "double"})

    def test_a_multi_line_calculated_column_body_is_not_cut_short(self):
        _, defs, _ = self.parse(
            "table 'Calendar'\n"
            "\tcolumn 'Week of' =\n"
            "\t\t\tVAR _w = 'Calendar'[Week]\n"
            "\t\t\tRETURN CALCULATE ( MIN ( 'Calendar'[Date] ), ALL ( 'Calendar' ) )\n"
            "\t\tsummarizeBy: none\n"
        )
        self.assertIn("CALCULATE", cm.function_names(defs[0]["dax"]))
        self.assertNotIn("summarizeBy", defs[0]["dax"])

    def test_a_string_calculated_column_is_a_dimension_not_report_layer(self):
        # The label test asks whether a *measure* is canvas furniture. A text
        # calculated column is an ordinary dimension.
        col = measure("Bucket", 'IF ( T[Amt] > 100, "Big", "Small" )', kind="calculated_column")
        results, _ = cm.classify([col], {}, NO_FLAGS)
        self.assertNotEqual(results[("T", "Bucket")]["routes"], ["SKIP"])


class CalculatedTablePartitions(unittest.TestCase):
    """`CALENDAR()` and `GENERATESERIES()` live in a calculated table's
    partition, not in any measure body. With no partition branch, T6 and S4 were
    reachable in code and unreachable in practice."""

    def parse(self, body, filename="Calendar.tmdl"):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, filename)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
            return cm.parse_table_file(path)

    def test_an_inline_calculated_source_routes_to_the_date_spine_recipe(self):
        _, defs, _ = self.parse(
            "table Calendar\n"
            "\tpartition Calendar = calculated\n"
            "\t\tmode: import\n"
            "\t\tsource = CALENDAR ( MIN ( Cards[Start] ), MAX ( Cards[Due] ) )\n"
        )
        self.assertEqual([d["kind"] for d in defs], ["calculated_table"])
        self.assertIn("T6", cm.local_routes(defs[0], {}, NO_FLAGS)[0])

    def test_an_indented_calculated_source_is_read(self):
        _, defs, _ = self.parse(
            "table TopN\n"
            "\tpartition TopN = calculated\n"
            "\t\tmode: import\n"
            "\t\tsource =\n"
            "\t\t\t\tGENERATESERIES ( 1, 20, 1 )\n"
            "\n"
            "\tannotation PBI_Id = abc\n",
            filename="TopN.tmdl",
        )
        self.assertIn("S4", cm.local_routes(defs[0], {}, NO_FLAGS)[0])

    def test_a_power_query_partition_is_not_read_as_dax(self):
        # `= m` is Power Query, `= entity` a Direct Lake binding. Routing either
        # as DAX reports recipes for a language the recipes do not cover.
        _, defs, _ = self.parse(
            "table Brand\n"
            "\tpartition Brand = m\n"
            "\t\tmode: import\n"
            "\t\tsource =\n"
            "\t\t\t\tlet\n"
            "\t\t\t\t    Source = Csv.Document ( File.Contents ( \"b.csv\" ) ),\n"
            "\t\t\t\t    column Year = 1\n"
            "\t\t\t\tin\n"
            "\t\t\t\t    Source\n",
            filename="Brand.tmdl",
        )
        self.assertEqual(defs, [])


class Roles(unittest.TestCase):
    """`definition/roles/*.tmdl` was never opened, so a model whose only
    USERPRINCIPALNAME lives in a role reported no row-level security at all."""

    def write(self, name, body):
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "roles"))
        with open(os.path.join(d, "roles", name), "w", encoding="utf-8") as fh:
            fh.write(body)
        return d

    ROLE = (
        "role 'Account Managers'\n"
        "\tmodelPermission: read\n"
        "\n"
        "\ttablePermission Customers =\n"
        "\t\t\tVAR _me =\n"
        "\t\t\t    SELECTCOLUMNS (\n"
        "\t\t\t        FILTER ( 'Employees', 'Employees'[Email] = USERPRINCIPALNAME () ),\n"
        "\t\t\t        \"@Name\", 'Employees'[Name]\n"
        "\t\t\t    )\n"
        "\t\t\tRETURN\n"
        "\t\t\t    'Customers'[Account Manager] IN _me\n"
        "\n"
        "\tannotation PBI_Id = abc\n"
    )

    def test_a_role_predicate_is_read_and_routed(self):
        defn = self.write("Account Managers.tmdl", self.ROLE)
        roles = cm.parse_roles_dir(defn)
        self.assertEqual(len(roles), 1)
        self.assertEqual(roles[0]["kind"], "role_permission")
        self.assertEqual(roles[0]["name"], "Customers")
        self.assertIn("(role) Account Managers", roles[0]["table"])
        self.assertIn("USERPRINCIPALNAME", cm.function_names(roles[0]["dax"]))
        self.assertIn("RLS", cm.local_routes(roles[0], {}, NO_FLAGS)[0])

    def test_the_annotation_after_it_is_not_swallowed_into_the_predicate(self):
        defn = self.write("Account Managers.tmdl", self.ROLE)
        self.assertNotIn("PBI_Id", cm.parse_roles_dir(defn)[0]["dax"])

    def test_a_role_predicate_is_never_filed_as_report_layer(self):
        # It returns a boolean. Running the label test on it filed the model's
        # security posture under "returns a label, not a number".
        defn = self.write("Account Managers.tmdl", self.ROLE)
        roles = cm.parse_roles_dir(defn)
        results, _ = cm.classify(roles, {}, NO_FLAGS)
        routes = results[(roles[0]["table"], "Customers")]["routes"]
        self.assertNotIn("SKIP", routes)
        self.assertIn("RLS", routes)

    def test_a_measure_reading_the_caller_identity_also_routes_to_rls(self):
        m = measure("Mine", "CALCULATE ( [Total], T[Owner] = USERPRINCIPALNAME () )")
        self.assertIn("RLS", cm.local_routes(m, {}, NO_FLAGS)[0])

    def test_a_model_with_no_roles_directory_is_not_an_error(self):
        self.assertEqual(cm.parse_roles_dir(tempfile.mkdtemp()), [])


class FormatStringDefinition(unittest.TestCase):
    def test_a_dynamic_format_string_does_not_become_property_keys(self):
        # It sits at base + 1 with the properties, but is a DAX block. The
        # property loop turned it, and every line of its body, into a key.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "Shipments.tmdl")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(
                    "table Shipments\n"
                    "\tmeasure 'Gross Profit' = SUMX ( Shipments, Shipments[Net] )\n"
                    "\t\tlineageTag: abc\n"
                    "\n"
                    "\t\tformatStringDefinition =\n"
                    "\t\t\t\tVAR _v = SELECTEDMEASURE ()\n"
                    "\t\t\t\tRETURN IF ( _v > 1000, \"#,0,\\\"k\\\"\", \"#,0\" )\n"
                    "\n"
                    "\t\tdisplayFolder: KPIs\n"
                )
            _, defs, _ = cm.parse_table_file(path)
        self.assertEqual(len(defs), 1)
        self.assertEqual(defs[0]["dax"], "SUMX ( Shipments, Shipments[Net] )")
        self.assertIn("SELECTEDMEASURE", defs[0].get("formatStringDefinition", ""))
        # The property after the block is still reached.
        self.assertEqual(defs[0]["displayFolder"], "KPIs")


class AutoDateTables(unittest.TestCase):
    def test_auto_date_tables_are_skipped_and_counted_at_the_model_level(self):
        with tempfile.TemporaryDirectory() as d:
            tables = os.path.join(d, "tables")
            os.makedirs(tables)
            with open(os.path.join(tables, "Sales.tmdl"), "w", encoding="utf-8") as fh:
                fh.write("table Sales\n\tmeasure Total = SUM ( Sales[Amt] )\n")
            for guid in ("abc", "def"):
                fn = f"LocalDateTable_{guid}.tmdl"
                with open(os.path.join(tables, fn), "w", encoding="utf-8") as fh:
                    fh.write(f"table LocalDateTable_{guid}\n"
                             f"\tcolumn Year = YEAR ( [Date] )\n\t\tdataType: int64\n")
            measures, _coltypes, flags, _note = cm.load_tmdl(d)
        self.assertEqual([m["name"] for m in measures], ["Total"])
        self.assertEqual(len(flags["auto_date"]), 2)

    def test_the_report_names_s7_as_a_model_level_route(self):
        m = measure("Total", "SUM ( T[a] )")
        results, _ = cm.classify([m], {}, NO_FLAGS)
        text = cm.report_text(results, "", "demo",
                              dict(NO_FLAGS, auto_date=["LocalDateTable_abc"]))
        self.assertIn("S7", text)
        self.assertIn("1 auto date table", text)


class KindsAreReportedSeparately(unittest.TestCase):
    def test_the_headline_counts_measures_and_nothing_else(self):
        # Appending user-defined functions to the measure list and printing the
        # sum published 1,622 measures for a corpus that held 1,406.
        defs = [
            measure("Total", "SUM ( T[a] )"),
            measure("Helper", "(x: INT64) => x + 1", table="(functions)", kind="function"),
            measure("MTD", "SELECTEDMEASURE ()", table="CG", kind="calculation_item"),
            measure("Bucket", "1", kind="calculated_column"),
        ]
        results, _ = cm.classify(defs, {}, NO_FLAGS)
        text = cm.report_text(results, "", "demo", NO_FLAGS)
        self.assertIn("# demo: 1 measures", text)
        self.assertIn("| measures | 1 |", text)
        self.assertIn("| user-defined functions | 1 |", text)
        self.assertIn("| calculation items | 1 |", text)
        self.assertIn("| calculated columns | 1 |", text)
        self.assertIn("4 definitions in all", text)


class NoVacuousZeros(unittest.TestCase):
    """`T4` was declared, counted as a stopgap, and reported as firing zero times
    across 1,622 measures - a zero guaranteed by construction, because no code
    path emitted it. A route with no emitter has to be declared teaching-only."""

    def emitted_routes(self):
        src = pathlib.Path(cm.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        # The route tables name every key; only other positions are emitters.
        declarations = {"RECIPES", "TEACHING_ONLY", "DIVERGENT_ROUTES",
                        "STOPGAP_ROUTES", "KIND_LABELS"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id in declarations for t in node.targets):
                node.value = ast.Constant(value=None)
        return {n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and n.value in cm.RECIPES}

    def test_every_recipe_is_emitted_or_declared_teaching_only(self):
        unreachable = set(cm.RECIPES) - self.emitted_routes() - cm.TEACHING_ONLY
        self.assertEqual(unreachable, set(),
                         f"declared but never emitted: {sorted(unreachable)}")

    def test_a_teaching_only_route_is_not_counted_as_divergent_or_stopgap(self):
        # Counting one guarantees a zero in a published total.
        self.assertEqual(cm.TEACHING_ONLY & (cm.DIVERGENT_ROUTES | cm.STOPGAP_ROUTES),
                         set())


if __name__ == "__main__":
    unittest.main()
