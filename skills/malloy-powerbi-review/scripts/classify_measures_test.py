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
        self.assertEqual(len(results), 2)


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
