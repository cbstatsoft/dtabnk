"""Regression tests for dtabnk panel merge semantics.

Run with: python -m pytest -q
Dependencies: polars, psutil, pytest. Tests do not read Excel or export files.
"""
import pytest
import polars as pl

from dtabnk import PanelMerger, build_parser, validate_args


def make_panel(countries, years, values, column="Value"):
    return pl.DataFrame({"Country": countries, "Year": years, column: values})


def test_outer_merge_keeps_all_keys_and_names_generic_values():
    merger = PanelMerger(keys=["Country", "Year"])
    first = merger.add(None, make_panel(["A", "B"], [2020, 2020], [1.0, 2.0]), "gdp.csv")
    merged = merger.add(first, make_panel(["B", "C"], [2020, 2021], [3.0, 4.0]), "life_expectancy.csv")
    assert merged.height == 3
    assert set(merged.columns) == {"Country", "Year", "gdp", "life_expectancy"}
    rows = {r["Country"]: r for r in merged.to_dicts()}
    assert rows["A"]["life_expectancy"] is None
    assert rows["B"]["life_expectancy"] == 3
    assert rows["C"]["gdp"] is None


@pytest.mark.parametrize("how,expected", [("inner", ["B"]), ("left", ["A", "B"]), ("outer", ["A", "B", "C"])])
def test_merge_modes(how, expected):
    merger = PanelMerger(keys=["Country", "Year"], how=how)
    first = merger.add(None, make_panel(["A", "B"], [2020, 2020], [1, 2]), "left.csv")
    merged = merger.add(first, make_panel(["B", "C"], [2020, 2020], [3, 4]), "right.csv")
    assert sorted(merged["Country"].to_list()) == expected


def test_duplicate_keys_rejected_before_join():
    merger = PanelMerger(keys=["Country", "Year"])
    with pytest.raises(ValueError, match="duplicate merge keys"):
        merger.add(None, make_panel(["A", "A"], [2020, 2020], [1, 2]), "duplicate.csv")


def test_blank_and_null_keys_rejected():
    merger = PanelMerger(keys=["Country", "Year"])
    for country in [" ", None]:
        with pytest.raises(ValueError, match="missing or blank merge keys"):
            merger.add(None, make_panel([country], [2020], [1]), "bad.csv")


def test_merge_normalises_trimmed_entities_and_year_types():
    merger = PanelMerger(keys=["Country", "Year"], how="inner")
    first = merger.add(None, make_panel([" A "], [2020], [1]), "a.csv")
    merged = merger.add(first, make_panel(["A"], ["2020"], [2]), "b.csv")
    assert merged.height == 1
    assert merged["Country"].to_list() == ["A"]
    assert merged["Year"].to_list() == [2020]


def test_collision_suffix_and_strict_rejection():
    first = make_panel(["A"], [2020], [1], "GDP")
    second = make_panel(["A"], [2020], [2], "GDP")
    permissive = PanelMerger(keys=["Country", "Year"])
    base = permissive.add(None, first, "one.csv")
    merged = permissive.add(base, second, "two.csv")
    assert "GDP" in merged.columns
    assert "GDP_two" in merged.columns

    strict = PanelMerger(keys=["Country", "Year"], conflicts="error")
    base = strict.add(None, first, "one.csv")
    with pytest.raises(ValueError, match="conflicts"):
        strict.add(base, second, "two.csv")


def test_merge_cli_validates_file_and_output_count():
    parser = build_parser()
    good = parser.parse_args(["a.csv", "b.csv", "--merge", "--out", "combined"])
    validate_args(good)
    assert good.merge_how == "outer"
    bad = parser.parse_args(["a.csv", "--merge"])
    with pytest.raises(SystemExit, match="at least two"):
        validate_args(bad)
    bad_out = parser.parse_args(["a.csv", "b.csv", "--merge", "--out", "a", "b"])
    with pytest.raises(SystemExit, match="exactly one"):
        validate_args(bad_out)
