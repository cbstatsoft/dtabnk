#!/usr/bin/env python3
# wb_data_converter.py
# (C) Connor Baird 2026
# GNU GPL-3.0-or-later

from __future__ import annotations

import argparse
import csv
import gc
import math
import os
import re
import subprocess
import sys
from typing import Iterable

REQ = ["polars", "pyreadstat", "rpy2", "openpyxl", "fastexcel", "psutil"]

DEFAULT_MIN_FREE_RAM_MB = 512
DEFAULT_PREVIEW_ROWS = 10
DEFAULT_STREAMING_CHUNK_SIZE = 10_000

YEAR_ALIASES = ["Year", "year", "Time", "time", "Date", "date", "Period", "period"]
VALUE_ALIASES = [
    "Value",
    "value",
    "OBS_VALUE",
    "obs_value",
    "Observation_Value",
    "observation_value",
]
SERIES_ALIASES = [
    "Series_Name",
    "Series",
    "Indicator_Name",
    "Indicator",
    "series_name",
    "series",
    "indicator_name",
    "indicator",
]

LICENSE_TEXT = """GNU General Public Licence v3.0 or later

This programme is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public Licence as published by the Free Software
Foundation, either version 3 of the Licence, or (at your option) any later version.

This programme is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public Licence for more details.

You should have received a copy of the GNU General Public Licence along with this
programme. If not, see <https://www.gnu.org/licenses/>.

(C) Connor Baird 2026
"""


def ensure_dependencies() -> None:
    missing: list[str] = []
    for package in REQ:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)

    if not missing:
        return

    print(f"Installing missing packages: {', '.join(missing)}...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    except subprocess.CalledProcessError as exc:
        sys.exit(f"Failed to install dependencies: {exc}")


ensure_dependencies()

import polars as pl
import psutil
import pyreadstat
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

try:
    pl.Config.set_streaming_chunk_size(DEFAULT_STREAMING_CHUNK_SIZE)
except Exception:
    pass


def get_available_ram_mb() -> int:
    mem = psutil.virtual_memory()
    return max(1, int(mem.available // (1024 * 1024)))


def derive_memory_policy(
    file_size_bytes: int,
    lazy_thresh_mb: int | None = None,
    parquet_thresh_mb: int | None = None,
    safe_mode: bool = False,
) -> dict[str, int | bool]:
    avail_mb = get_available_ram_mb()
    file_mb = max(1, math.ceil(file_size_bytes / (1024 * 1024)))

    dynamic_lazy_mb = lazy_thresh_mb if lazy_thresh_mb is not None else max(
        32, int(avail_mb * (0.05 if safe_mode else 0.10))
    )
    dynamic_parquet_mb = parquet_thresh_mb if parquet_thresh_mb is not None else max(
        64, int(avail_mb * (0.10 if safe_mode else 0.25))
    )

    use_lazy = file_mb >= dynamic_lazy_mb
    use_parquet = file_mb >= dynamic_parquet_mb or avail_mb < (3072 if safe_mode else 2048)

    return {
        "avail_mb": avail_mb,
        "file_mb": file_mb,
        "lazy_thresh_mb": dynamic_lazy_mb,
        "parquet_thresh_mb": dynamic_parquet_mb,
        "use_lazy": use_lazy,
        "use_parquet": use_parquet,
    }


def ensure_memory_headroom(
    stage: str,
    input_size_bytes: int,
    multiplier: float,
    minimum_free_mb: int = DEFAULT_MIN_FREE_RAM_MB,
    safe_mode: bool = False,
) -> None:
    avail_mb = get_available_ram_mb()
    reserve_mb = max(minimum_free_mb, 1024 if safe_mode else minimum_free_mb)
    needed_mb = max(1, int(math.ceil((input_size_bytes * multiplier) / (1024 * 1024))))

    if avail_mb - needed_mb < reserve_mb:
        raise MemoryError(
            f"Refusing {stage}: available RAM ~{avail_mb} MB, "
            f"estimated need ~{needed_mb} MB, reserve floor {reserve_mb} MB."
        )


def find_header_row(path: str, delimiter: str = ",") -> int:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            for i, row in enumerate(reader):
                cleaned = [cell.strip() for cell in row]
                if not cleaned or not any(cleaned):
                    continue
                if len(cleaned) < 2:
                    continue

                first = cleaned[0].replace(".", "").replace("-", "")
                has_text = any(any(ch.isalnum() for ch in cell) for cell in cleaned)
                if not first.isdigit() and has_text:
                    return i
    except Exception:
        pass

    return 0


def get_skip_rows(path: str, delimiter: str, header_row_override: int | None) -> int:
    if header_row_override is not None:
        return max(0, header_row_override)
    return find_header_row(path, delimiter=delimiter)


def strip_bottom_metadata(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    first_col = df.columns[0]
    return df.filter(
        ~pl.col(first_col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.contains(r"(?i)^(Data from database:|Last Updated:)")
    )


def sanitise(columns: Iterable[str], max_len: int = 64) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    for raw in columns:
        col = str(raw).strip().replace(" ", "_").replace("%", "pct")
        col = "".join(ch for ch in col if ch.isalnum() or ch == "_")

        if not col:
            col = "v"
        if not col[0].isalpha():
            col = f"v_{col}"

        col = col[:max_len].rstrip("_") or "v"
        base = col
        i = 1
        while col in seen:
            suffix = f"_{i}"
            col = f"{base[: max_len - len(suffix)]}{suffix}".rstrip("_")
            i += 1

        seen.add(col)
        out.append(col)

    return out


def sanitise_one(name: str) -> str:
    return sanitise([name])[0]


def is_year_like(name: str) -> bool:
    return re.search(r"(19|20)\d{2}", str(name)) is not None


def collect_with_engine(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)
    except Exception:
        return lf.collect()


def read_excel_compat(path: str) -> pl.DataFrame:
    errors: list[str] = []

    for engine in ("calamine", "fastexcel", "openpyxl"):
        try:
            return pl.read_excel(path, engine=engine)
        except Exception as exc:
            errors.append(f"{engine}: {exc}")

    try:
        return pl.read_excel(path)
    except Exception as exc:
        errors.append(f"default: {exc}")

    raise RuntimeError("Failed to read Excel file. " + " | ".join(errors[-3:]))


def read_source(
    path: str,
    lazy_thresh_mb: int | None,
    parquet_thresh_mb: int | None,
    safe_mode: bool,
    delimiter: str,
    header_row_override: int | None,
) -> tuple[pl.DataFrame, dict[str, int | bool]]:
    ext = os.path.splitext(path)[1].lower()
    file_size = os.path.getsize(path)
    skip_rows = get_skip_rows(path, delimiter, header_row_override) if ext == ".csv" else 0

    policy = derive_memory_policy(
        file_size_bytes=file_size,
        lazy_thresh_mb=lazy_thresh_mb,
        parquet_thresh_mb=parquet_thresh_mb,
        safe_mode=safe_mode,
    )

    print(
        f"Available RAM: {policy['avail_mb']} MB | "
        f"File: {policy['file_mb']} MB | "
        f"Lazy threshold: {policy['lazy_thresh_mb']} MB | "
        f"Parquet threshold: {policy['parquet_thresh_mb']} MB"
    )

    use_lazy = bool(policy["use_lazy"])
    use_parquet = bool(policy["use_parquet"])
    temp_parquet_path: str | None = None

    try:
        if ext == ".csv" and use_parquet:
            print(
                f"Large file ({file_size / 1024 / 1024:.1f} MB). "
                "Using streaming CSV -> Parquet intermediate..."
            )

            temp_parquet_path = f"{path}.parquet.tmp"

            try:
                lf = pl.scan_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=delimiter,
                    low_memory=True,
                )
                lf.sink_parquet(temp_parquet_path, compression="zstd")
                df = pl.read_parquet(temp_parquet_path)
                print("Parquet intermediate conversion complete.")
                return strip_bottom_metadata(df), policy
            except Exception as exc:
                print(f"Streaming Parquet intermediate failed: {exc}. Falling back.")

        if use_parquet:
            print(
                f"Large file ({file_size / 1024 / 1024:.1f} MB). "
                "Using Parquet intermediate for efficiency..."
            )

            ensure_memory_headroom(
                stage="source read before Parquet intermediate",
                input_size_bytes=file_size,
                multiplier=1.2 if ext == ".csv" else 2.0,
                minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
                safe_mode=safe_mode,
            )

            temp_parquet_path = f"{path}.parquet.tmp"

            if ext == ".csv":
                df_src = pl.read_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=delimiter,
                    low_memory=True,
                )
            elif ext in {".xlsx", ".xls"}:
                df_src = read_excel_compat(path)
            else:
                raise ValueError(f"Unsupported format: {ext}")

            df_src.write_parquet(temp_parquet_path, compression="zstd")
            del df_src
            gc.collect()

            df = pl.read_parquet(temp_parquet_path)
            print("Parquet intermediate conversion complete.")
            return strip_bottom_metadata(df), policy

        if ext == ".csv":
            if use_lazy:
                try:
                    lf = pl.scan_csv(
                        path,
                        skip_rows=skip_rows,
                        separator=delimiter,
                        low_memory=True,
                    )
                    df = collect_with_engine(lf)
                except Exception:
                    ensure_memory_headroom(
                        stage="eager CSV read fallback",
                        input_size_bytes=file_size,
                        multiplier=1.5,
                        minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
                        safe_mode=safe_mode,
                    )
                    df = pl.read_csv(
                        path,
                        skip_rows=skip_rows,
                        separator=delimiter,
                        low_memory=True,
                    )
            else:
                ensure_memory_headroom(
                    stage="eager CSV read",
                    input_size_bytes=file_size,
                    multiplier=1.5,
                    minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
                    safe_mode=safe_mode,
                )
                df = pl.read_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=delimiter,
                    low_memory=True,
                )
        elif ext in {".xlsx", ".xls"}:
            ensure_memory_headroom(
                stage="Excel read",
                input_size_bytes=file_size,
                multiplier=2.5 if safe_mode else 2.0,
                minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
                safe_mode=safe_mode,
            )
            df = read_excel_compat(path)
        else:
            raise ValueError(f"Unsupported format: {ext}")

        return strip_bottom_metadata(df), policy

    finally:
        if temp_parquet_path and os.path.exists(temp_parquet_path):
            try:
                os.remove(temp_parquet_path)
            except Exception:
                pass


def find_column_name(
    df: pl.DataFrame,
    candidates: list[str] | tuple[str, ...],
    exclude: set[str] | None = None,
) -> str | None:
    excluded = exclude or set()
    normalised = [sanitise_one(c) for c in candidates if c]

    for candidate in normalised:
        if candidate in df.columns and candidate not in excluded:
            return candidate

    lower_map = {col.lower(): col for col in df.columns if col not in excluded}
    for candidate in normalised:
        hit = lower_map.get(candidate.lower())
        if hit:
            return hit

    return None


def resolve_column_name(
    df: pl.DataFrame,
    requested: str | None = None,
    fallbacks: list[str] | tuple[str, ...] | None = None,
    exclude: set[str] | None = None,
    required: bool = True,
    label: str = "column",
) -> str | None:
    candidates: list[str] = []
    if requested:
        candidates.append(requested)
    if fallbacks:
        candidates.extend(fallbacks)

    hit = find_column_name(df, candidates, exclude=exclude)
    if hit is not None:
        if requested and hit != sanitise_one(requested):
            print(f"Info: Using '{hit}' for {label} '{requested}'.")
        return hit

    if not required:
        return None

    available = ", ".join(df.columns[:20])
    if len(df.columns) > 20:
        available += ", ..."
    requested_text = requested if requested else "/".join(candidates) if candidates else label
    raise ValueError(f"{label.capitalize()} '{requested_text}' not found. Available: {available}")


def resolve_id_column(
    df: pl.DataFrame,
    requested_id: str,
    exclude: set[str] | None = None,
) -> str:
    requested_norm = sanitise_one(requested_id)
    fallbacks: list[str] = []

    if requested_norm == "Country_Name":
        fallbacks = ["Country"]
    elif requested_norm == "Country":
        fallbacks = ["Country_Name"]

    hit = resolve_column_name(
        df,
        requested=requested_norm,
        fallbacks=fallbacks,
        exclude=exclude,
        required=False,
        label="ID column",
    )
    if hit:
        return hit

    excluded = exclude or set()
    for col in df.columns:
        if col in excluded:
            continue
        if col in {"Year", "Value", "Series", "Series_Name"}:
            continue
        if not is_year_like(col):
            print(f"Info: Using '{col}' as ID column.")
            return col

    available = ", ".join(df.columns[:20])
    raise ValueError(f"Unable to resolve ID column. Available: {available}")


def detect_layout(
    df: pl.DataFrame,
    requested_layout: str,
    year_col: str | None,
    value_col: str | None,
) -> str:
    if requested_layout != "auto":
        return requested_layout

    year_hit = find_column_name(df, [year_col] if year_col else YEAR_ALIASES)
    value_hit = find_column_name(df, [value_col] if value_col else VALUE_ALIASES)

    if year_hit and value_hit:
        return "long"

    if df.columns:
        first_col = df.columns[0].lower()
        year_aliases = {sanitise_one(x).lower() for x in YEAR_ALIASES}
        if first_col in year_aliases and len(df.columns) > 2:
            return "year_rows"

    return "wide"


def cast_year_and_value(df: pl.DataFrame, year_col: str, value_col: str = "Value") -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col(year_col)
            .cast(pl.Utf8, strict=False)
            .str.extract(r"(\d{4})")
            .cast(pl.Int32, strict=False)
            .alias(year_col),
            pl.when(pl.col(value_col).cast(pl.Utf8, strict=False) == "..")
            .then(None)
            .otherwise(pl.col(value_col))
            .cast(pl.Float64, strict=False)
            .alias(value_col),
        ]
    ).filter(pl.col(year_col).is_not_null())


def process_wide_layout(
    df: pl.DataFrame,
    file_size: int,
    id_var: str,
    series_col_arg: str | None,
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    drop_candidates = [c for c in ("Country_Code", "Series_Code") if c in df.columns]
    if drop_candidates:
        df = df.drop(drop_candidates)

    series_col = resolve_column_name(
        df,
        requested=series_col_arg,
        fallbacks=SERIES_ALIASES,
        required=False,
        label="series column",
    )

    exclude = {series_col} if series_col else set()
    actual_id_var = resolve_id_column(df, id_var, exclude=exclude)

    id_vars = [actual_id_var]
    if series_col:
        id_vars.append(series_col)

    value_vars = [c for c in df.columns if c not in id_vars and is_year_like(c)]
    if not value_vars:
        available = ", ".join(df.columns[:20])
        raise ValueError(f"No year-like value columns found to unpivot. Available columns: {available}")

    estimated_unpivot_multiplier = max(2.0, min(8.0, len(value_vars) / 4))
    ensure_memory_headroom(
        stage="unpivot",
        input_size_bytes=file_size,
        multiplier=estimated_unpivot_multiplier,
        minimum_free_mb=min_free_ram_mb,
        safe_mode=safe_mode,
    )

    df = df.unpivot(
        index=id_vars,
        on=value_vars,
        variable_name="Year",
        value_name="Value",
    )
    df = cast_year_and_value(df, year_col="Year", value_col="Value")

    if series_col and series_col in df.columns:
        if safe_mode and get_available_ram_mb() < max(2048, min_free_ram_mb * 2):
            raise MemoryError(
                "Refusing pivot in --safe-mode: insufficient RAM headroom for eager pivot."
            )

        ensure_memory_headroom(
            stage="pivot",
            input_size_bytes=max(file_size, df.estimated_size()),
            multiplier=3.5 if safe_mode else 3.0,
            minimum_free_mb=max(min_free_ram_mb, 1024),
            safe_mode=safe_mode,
        )

        df = df.rename({series_col: "Series"})
        df = df.pivot(
            values="Value",
            index=[actual_id_var, "Year"],
            on="Series",
            aggregate_function="mean",
        )
        df = df.rename(dict(zip(df.columns, sanitise(df.columns))))

    return df


def process_long_layout(
    df: pl.DataFrame,
    id_var: str,
    year_col_arg: str | None,
    value_col_arg: str | None,
    series_col_arg: str | None,
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    _ = safe_mode
    _ = min_free_ram_mb

    drop_candidates = [c for c in ("Country_Code", "Series_Code") if c in df.columns]
    if drop_candidates:
        df = df.drop(drop_candidates)

    year_col = resolve_column_name(
        df,
        requested=year_col_arg,
        fallbacks=YEAR_ALIASES,
        label="year column",
    )
    value_col = resolve_column_name(
        df,
        requested=value_col_arg,
        fallbacks=VALUE_ALIASES,
        exclude={year_col},
        label="value column",
    )
    series_col = resolve_column_name(
        df,
        requested=series_col_arg,
        fallbacks=SERIES_ALIASES,
        exclude={year_col, value_col},
        required=False,
        label="series column",
    )

    exclude = {year_col, value_col}
    if series_col:
        exclude.add(series_col)

    actual_id_var = resolve_id_column(df, id_var, exclude=exclude)

    keep_cols = [actual_id_var, year_col, value_col]
    if series_col:
        keep_cols.append(series_col)

    df = df.select(keep_cols).rename({year_col: "Year", value_col: "Value"})
    if series_col and series_col != "Series":
        df = df.rename({series_col: "Series"})

    df = cast_year_and_value(df, year_col="Year", value_col="Value")

    if "Series" in df.columns:
        ensure_memory_headroom(
            stage="pivot",
            input_size_bytes=max(df.estimated_size(), 1),
            multiplier=3.0 if safe_mode else 2.5,
            minimum_free_mb=max(min_free_ram_mb, 1024 if safe_mode else min_free_ram_mb),
            safe_mode=safe_mode,
        )
        df = df.pivot(
            values="Value",
            index=[actual_id_var, "Year"],
            on="Series",
            aggregate_function="mean",
        )
        df = df.rename(dict(zip(df.columns, sanitise(df.columns))))

    return df


def process_year_rows_layout(
    df: pl.DataFrame,
    file_size: int,
    id_var: str,
    year_col_arg: str | None,
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    year_col = resolve_column_name(
        df,
        requested=year_col_arg,
        fallbacks=YEAR_ALIASES,
        label="year column",
    )

    value_vars = [c for c in df.columns if c != year_col]
    if not value_vars:
        raise ValueError("No entity columns found in year_rows layout.")

    output_id_var = sanitise_one(id_var)

    estimated_unpivot_multiplier = max(2.0, min(8.0, len(value_vars) / 4))
    ensure_memory_headroom(
        stage="unpivot",
        input_size_bytes=file_size,
        multiplier=estimated_unpivot_multiplier,
        minimum_free_mb=min_free_ram_mb,
        safe_mode=safe_mode,
    )

    df = df.unpivot(
        index=[year_col],
        on=value_vars,
        variable_name=output_id_var,
        value_name="Value",
    )

    if year_col != "Year":
        df = df.rename({year_col: "Year"})

    df = cast_year_and_value(df, year_col="Year", value_col="Value")
    return df


def process_file(
    path: str,
    id_var: str,
    layout: str,
    year_col: str | None,
    value_col: str | None,
    series_col: str | None,
    lazy_thresh: int | None,
    parquet_thresh: int | None,
    min_free_ram_mb: int,
    safe_mode: bool,
    delimiter: str,
    header_row_override: int | None,
) -> pl.DataFrame:
    file_size = os.path.getsize(path)

    df, _policy = read_source(
        path=path,
        lazy_thresh_mb=lazy_thresh,
        parquet_thresh_mb=parquet_thresh,
        safe_mode=safe_mode,
        delimiter=delimiter,
        header_row_override=header_row_override,
    )

    original_cols = df.columns
    df = df.rename(dict(zip(original_cols, sanitise(original_cols))))

    chosen_layout = detect_layout(df, layout, year_col, value_col)
    print(f"Info: Using layout '{chosen_layout}'.")

    if chosen_layout == "wide":
        return process_wide_layout(
            df=df,
            file_size=file_size,
            id_var=id_var,
            series_col_arg=series_col,
            min_free_ram_mb=min_free_ram_mb,
            safe_mode=safe_mode,
        )

    if chosen_layout == "long":
        return process_long_layout(
            df=df,
            id_var=id_var,
            year_col_arg=year_col,
            value_col_arg=value_col,
            series_col_arg=series_col,
            min_free_ram_mb=min_free_ram_mb,
            safe_mode=safe_mode,
        )

    if chosen_layout == "year_rows":
        return process_year_rows_layout(
            df=df,
            file_size=file_size,
            id_var=id_var,
            year_col_arg=year_col,
            min_free_ram_mb=min_free_ram_mb,
            safe_mode=safe_mode,
        )

    raise ValueError(f"Unsupported layout: {chosen_layout}")


def prepare_export_df(df: pl.DataFrame) -> pl.DataFrame:
    export_df = df

    if "Country" in export_df.columns and "Country_Name" in export_df.columns:
        export_df = export_df.drop("Country_Name")
    elif "Country_Name" in export_df.columns:
        export_df = export_df.rename({"Country_Name": "Country"})

    if "Year" not in export_df.columns:
        for col in export_df.columns:
            if str(col).lower() in {"year", "time", "date"}:
                export_df = export_df.rename({col: "Year"})
                break

    preferred = [c for c in ("Country", "Year") if c in export_df.columns]
    rest = [c for c in export_df.columns if c not in preferred]
    if preferred:
        export_df = export_df.select(preferred + rest)

    return export_df


def normalise_time_column_name(pdf):
    for col in list(pdf.columns):
        lowered = str(col).lower()
        if lowered in {"year", "time", "date"}:
            if col != "Year":
                pdf = pdf.rename(columns={col: "Year"})
            return pdf
    return pdf


def preview_output(df: pl.DataFrame, rows: int = DEFAULT_PREVIEW_ROWS) -> None:
    export_df = prepare_export_df(df)

    print("\n=== Preview of export data ===")
    print(f"Rows: {export_df.height}")
    print(f"Columns: {len(export_df.columns)}")
    print("Column names:")
    print(", ".join(export_df.columns))
    print(f"\nFirst {min(rows, export_df.height)} rows:")
    print(export_df.head(rows))
    print("=== End preview ===\n")


def write(
    df: pl.DataFrame,
    base: str,
    fmt: str,
    stata_version: int,
    overwrite: bool = False,
    min_free_ram_mb: int = DEFAULT_MIN_FREE_RAM_MB,
    safe_mode: bool = False,
) -> None:
    output_path = f"{base}.{fmt}"

    if os.path.exists(output_path) and not overwrite:
        print(f"Skipping {output_path}: file already exists. Use --overwrite.")
        return

    export_df = prepare_export_df(df)
    estimated_df_bytes = max(export_df.estimated_size(), 1)

    if safe_mode and fmt in {"dta", "sav", "rdata"} and get_available_ram_mb() < max(2048, min_free_ram_mb * 2):
        raise MemoryError(
            f"Refusing export to {fmt} in --safe-mode: insufficient RAM for pandas/R conversion."
        )

    ensure_memory_headroom(
        stage=f"export to {fmt}",
        input_size_bytes=estimated_df_bytes,
        multiplier=3.0 if fmt in {"sav", "rdata"} else 2.5,
        minimum_free_mb=max(min_free_ram_mb, 1024 if fmt in {"sav", "rdata"} else min_free_ram_mb),
        safe_mode=safe_mode,
    )

    pdf = export_df.to_pandas()
    pdf = normalise_time_column_name(pdf)

    try:
        if fmt == "dta":
            try:
                pyreadstat.write_dta(pdf, output_path, version=stata_version)
            except TypeError:
                pyreadstat.write_dta(pdf, output_path)
        elif fmt == "sav":
            pyreadstat.write_sav(pdf, output_path)
        elif fmt == "rdata":
            with localconverter(ro.default_converter + pandas2ri.converter):
                ro.globalenv["df"] = pdf
            ro.globalenv["outfile"] = output_path
            ro.r("save(df, file=outfile)")
        else:
            raise ValueError(f"Unsupported output format: {fmt}")
    except Exception as exc:
        print(f"Error writing {output_path}: {exc}")
    finally:
        del pdf
        gc.collect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WB Data Converter (safe, efficient, robust, RAM-aware, layout-flexible)"
    )
    parser.add_argument("files", nargs="*", help="Input files (CSV/Excel)")
    parser.add_argument("--sav", action="store_true", help="Output SPSS (.sav)")
    parser.add_argument("--rdata", action="store_true", help="Output R (.RData)")
    parser.add_argument("--all", action="store_true", help="Output all formats")
    parser.add_argument("--out", nargs="*", help="Output filenames (must match input count)")
    parser.add_argument("--id", default="Country_Name", help="ID column name or output ID name")
    parser.add_argument(
        "--layout",
        choices=["auto", "wide", "long", "year_rows"],
        default="auto",
        help="Input layout: auto, wide, long, or year_rows.",
    )
    parser.add_argument("--year-col", default=None, help="Year column name for long/year_rows layouts")
    parser.add_argument("--value-col", default=None, help="Value column name for long layout")
    parser.add_argument("--series-col", default=None, help="Series column name for wide/long layout")
    parser.add_argument("--stata", type=int, default=15, help="STATA .dta version (11-15)")
    parser.add_argument(
        "--threshold",
        type=int,
        default=None,
        help="Lazy-mode threshold in MB. Default: auto based on available RAM.",
    )
    parser.add_argument(
        "--parquet",
        type=int,
        default=None,
        help="Parquet-intermediate threshold in MB. Default: auto based on available RAM.",
    )
    parser.add_argument(
        "--min-free-ram",
        type=int,
        default=DEFAULT_MIN_FREE_RAM_MB,
        help="Minimum RAM in MB to keep free as a safety reserve.",
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Be more conservative: lower thresholds and refuse risky pivot/export operations.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview the export-shaped output in the console before writing files.",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=DEFAULT_PREVIEW_ROWS,
        help=f"Number of rows to show with --preview. Default: {DEFAULT_PREVIEW_ROWS}.",
    )
    parser.add_argument("--delimiter", default=",", help="CSV delimiter")
    parser.add_argument(
        "--header-row",
        type=int,
        default=None,
        help="Override detected CSV header row (0-based).",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    parser.add_argument("--license", action="store_true", help="Display licence and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.license:
        print(LICENSE_TEXT)
        raise SystemExit(0)

    if not args.files:
        raise SystemExit(
            "Error: no input files provided. Use --license to view license or provide files."
        )

    if args.out and len(args.out) != len(args.files):
        raise SystemExit("Error: number of output files (--out) must match number of input files.")

    if not 11 <= args.stata <= 15:
        raise SystemExit("Error: --stata must be between 11 and 15.")

    if args.min_free_ram < 128:
        raise SystemExit("Error: --min-free-ram must be at least 128 MB.")

    if args.preview_rows < 1:
        raise SystemExit("Error: --preview-rows must be at least 1.")

    formats = ["dta"]
    if args.all:
        formats = ["dta", "sav", "rdata"]
    else:
        if args.sav:
            formats.append("sav")
        if args.rdata:
            formats.append("rdata")

    for i, input_file in enumerate(args.files):
        try:
            df = process_file(
                path=input_file,
                id_var=args.id,
                layout=args.layout,
                year_col=args.year_col,
                value_col=args.value_col,
                series_col=args.series_col,
                lazy_thresh=args.threshold,
                parquet_thresh=args.parquet,
                min_free_ram_mb=args.min_free_ram,
                safe_mode=args.safe_mode,
                delimiter=args.delimiter,
                header_row_override=args.header_row,
            )

            if args.preview:
                preview_output(df, rows=args.preview_rows)

            base = args.out[i] if args.out else os.path.splitext(input_file)[0]
            for fmt in formats:
                write(
                    df=df,
                    base=base,
                    fmt=fmt,
                    stata_version=args.stata,
                    overwrite=args.overwrite,
                    min_free_ram_mb=args.min_free_ram,
                    safe_mode=args.safe_mode,
                )

            print(f"Done: {input_file}")
            del df
            gc.collect()
        except MemoryError as exc:
            print(f"Memory safety stop for {input_file}: {exc}")
            continue
        except Exception as exc:
            print(f"Error processing {input_file}: {exc}")
            continue


if __name__ == "__main__":
    main()
