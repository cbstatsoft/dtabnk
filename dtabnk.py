#!/usr/bin/env python3
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
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

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
    missing: List[str] = []
    for package in REQ:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)

    if not missing:
        return

    print("Installing missing packages: {}...".format(", ".join(missing)))
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
    except subprocess.CalledProcessError as exc:
        sys.exit("Failed to install dependencies: {}".format(exc))


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

FrameLike = Union[pl.DataFrame, pl.LazyFrame]


def get_available_ram_mb() -> int:
    mem = psutil.virtual_memory()
    return max(1, int(mem.available // (1024 * 1024)))


def derive_memory_policy(
    file_size_bytes: int,
    lazy_thresh_mb: Optional[int] = None,
    parquet_thresh_mb: Optional[int] = None,
    safe_mode: bool = False,
) -> Dict[str, Union[int, bool]]:
    avail_mb = get_available_ram_mb()
    file_mb = max(1, int(math.ceil(file_size_bytes / (1024 * 1024))))

    dynamic_lazy_mb = (
        lazy_thresh_mb
        if lazy_thresh_mb is not None
        else max(32, int(avail_mb * (0.05 if safe_mode else 0.10)))
    )
    dynamic_parquet_mb = (
        parquet_thresh_mb
        if parquet_thresh_mb is not None
        else max(64, int(avail_mb * (0.10 if safe_mode else 0.25)))
    )

    use_lazy = file_mb >= dynamic_lazy_mb
    use_parquet = file_mb >= dynamic_parquet_mb or avail_mb < (
        3072 if safe_mode else 2048
    )

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
            "Refusing {}: available RAM ~{} MB, estimated need ~{} MB, reserve floor {} MB.".format(
                stage,
                avail_mb,
                needed_mb,
                reserve_mb,
            )
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


def get_skip_rows(path: str, delimiter: str, header_row_override: Optional[int]) -> int:
    if header_row_override is not None:
        return max(0, header_row_override)
    return find_header_row(path, delimiter=delimiter)


def collect_with_engine(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)
    except Exception:
        return lf.collect()


def collect_frame(frame: FrameLike) -> pl.DataFrame:
    if isinstance(frame, pl.LazyFrame):
        return collect_with_engine(frame)
    return frame


def get_columns(frame: FrameLike) -> List[str]:
    if isinstance(frame, pl.LazyFrame):
        return frame.collect_schema().names()
    return list(frame.columns)


def estimate_frame_bytes(frame: FrameLike, fallback_bytes: int) -> int:
    if isinstance(frame, pl.DataFrame):
        return max(frame.estimated_size(), 1)
    return max(fallback_bytes, 1)


def strip_bottom_metadata(frame: FrameLike) -> FrameLike:
    cols = get_columns(frame)
    if not cols:
        return frame

    first_col = cols[0]
    return frame.filter(
        ~pl.col(first_col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.contains(r"(?i)^(Data from database:|Last Updated:)")
    )


def sanitise(columns: Iterable[str], max_len: int = 64) -> List[str]:
    seen = set()
    out = []

    for raw in columns:
        col = str(raw).strip().replace(" ", "_")
        col = col.replace("%", "pct").replace("US$", "USD").replace("$", "USD")
        col = "".join(ch for ch in col if ch.isalnum() or ch == "_")

        if not col:
            col = "v"
        if not col[0].isalpha():
            col = "v_{}".format(col)

        col = col[:max_len].rstrip("_") or "v"
        base = col
        i = 1
        while col in seen:
            suffix = "_{}".format(i)
            col = "{}{}".format(base[: max_len - len(suffix)], suffix).rstrip("_")
            i += 1

        seen.add(col)
        out.append(col)

    return out


def sanitise_one(name: str) -> str:
    return sanitise([name])[0]


def is_year_like(name: str) -> bool:
    return re.search(r"(19|20)\d{2}", str(name)) is not None


def read_excel_compat(path: str) -> pl.DataFrame:
    errors: List[str] = []

    for engine in ("calamine", "openpyxl"):
        try:
            return pl.read_excel(path, engine=engine)
        except Exception as exc:
            errors.append("{}: {}".format(engine, exc))

    try:
        return pl.read_excel(path)
    except Exception as exc:
        errors.append("default: {}".format(exc))

    raise RuntimeError("Failed to read Excel file. " + " | ".join(errors[-3:]))


def read_source(
    path: str,
    lazy_thresh_mb: Optional[int],
    parquet_thresh_mb: Optional[int],
    safe_mode: bool,
    delimiter: str,
    header_row_override: Optional[int],
) -> Tuple[FrameLike, Optional[str], Dict[str, Union[int, bool]]]:
    ext = os.path.splitext(path)[1].lower()
    file_size = os.path.getsize(path)
    skip_rows = (
        get_skip_rows(path, delimiter, header_row_override) if ext == ".csv" else 0
    )

    policy = derive_memory_policy(
        file_size_bytes=file_size,
        lazy_thresh_mb=lazy_thresh_mb,
        parquet_thresh_mb=parquet_thresh_mb,
        safe_mode=safe_mode,
    )

    print(
        "Available RAM: {} MB | File: {} MB | Lazy threshold: {} MB | Parquet threshold: {} MB".format(
            policy["avail_mb"],
            policy["file_mb"],
            policy["lazy_thresh_mb"],
            policy["parquet_thresh_mb"],
        )
    )

    use_lazy = bool(policy["use_lazy"])
    use_parquet = bool(policy["use_parquet"])
    temp_parquet_path = None

    if ext == ".csv" and use_parquet:
        print(
            "Large file ({:.1f} MB). Using streaming CSV -> Parquet intermediate...".format(
                file_size / 1024 / 1024
            )
        )

        temp_parquet_path = "{}.parquet.tmp".format(path)

        try:
            lf = pl.scan_csv(
                path,
                skip_rows=skip_rows,
                separator=delimiter,
                low_memory=True,
            )
            lf.sink_parquet(temp_parquet_path, compression="zstd")
            frame = pl.scan_parquet(temp_parquet_path)
            print("Parquet intermediate conversion complete.")
            return strip_bottom_metadata(frame), temp_parquet_path, policy
        except Exception as exc:
            print(
                "Streaming Parquet intermediate failed: {}. Falling back.".format(exc)
            )
            if os.path.exists(temp_parquet_path):
                try:
                    os.remove(temp_parquet_path)
                except Exception:
                    pass
            temp_parquet_path = None

    if use_parquet:
        print(
            "Large file ({:.1f} MB). Using Parquet intermediate for efficiency...".format(
                file_size / 1024 / 1024
            )
        )

        ensure_memory_headroom(
            stage="source read before Parquet intermediate",
            input_size_bytes=file_size,
            multiplier=1.2 if ext == ".csv" else 2.0,
            minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
            safe_mode=safe_mode,
        )

        temp_parquet_path = "{}.parquet.tmp".format(path)

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
            raise ValueError("Unsupported format: {}".format(ext))

        df_src = collect_frame(strip_bottom_metadata(df_src))
        df_src.write_parquet(temp_parquet_path, compression="zstd")
        del df_src
        gc.collect()

        frame = pl.scan_parquet(temp_parquet_path)
        print("Parquet intermediate conversion complete.")
        return frame, temp_parquet_path, policy

    if ext == ".csv":
        if use_lazy:
            try:
                frame = pl.scan_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=delimiter,
                    low_memory=True,
                )
                return strip_bottom_metadata(frame), None, policy
            except Exception:
                ensure_memory_headroom(
                    stage="eager CSV read fallback",
                    input_size_bytes=file_size,
                    multiplier=1.5,
                    minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
                    safe_mode=safe_mode,
                )
                frame = pl.read_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=delimiter,
                    low_memory=True,
                )
                return strip_bottom_metadata(frame), None, policy

        ensure_memory_headroom(
            stage="eager CSV read",
            input_size_bytes=file_size,
            multiplier=1.5,
            minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
            safe_mode=safe_mode,
        )
        frame = pl.read_csv(
            path,
            skip_rows=skip_rows,
            separator=delimiter,
            low_memory=True,
        )
        return strip_bottom_metadata(frame), None, policy

    if ext in {".xlsx", ".xls"}:
        ensure_memory_headroom(
            stage="Excel read",
            input_size_bytes=file_size,
            multiplier=2.5 if safe_mode else 2.0,
            minimum_free_mb=DEFAULT_MIN_FREE_RAM_MB,
            safe_mode=safe_mode,
        )
        frame = read_excel_compat(path)
        return strip_bottom_metadata(frame), None, policy

    raise ValueError("Unsupported format: {}".format(ext))


def find_column_name(
    frame: FrameLike,
    candidates: Union[List[str], Tuple[str, ...]],
    exclude: Optional[Set[str]] = None,
) -> Optional[str]:
    excluded = exclude or set()
    columns = get_columns(frame)
    normalised = [sanitise_one(c) for c in candidates if c]

    for candidate in normalised:
        if candidate in columns and candidate not in excluded:
            return candidate

    lower_map = {col.lower(): col for col in columns if col not in excluded}
    for candidate in normalised:
        hit = lower_map.get(candidate.lower())
        if hit:
            return hit

    return None


def resolve_column_name(
    frame: FrameLike,
    requested: Optional[str] = None,
    fallbacks: Optional[Union[List[str], Tuple[str, ...]]] = None,
    exclude: Optional[Set[str]] = None,
    required: bool = True,
    label: str = "column",
) -> Optional[str]:
    candidates = []
    if requested:
        candidates.append(requested)
    if fallbacks:
        candidates.extend(fallbacks)

    hit = find_column_name(frame, candidates, exclude=exclude)
    if hit is not None:
        if requested and hit != sanitise_one(requested):
            print("Info: Using '{}' for {} '{}'.".format(hit, label, requested))
        return hit

    if not required:
        return None

    columns = get_columns(frame)
    available = ", ".join(columns[:20])
    if len(columns) > 20:
        available += ", ..."
    requested_text = (
        requested if requested else "/".join(candidates) if candidates else label
    )
    raise ValueError(
        "{} '{}' not found. Available: {}".format(
            label.capitalize(), requested_text, available
        )
    )


def resolve_id_column(
    frame: FrameLike,
    requested_id: str,
    exclude: Optional[Set[str]] = None,
) -> str:
    requested_norm = sanitise_one(requested_id)
    fallbacks = []

    if requested_norm == "Country_Name":
        fallbacks = ["Country"]
    elif requested_norm == "Country":
        fallbacks = ["Country_Name"]

    hit = resolve_column_name(
        frame,
        requested=requested_norm,
        fallbacks=fallbacks,
        exclude=exclude,
        required=False,
        label="ID column",
    )
    if hit:
        return hit

    excluded = exclude or set()
    for col in get_columns(frame):
        if col in excluded:
            continue
        if col in {"Year", "Value", "Series", "Series_Name"}:
            continue
        if not is_year_like(col):
            print("Info: Using '{}' as ID column.".format(col))
            return col

    available = ", ".join(get_columns(frame)[:20])
    raise ValueError("Unable to resolve ID column. Available: {}".format(available))


def detect_layout(
    frame: FrameLike,
    requested_layout: str,
    year_col: Optional[str],
    value_col: Optional[str],
) -> str:
    if requested_layout != "auto":
        return requested_layout

    year_hit = find_column_name(frame, [year_col] if year_col else YEAR_ALIASES)
    value_hit = find_column_name(frame, [value_col] if value_col else VALUE_ALIASES)

    if year_hit and value_hit:
        return "long"

    columns = get_columns(frame)
    if columns:
        first_col = columns[0].lower()
        year_aliases = {sanitise_one(x).lower() for x in YEAR_ALIASES}
        if first_col in year_aliases and len(columns) > 2:
            return "year_rows"

    return "wide"


def cast_year_and_value(
    frame: FrameLike, year_col: str, value_col: str = "Value"
) -> FrameLike:
    return frame.with_columns(
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


def pivot_eager(
    frame: FrameLike,
    index: List[str],
    columns: str,
    values: str,
    aggregate_function: str = "mean",
) -> pl.DataFrame:
    df = collect_frame(frame)

    try:
        return df.pivot(
            on=columns,
            index=index,
            values=values,
            aggregate_function=aggregate_function,
        )
    except TypeError:
        return df.pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=aggregate_function,
        )


def process_wide_layout(
    frame: FrameLike,
    file_size: int,
    id_var: str,
    series_col_arg: Optional[str],
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    columns = get_columns(frame)

    drop_candidates = [c for c in ("Country_Code", "Series_Code") if c in columns]
    if drop_candidates:
        frame = frame.drop(drop_candidates)

    series_col = resolve_column_name(
        frame,
        requested=series_col_arg,
        fallbacks=SERIES_ALIASES,
        required=False,
        label="series column",
    )

    exclude = {series_col} if series_col else set()
    actual_id_var = resolve_id_column(frame, id_var, exclude=exclude)

    id_vars = [actual_id_var]
    if series_col:
        id_vars.append(series_col)

    value_vars = [c for c in get_columns(frame) if c not in id_vars and is_year_like(c)]
    if not value_vars:
        available = ", ".join(get_columns(frame)[:20])
        raise ValueError(
            "No year-like value columns found to unpivot. Available columns: {}".format(
                available
            )
        )

    frame = frame.select(id_vars + value_vars)

    estimated_unpivot_multiplier = max(2.0, min(8.0, len(value_vars) / 4))
    ensure_memory_headroom(
        stage="unpivot",
        input_size_bytes=file_size,
        multiplier=estimated_unpivot_multiplier,
        minimum_free_mb=min_free_ram_mb,
        safe_mode=safe_mode,
    )

    frame = frame.unpivot(
        index=id_vars,
        on=value_vars,
        variable_name="Year",
        value_name="Value",
    )
    frame = cast_year_and_value(frame, year_col="Year", value_col="Value")

    if series_col and series_col in get_columns(frame):
        if safe_mode and get_available_ram_mb() < max(2048, min_free_ram_mb * 2):
            raise MemoryError(
                "Refusing pivot in --safe-mode: insufficient RAM headroom for eager pivot."
            )

        ensure_memory_headroom(
            stage="pivot",
            input_size_bytes=estimate_frame_bytes(frame, fallback_bytes=file_size),
            multiplier=3.5 if safe_mode else 3.0,
            minimum_free_mb=max(min_free_ram_mb, 1024),
            safe_mode=safe_mode,
        )

        frame = frame.rename({series_col: "Series"})
        pivoted = pivot_eager(
            frame=frame,
            index=[actual_id_var, "Year"],
            columns="Series",
            values="Value",
            aggregate_function="mean",
        )
        pivoted = pivoted.rename(dict(zip(pivoted.columns, sanitise(pivoted.columns))))
        return pivoted

    return collect_frame(frame)


def process_long_layout(
    frame: FrameLike,
    file_size: int,
    id_var: str,
    year_col_arg: Optional[str],
    value_col_arg: Optional[str],
    series_col_arg: Optional[str],
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    columns = get_columns(frame)
    drop_candidates = [c for c in ("Country_Code", "Series_Code") if c in columns]
    if drop_candidates:
        frame = frame.drop(drop_candidates)

    year_col = resolve_column_name(
        frame,
        requested=year_col_arg,
        fallbacks=YEAR_ALIASES,
        label="year column",
    )
    value_col = resolve_column_name(
        frame,
        requested=value_col_arg,
        fallbacks=VALUE_ALIASES,
        exclude={year_col},
        label="value column",
    )
    series_col = resolve_column_name(
        frame,
        requested=series_col_arg,
        fallbacks=SERIES_ALIASES,
        exclude={year_col, value_col},
        required=False,
        label="series column",
    )

    exclude = {year_col, value_col}
    if series_col:
        exclude.add(series_col)

    actual_id_var = resolve_id_column(frame, id_var, exclude=exclude)

    keep_cols = [actual_id_var, year_col, value_col]
    if series_col:
        keep_cols.append(series_col)

    frame = frame.select(keep_cols).rename({year_col: "Year", value_col: "Value"})
    if series_col and series_col != "Series":
        frame = frame.rename({series_col: "Series"})

    frame = cast_year_and_value(frame, year_col="Year", value_col="Value")

    if "Series" in get_columns(frame):
        ensure_memory_headroom(
            stage="pivot",
            input_size_bytes=estimate_frame_bytes(frame, fallback_bytes=file_size),
            multiplier=3.0 if safe_mode else 2.5,
            minimum_free_mb=max(
                min_free_ram_mb, 1024 if safe_mode else min_free_ram_mb
            ),
            safe_mode=safe_mode,
        )
        pivoted = pivot_eager(
            frame=frame,
            index=[actual_id_var, "Year"],
            columns="Series",
            values="Value",
            aggregate_function="mean",
        )
        pivoted = pivoted.rename(dict(zip(pivoted.columns, sanitise(pivoted.columns))))
        return pivoted

    return collect_frame(frame)


def process_year_rows_layout(
    frame: FrameLike,
    file_size: int,
    id_var: str,
    year_col_arg: Optional[str],
    min_free_ram_mb: int,
    safe_mode: bool,
) -> pl.DataFrame:
    year_col = resolve_column_name(
        frame,
        requested=year_col_arg,
        fallbacks=YEAR_ALIASES,
        label="year column",
    )

    value_vars = [c for c in get_columns(frame) if c != year_col]
    if not value_vars:
        raise ValueError("No entity columns found in year_rows layout.")

    output_id_var = sanitise_one(id_var)
    frame = frame.select([year_col] + value_vars)

    estimated_unpivot_multiplier = max(2.0, min(8.0, len(value_vars) / 4))
    ensure_memory_headroom(
        stage="unpivot",
        input_size_bytes=file_size,
        multiplier=estimated_unpivot_multiplier,
        minimum_free_mb=min_free_ram_mb,
        safe_mode=safe_mode,
    )

    frame = frame.unpivot(
        index=[year_col],
        on=value_vars,
        variable_name=output_id_var,
        value_name="Value",
    )

    if year_col != "Year":
        frame = frame.rename({year_col: "Year"})

    frame = cast_year_and_value(frame, year_col="Year", value_col="Value")
    return collect_frame(frame)


def process_file(
    path: str,
    id_var: str,
    layout: str,
    year_col: Optional[str],
    value_col: Optional[str],
    series_col: Optional[str],
    lazy_thresh: Optional[int],
    parquet_thresh: Optional[int],
    min_free_ram_mb: int,
    safe_mode: bool,
    delimiter: str,
    header_row_override: Optional[int],
) -> pl.DataFrame:
    file_size = os.path.getsize(path)
    frame, temp_parquet_path, _policy = read_source(
        path=path,
        lazy_thresh_mb=lazy_thresh,
        parquet_thresh_mb=parquet_thresh,
        safe_mode=safe_mode,
        delimiter=delimiter,
        header_row_override=header_row_override,
    )

    try:
        original_cols = get_columns(frame)
        frame = frame.rename(dict(zip(original_cols, sanitise(original_cols))))

        chosen_layout = detect_layout(frame, layout, year_col, value_col)
        print("Info: Using layout '{}'.".format(chosen_layout))

        if chosen_layout == "wide":
            return process_wide_layout(
                frame=frame,
                file_size=file_size,
                id_var=id_var,
                series_col_arg=series_col,
                min_free_ram_mb=min_free_ram_mb,
                safe_mode=safe_mode,
            )

        if chosen_layout == "long":
            return process_long_layout(
                frame=frame,
                file_size=file_size,
                id_var=id_var,
                year_col_arg=year_col,
                value_col_arg=value_col,
                series_col_arg=series_col,
                min_free_ram_mb=min_free_ram_mb,
                safe_mode=safe_mode,
            )

        if chosen_layout == "year_rows":
            return process_year_rows_layout(
                frame=frame,
                file_size=file_size,
                id_var=id_var,
                year_col_arg=year_col,
                min_free_ram_mb=min_free_ram_mb,
                safe_mode=safe_mode,
            )

        raise ValueError("Unsupported layout: {}".format(chosen_layout))
    finally:
        if temp_parquet_path and os.path.exists(temp_parquet_path):
            try:
                os.remove(temp_parquet_path)
            except Exception:
                pass


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
    print("Rows: {}".format(export_df.height))
    print("Columns: {}".format(len(export_df.columns)))
    print("Column names:")
    print(", ".join(export_df.columns))
    print("\nFirst {} rows:".format(min(rows, export_df.height)))
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
    output_path = "{}.{}".format(base, fmt)

    if os.path.exists(output_path) and not overwrite:
        print("Skipping {}: file already exists. Use --overwrite.".format(output_path))
        return

    export_df = prepare_export_df(df)
    estimated_df_bytes = max(export_df.estimated_size(), 1)

    if (
        safe_mode
        and fmt == "rdata"
        and get_available_ram_mb() < max(2048, min_free_ram_mb * 2)
    ):
        raise MemoryError(
            "Refusing export to {} in --safe-mode: insufficient RAM for conversion.".format(
                fmt
            )
        )

    ensure_memory_headroom(
        stage="export to {}".format(fmt),
        input_size_bytes=estimated_df_bytes,
        multiplier=3.0 if fmt == "rdata" else 1.5,
        minimum_free_mb=max(
            min_free_ram_mb, 1024 if fmt == "rdata" else min_free_ram_mb
        ),
        safe_mode=safe_mode,
    )

    try:
        if fmt == "dta":
            try:
                pyreadstat.write_dta(export_df, output_path, version=stata_version)
            except TypeError:
                pyreadstat.write_dta(
                    export_df.to_pandas(), output_path, version=stata_version
                )
        elif fmt == "sav":
            try:
                pyreadstat.write_sav(export_df, output_path)
            except TypeError:
                pyreadstat.write_sav(export_df.to_pandas(), output_path)
        elif fmt == "rdata":
            pdf = normalise_time_column_name(export_df.to_pandas())
            try:
                with localconverter(ro.default_converter + pandas2ri.converter):
                    ro.globalenv["df"] = pdf
                ro.globalenv["outfile"] = output_path
                ro.r("save(df, file=outfile)")
            finally:
                del pdf
                gc.collect()
        elif fmt == "parquet":
            export_df.write_parquet(output_path, compression="zstd")
        else:
            raise ValueError("Unsupported output format: {}".format(fmt))
    except Exception as exc:
        print("Error writing {}: {}".format(output_path, exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "dtabnk: convert World Bank Open Data CSV/Excel files to panel datasets "
            "in STATA (default), SPSS, R, and/or Parquet formats."
        )
    )
    parser.add_argument("files", nargs="*", help="Input files (.csv, .xlsx, .xls).")
    parser.add_argument(
        "--sav", action="store_true", help="Output SPSS/PSPP .sav file."
    )
    parser.add_argument("--rdata", action="store_true", help="Output R .RData file.")
    parser.add_argument(
        "--parquet-out", action="store_true", help="Output Parquet .parquet file."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Output all available formats (STATA, SPSS, R, Parquet).",
    )
    parser.add_argument(
        "--out",
        nargs="*",
        help="Specify output filename(s) (default: input filename). Must match the number of input files.",
    )
    parser.add_argument(
        "--id",
        default="Country_Name",
        help="Specify the entity ID column name (default: Country_Name).",
    )
    parser.add_argument(
        "--layout",
        choices=["auto", "wide", "long", "year_rows"],
        default="auto",
        help="Specify input layout: auto, wide, long, or year_rows (default: auto).",
    )
    parser.add_argument(
        "--year-col",
        default=None,
        help="Specify the year column for long or year_rows layouts.",
    )
    parser.add_argument(
        "--value-col",
        default=None,
        help="Specify the value column for long layouts.",
    )
    parser.add_argument(
        "--series-col",
        default=None,
        help="Specify the series column for wide or long layouts.",
    )
    parser.add_argument(
        "--stata",
        type=int,
        default=15,
        help="Specify STATA .dta version (11-15; default: 15).",
    )
    parser.add_argument(
        "--lazy",
        type=int,
        default=None,
        help="Size (MB) threshold to switch to lazy CSV processing (default: auto based on available RAM).",
    )
    parser.add_argument(
        "--parquet",
        type=int,
        default=None,
        help="Size (MB) threshold to enable Parquet Intermediate processing (default: auto based on available RAM).",
    )
    parser.add_argument(
        "--min-free-ram",
        type=int,
        default=DEFAULT_MIN_FREE_RAM_MB,
        help="Minimum RAM (MB) to keep free as a safety reserve.",
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use more conservative memory behaviour and stop before risky reshape/export steps.",
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
        help="Number of preview rows to display (default: {}).".format(
            DEFAULT_PREVIEW_ROWS
        ),
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Specify CSV delimiter (default: ',').",
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=None,
        help="Override detected CSV header row (0-based).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files without prompting.",
    )
    parser.add_argument(
        "--license",
        "--licence",
        dest="license",
        action="store_true",
        help="Print software licence information and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.license:
        print(LICENSE_TEXT)
        raise SystemExit(0)

    if not args.files:
        raise SystemExit(
            "Error: no input files provided. Use -h to view help."
        )

    if args.out and len(args.out) != len(args.files):
        raise SystemExit(
            "Error: number of output files (--out) must match number of input files."
        )

    if not 11 <= args.stata <= 15:
        raise SystemExit("Error: --stata must be between 11 and 15.")

    if args.min_free_ram < 128:
        raise SystemExit("Error: --min-free-ram must be at least 128 MB.")

    if args.preview_rows < 1:
        raise SystemExit("Error: --preview-rows must be at least 1.")

    formats = ["dta"]
    if args.all:
        formats = ["dta", "sav", "rdata", "parquet"]
    else:
        if args.sav:
            formats.append("sav")
        if args.rdata:
            formats.append("rdata")
        if args.parquet_out:
            formats.append("parquet")

    for i, input_file in enumerate(args.files):
        try:
            df = process_file(
                path=input_file,
                id_var=args.id,
                layout=args.layout,
                year_col=args.year_col,
                value_col=args.value_col,
                series_col=args.series_col,
                lazy_thresh=args.lazy,
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

            print("Done: {}".format(input_file))
            del df
            gc.collect()
        except MemoryError as exc:
            print("Memory safety stop for {}: {}".format(input_file, exc))
            continue
        except Exception as exc:
            print("Error processing {}: {}".format(input_file, exc))
            continue


if __name__ == "__main__":
    main()
