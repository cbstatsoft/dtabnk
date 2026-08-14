#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gc
import importlib
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

# ---------------------------------------------------------------------------
# Application constants
# ---------------------------------------------------------------------------

CORE_PACKAGES = (
    ("polars", "polars>=1.0"),
    ("psutil", "psutil"),
)

DEFAULT_MIN_FREE_RAM_MB = 512
DEFAULT_PREVIEW_ROWS = 10
DEFAULT_STREAMING_CHUNK_SIZE = 10_000
STATISTICAL_NAME_MAX_LENGTH = 32

OUTPUT_EXTENSIONS = {
    "dta": "dta",
    "sav": "sav",
    "rdata": "RData",
    "parquet": "parquet",
}

YEAR_ALIASES = ("Year", "year", "Time", "time", "Date", "date", "Period", "period")
VALUE_ALIASES = (
    "Value",
    "value",
    "OBS_VALUE",
    "obs_value",
    "Observation_Value",
    "observation_value",
)
SERIES_ALIASES = (
    "Series_Name",
    "Series",
    "Indicator_Name",
    "Indicator",
    "series_name",
    "series",
    "indicator_name",
    "indicator",
)

HEADER_SERIES_PATTERN = re.compile(
    r"((?:19|20)\d{2})(?:\s*\[YR(?:19|20)\d{2}\])?\s*-\s*(.+?)"
    r"(?:\s*\[([A-Za-z0-9._]+)\])?\s*$"
)

LICENCE_TEXT = """GNU General Public Licence v3.0 or later

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


# ---------------------------------------------------------------------------
# Dependency bootstrap
# ---------------------------------------------------------------------------


def install_package(package_spec: str) -> None:
    """Install one Python package with the interpreter running dtabnk."""
    print("Installing missing package: {}...".format(package_spec))
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_spec])
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "Failed to install Python package '{}': {}".format(package_spec, exc)
        ) from exc


def ensure_package(import_name: str, package_spec: Optional[str] = None):
    """Import a package, installing it first when it is not available."""
    try:
        return importlib.import_module(import_name)
    except ImportError:
        install_package(package_spec or import_name)

    try:
        return importlib.import_module(import_name)
    except ImportError as exc:
        raise RuntimeError(
            "Package '{}' was installed but could not be imported: {}".format(
                import_name, exc
            )
        ) from exc


def ensure_core_dependencies() -> None:
    """Ensure only dependencies required for every conversion are present."""
    for import_name, package_spec in CORE_PACKAGES:
        ensure_package(import_name, package_spec)


if sys.version_info < (3, 8):
    raise SystemExit("dtabnk requires Python 3.8 or later.")


try:
    ensure_core_dependencies()
except RuntimeError as exc:
    raise SystemExit(str(exc)) from exc

import polars as pl
import psutil


def version_tuple(version: str) -> Tuple[int, ...]:
    """Extract the numeric prefix of a dotted version string."""
    parts = []
    for token in str(version).split("."):
        match = re.match(r"(\d+)", token)
        if not match:
            break
        parts.append(int(match.group(1)))
    return tuple(parts)


POLARS_VERSION = version_tuple(getattr(pl, "__version__", "0"))
if POLARS_VERSION < (1, 0):
    raise SystemExit(
        "dtabnk requires Polars 1.0 or later. Upgrade with "
        "'python -m pip install -U polars' and run the command again."
    )

# Polars 1.36.0 was withdrawn for an unpivot regression. dtabnk relies on
# unpivot heavily, so fail explicitly rather than risk silently wrong output.
if POLARS_VERSION[:3] == (1, 36, 0):
    raise SystemExit(
        "Polars 1.36.0 is not supported because of its known unpivot "
        "regression. Upgrade Polars and run the command again."
    )

try:
    pl.Config.set_streaming_chunk_size(DEFAULT_STREAMING_CHUNK_SIZE)
except Exception:
    # Older Polars releases may not expose this configuration method.
    pass


FrameLike = Union[pl.DataFrame, pl.LazyFrame]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MemoryPolicy:
    available_mb: int
    file_mb: int
    lazy_threshold_mb: int
    parquet_threshold_mb: int
    use_lazy: bool
    use_parquet: bool


@dataclass(frozen=True)
class ReadOptions:
    lazy_threshold_mb: Optional[int]
    parquet_threshold_mb: Optional[int]
    minimum_free_ram_mb: int
    safe_mode: bool
    delimiter: str
    header_row: Optional[int]
    reshape_heavy: bool
    multi_export: bool


@dataclass(frozen=True)
class TransformOptions:
    id_column: str
    layout: str
    year_column: Optional[str]
    value_column: Optional[str]
    series_column: Optional[str]
    minimum_free_ram_mb: int
    safe_mode: bool


@dataclass
class SourceData:
    frame: FrameLike
    file_size_bytes: int
    policy: MemoryPolicy
    temporary_path: Optional[str] = None

    def cleanup(self) -> None:
        if not self.temporary_path:
            return

        try:
            os.remove(self.temporary_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(
                "Warning: could not remove temporary file '{}': {}".format(
                    self.temporary_path, exc
                )
            )


# ---------------------------------------------------------------------------
# Memory policy
# ---------------------------------------------------------------------------


class MemoryManager:
    """Centralise RAM estimates and safety checks."""

    @staticmethod
    def available_ram_mb() -> int:
        memory = psutil.virtual_memory()
        return max(1, int(memory.available // (1024 * 1024)))

    @classmethod
    def derive_policy(
        cls,
        file_size_bytes: int,
        lazy_threshold_mb: Optional[int] = None,
        parquet_threshold_mb: Optional[int] = None,
        safe_mode: bool = False,
        reshape_heavy: bool = False,
        multi_export: bool = False,
    ) -> MemoryPolicy:
        available_mb = cls.available_ram_mb()
        file_mb = max(1, int(math.ceil(file_size_bytes / (1024 * 1024))))

        lazy_threshold = (
            lazy_threshold_mb
            if lazy_threshold_mb is not None
            else min(
                128,
                max(32, int(available_mb * (0.05 if safe_mode else 0.08))),
            )
        )

        if parquet_threshold_mb is not None:
            parquet_threshold = parquet_threshold_mb
        else:
            floor = 96 if safe_mode else 128
            cap = 192 if (reshape_heavy or multi_export) else 256
            ram_factor = 0.08 if safe_mode else 0.12
            parquet_threshold = min(
                cap,
                max(floor, int(available_mb * ram_factor)),
            )

            if reshape_heavy:
                parquet_threshold = max(96, parquet_threshold - 32)
            if multi_export:
                parquet_threshold = max(96, parquet_threshold - 32)

        use_lazy = file_mb >= lazy_threshold
        use_parquet = file_mb >= parquet_threshold or available_mb < (
            3072 if safe_mode else 2048
        )

        return MemoryPolicy(
            available_mb=available_mb,
            file_mb=file_mb,
            lazy_threshold_mb=lazy_threshold,
            parquet_threshold_mb=parquet_threshold,
            use_lazy=use_lazy,
            use_parquet=use_parquet,
        )

    @classmethod
    def ensure_headroom(
        cls,
        stage: str,
        input_size_bytes: int,
        multiplier: float,
        minimum_free_mb: int = DEFAULT_MIN_FREE_RAM_MB,
        safe_mode: bool = False,
    ) -> None:
        available_mb = cls.available_ram_mb()
        reserve_mb = max(minimum_free_mb, 1024 if safe_mode else minimum_free_mb)
        required_mb = max(
            1,
            int(math.ceil((input_size_bytes * multiplier) / (1024 * 1024))),
        )

        if available_mb - required_mb < reserve_mb:
            raise MemoryError(
                "Refusing {}: available RAM ~{} MB, estimated need ~{} MB, "
                "reserve floor {} MB.".format(
                    stage,
                    available_mb,
                    required_mb,
                    reserve_mb,
                )
            )

    @classmethod
    def allow_pivot(
        cls,
        frame: FrameLike,
        fallback_bytes: int,
        minimum_free_ram_mb: int,
        safe_mode: bool,
    ) -> bool:
        estimated_bytes = estimate_frame_bytes(frame, fallback_bytes)
        available_mb = cls.available_ram_mb()
        reserve_mb = max(
            minimum_free_ram_mb,
            1024 if safe_mode else minimum_free_ram_mb,
        )
        budget_mb = max(0, available_mb - reserve_mb)
        multiplier = 3.5 if safe_mode else 3.0
        required_mb = int(math.ceil((estimated_bytes * multiplier) / (1024 * 1024)))
        budget_fraction = 0.60 if safe_mode else 0.75

        return required_mb < max(256, int(budget_mb * budget_fraction))


# ---------------------------------------------------------------------------
# Frame helpers
# ---------------------------------------------------------------------------


def collect_lazy_frame(frame: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame using the best streaming API available."""
    try:
        return frame.collect(engine="streaming")
    except TypeError:
        return frame.collect(streaming=True)
    except Exception:
        return frame.collect()


def collect_frame(frame: FrameLike) -> pl.DataFrame:
    if isinstance(frame, pl.LazyFrame):
        return collect_lazy_frame(frame)
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
    columns = get_columns(frame)
    if not columns:
        return frame

    first_column = columns[0]
    footer_pattern = r"(?i)^(Data from database:|Last Updated:)"
    return frame.filter(
        ~pl.col(first_column)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.contains(footer_pattern)
    )


def sanitise(
    columns: Iterable[str],
    max_length: int = STATISTICAL_NAME_MAX_LENGTH,
) -> List[str]:
    """Return unique, software-safe variable names."""
    seen = set()
    output = []

    for raw_name in columns:
        name = unicodedata.normalize("NFKD", str(raw_name))
        name = name.encode("ascii", "ignore").decode("ascii")
        name = name.strip().replace(" ", "_")
        name = name.replace("%", "pct").replace("US$", "USD").replace("$", "USD")
        name = "".join(
            character
            for character in name
            if character.isascii() and (character.isalnum() or character == "_")
        )

        if not name:
            name = "v"
        if not name[0].isalpha():
            name = "v_{}".format(name)

        name = name[:max_length].rstrip("_") or "v"
        base_name = name
        suffix_number = 1

        while name.lower() in seen:
            suffix = "_{}".format(suffix_number)
            name = "{}{}".format(
                base_name[: max_length - len(suffix)],
                suffix,
            ).rstrip("_")
            suffix_number += 1

        seen.add(name.lower())
        output.append(name)

    return output


def sanitise_one(name: str) -> str:
    return sanitise([name])[0]


def is_year_like(name: str) -> bool:
    return re.search(r"(19|20)\d{2}", str(name)) is not None


def cast_year_and_value(
    frame: FrameLike,
    year_column: str,
    value_column: str = "Value",
) -> FrameLike:
    return frame.with_columns(
        [
            pl.col(year_column)
            .cast(pl.Utf8, strict=False)
            .str.extract(r"(\d{4})")
            .cast(pl.Int32, strict=False)
            .alias(year_column),
            pl.when(pl.col(value_column).cast(pl.Utf8, strict=False) == "..")
            .then(None)
            .otherwise(pl.col(value_column))
            .cast(pl.Float64, strict=False)
            .alias(value_column),
        ]
    ).filter(pl.col(year_column).is_not_null())


def pivot_eager(
    frame: FrameLike,
    index: List[str],
    columns: str,
    values: str,
    aggregate_function: str = "mean",
) -> pl.DataFrame:
    data = collect_frame(frame)

    try:
        return data.pivot(
            on=columns,
            index=index,
            values=values,
            aggregate_function=aggregate_function,
        )
    except TypeError:
        # Compatibility with older Polars releases.
        return data.pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=aggregate_function,
        )


# ---------------------------------------------------------------------------
# Column and layout detection
# ---------------------------------------------------------------------------


def parse_header_series_column(
    name: str,
) -> Optional[Tuple[int, str, Optional[str]]]:
    match = HEADER_SERIES_PATTERN.search(str(name).strip())
    if not match:
        return None

    year = int(match.group(1))
    series = match.group(2).strip()
    series_code = match.group(3).strip() if match.group(3) else None
    return year, series, series_code


def find_column_name(
    frame: FrameLike,
    candidates: Sequence[str],
    exclude: Optional[Set[str]] = None,
) -> Optional[str]:
    excluded = exclude or set()
    columns = get_columns(frame)
    normalised_candidates = [
        sanitise_one(candidate) for candidate in candidates if candidate
    ]

    for candidate in normalised_candidates:
        if candidate in columns and candidate not in excluded:
            return candidate

    lower_map = {column.lower(): column for column in columns if column not in excluded}
    for candidate in normalised_candidates:
        match = lower_map.get(candidate.lower())
        if match:
            return match

    return None


def resolve_column_name(
    frame: FrameLike,
    requested: Optional[str] = None,
    fallbacks: Optional[Sequence[str]] = None,
    exclude: Optional[Set[str]] = None,
    required: bool = True,
    label: str = "column",
) -> Optional[str]:
    candidates = []
    if requested:
        candidates.append(requested)
    if fallbacks:
        candidates.extend(fallbacks)

    match = find_column_name(frame, candidates, exclude=exclude)
    if match is not None:
        if requested and match != sanitise_one(requested):
            print("Info: Using '{}' for {} '{}'.".format(match, label, requested))
        return match

    if not required:
        return None

    columns = get_columns(frame)
    available = ", ".join(columns[:20])
    if len(columns) > 20:
        available += ", ..."

    requested_text = requested or "/".join(candidates) or label
    raise ValueError(
        "{} '{}' not found. Available: {}".format(
            label.capitalize(),
            requested_text,
            available,
        )
    )


def resolve_id_column(
    frame: FrameLike,
    requested_id: str,
    exclude: Optional[Set[str]] = None,
) -> str:
    requested_normalised = sanitise_one(requested_id)
    fallbacks = []

    if requested_normalised == "Country_Name":
        fallbacks = ["Country"]
    elif requested_normalised == "Country":
        fallbacks = ["Country_Name"]

    match = resolve_column_name(
        frame,
        requested=requested_normalised,
        fallbacks=fallbacks,
        exclude=exclude,
        required=False,
        label="ID column",
    )
    if match:
        return match

    excluded = exclude or set()
    for column in get_columns(frame):
        if column in excluded:
            continue
        if column in {"Year", "Value", "Series", "Series_Name"}:
            continue
        if not is_year_like(column):
            print("Info: Using '{}' as ID column.".format(column))
            return column

    available = ", ".join(get_columns(frame)[:20])
    raise ValueError("Unable to resolve ID column. Available: {}".format(available))


def detect_header_series_wide_layout(
    frame: FrameLike,
    raw_columns_by_name: Optional[Dict[str, str]] = None,
) -> bool:
    columns = get_columns(frame)
    if len(columns) < 3:
        return False

    excluded = {
        sanitise_one("Country Name"),
        sanitise_one("Country Code"),
        sanitise_one("Series Name"),
        sanitise_one("Series Code"),
        sanitise_one("Indicator Name"),
        sanitise_one("Indicator Code"),
    }

    candidates = 0
    matches = 0

    for column in columns:
        if column in excluded:
            continue

        raw_name = (
            raw_columns_by_name.get(column, column) if raw_columns_by_name else column
        )
        candidates += 1
        if parse_header_series_column(raw_name):
            matches += 1

    return matches >= 2 and matches >= max(2, int(candidates * 0.5))


def detect_layout(
    frame: FrameLike,
    requested_layout: str,
    year_column: Optional[str],
    value_column: Optional[str],
    raw_columns_by_name: Optional[Dict[str, str]] = None,
) -> str:
    if requested_layout != "auto":
        return requested_layout

    if detect_header_series_wide_layout(frame, raw_columns_by_name):
        return "wide_header_series"

    year_match = find_column_name(
        frame,
        [year_column] if year_column else YEAR_ALIASES,
    )
    value_match = find_column_name(
        frame,
        [value_column] if value_column else VALUE_ALIASES,
    )

    if year_match and value_match:
        return "long"

    columns = get_columns(frame)
    if columns:
        first_column = columns[0].lower()
        year_aliases = {sanitise_one(alias).lower() for alias in YEAR_ALIASES}
        if first_column in year_aliases and len(columns) > 2:
            return "year_rows"

    return "wide"


# ---------------------------------------------------------------------------
# Source reading
# ---------------------------------------------------------------------------


class SourceReader:
    """Read CSV/Excel input according to one explicit memory policy."""

    def __init__(self, options: ReadOptions) -> None:
        self.options = options

    def read(self, path: str) -> SourceData:
        extension = os.path.splitext(path)[1].lower()
        if extension not in {".csv", ".xlsx", ".xls"}:
            raise ValueError("Unsupported format: {}".format(extension))

        file_size = os.path.getsize(path)
        policy = MemoryManager.derive_policy(
            file_size_bytes=file_size,
            lazy_threshold_mb=self.options.lazy_threshold_mb,
            parquet_threshold_mb=self.options.parquet_threshold_mb,
            safe_mode=self.options.safe_mode,
            reshape_heavy=self.options.reshape_heavy,
            multi_export=self.options.multi_export,
        )
        self._print_policy(policy)

        if policy.use_parquet:
            return self._read_via_parquet(path, extension, file_size, policy)

        if extension == ".csv":
            frame = self._read_csv(path, file_size, lazy=policy.use_lazy)
        else:
            frame = self._read_excel(path, file_size)

        return SourceData(
            frame=strip_bottom_metadata(frame),
            file_size_bytes=file_size,
            policy=policy,
        )

    @staticmethod
    def _print_policy(policy: MemoryPolicy) -> None:
        print(
            "Available RAM: {} MB | File: {} MB | Lazy threshold: {} MB | "
            "Parquet threshold: {} MB".format(
                policy.available_mb,
                policy.file_mb,
                policy.lazy_threshold_mb,
                policy.parquet_threshold_mb,
            )
        )

    def _read_csv(
        self,
        path: str,
        file_size: int,
        lazy: bool,
    ) -> FrameLike:
        skip_rows = self._skip_rows(path)

        if lazy:
            print("Info: Using lazy CSV scanning.")
            try:
                return pl.scan_csv(
                    path,
                    skip_rows=skip_rows,
                    separator=self.options.delimiter,
                    low_memory=True,
                )
            except Exception as exc:
                print(
                    "Lazy CSV scan failed: {}. Falling back to an eager read.".format(
                        exc
                    )
                )

        MemoryManager.ensure_headroom(
            stage="eager CSV read",
            input_size_bytes=file_size,
            multiplier=1.5,
            minimum_free_mb=self.options.minimum_free_ram_mb,
            safe_mode=self.options.safe_mode,
        )
        return pl.read_csv(
            path,
            skip_rows=skip_rows,
            separator=self.options.delimiter,
            low_memory=True,
        )

    def _read_excel(self, path: str, file_size: int) -> pl.DataFrame:
        MemoryManager.ensure_headroom(
            stage="Excel read",
            input_size_bytes=file_size,
            multiplier=2.5 if self.options.safe_mode else 2.0,
            minimum_free_mb=self.options.minimum_free_ram_mb,
            safe_mode=self.options.safe_mode,
        )
        return self._read_excel_compat(path)

    def _read_via_parquet(
        self,
        path: str,
        extension: str,
        file_size: int,
        policy: MemoryPolicy,
    ) -> SourceData:
        temporary_path = self._temporary_parquet_path()
        print(
            "Large or memory-sensitive input ({:.1f} MB). Using a Parquet "
            "intermediate.".format(file_size / 1024 / 1024)
        )

        try:
            if extension == ".csv":
                self._csv_to_parquet(path, temporary_path, file_size)
            else:
                self._excel_to_parquet(path, temporary_path, file_size)

            print("Parquet intermediate conversion complete.")
            return SourceData(
                frame=pl.scan_parquet(temporary_path),
                file_size_bytes=file_size,
                policy=policy,
                temporary_path=temporary_path,
            )
        except Exception:
            try:
                os.remove(temporary_path)
            except OSError:
                pass
            raise

    def _csv_to_parquet(
        self,
        source_path: str,
        parquet_path: str,
        file_size: int,
    ) -> None:
        skip_rows = self._skip_rows(source_path)

        try:
            frame = pl.scan_csv(
                source_path,
                skip_rows=skip_rows,
                separator=self.options.delimiter,
                low_memory=True,
            )
            frame = strip_bottom_metadata(frame)
            frame.sink_parquet(parquet_path, compression="zstd")
            return
        except Exception as exc:
            print(
                "Streaming CSV-to-Parquet conversion failed: {}. "
                "Falling back to an eager conversion.".format(exc)
            )

        MemoryManager.ensure_headroom(
            stage="source read before Parquet intermediate",
            input_size_bytes=file_size,
            multiplier=1.2,
            minimum_free_mb=self.options.minimum_free_ram_mb,
            safe_mode=self.options.safe_mode,
        )
        frame = pl.read_csv(
            source_path,
            skip_rows=skip_rows,
            separator=self.options.delimiter,
            low_memory=True,
        )
        frame = collect_frame(strip_bottom_metadata(frame))
        frame.write_parquet(parquet_path, compression="zstd")
        del frame
        gc.collect()

    def _excel_to_parquet(
        self,
        source_path: str,
        parquet_path: str,
        file_size: int,
    ) -> None:
        frame = self._read_excel(source_path, file_size)
        frame = collect_frame(strip_bottom_metadata(frame))
        frame.write_parquet(parquet_path, compression="zstd")
        del frame
        gc.collect()

    def _skip_rows(self, path: str) -> int:
        if self.options.header_row is not None:
            return max(0, self.options.header_row)
        return self._find_header_row(path)

    def _find_header_row(self, path: str) -> int:
        try:
            with open(
                path,
                "r",
                encoding="utf-8",
                errors="ignore",
                newline="",
            ) as handle:
                reader = csv.reader(handle, delimiter=self.options.delimiter)
                for row_number, row in enumerate(reader):
                    cleaned = [cell.strip() for cell in row]
                    if not cleaned or not any(cleaned) or len(cleaned) < 2:
                        continue

                    first_cell = cleaned[0].replace(".", "").replace("-", "")
                    contains_text = any(
                        any(character.isalnum() for character in cell)
                        for cell in cleaned
                    )
                    if not first_cell.isdigit() and contains_text:
                        return row_number
        except (OSError, csv.Error):
            pass

        return 0

    @staticmethod
    def _read_excel_compat(path: str) -> pl.DataFrame:
        errors = []

        # Polars' calamine engine is backed by fastexcel. Excel support is
        # optional so CSV-only users do not need either Excel dependency.
        for engine, dependency in (
            ("calamine", "fastexcel"),
            ("openpyxl", "openpyxl"),
        ):
            try:
                ensure_package(dependency)
            except RuntimeError as exc:
                errors.append("{} dependency: {}".format(engine, exc))
                continue

            try:
                return pl.read_excel(path, engine=engine)
            except Exception as exc:
                errors.append("{}: {}".format(engine, exc))

        try:
            return pl.read_excel(path)
        except Exception as exc:
            errors.append("default: {}".format(exc))

        raise RuntimeError(
            "Failed to read Excel file. {}".format(" | ".join(errors[-3:]))
        )

    @staticmethod
    def _temporary_parquet_path() -> str:
        descriptor, path = tempfile.mkstemp(prefix="dtabnk_", suffix=".parquet")
        os.close(descriptor)
        os.remove(path)
        return path


# ---------------------------------------------------------------------------
# Layout transformations
# ---------------------------------------------------------------------------


class PanelTransformer:
    """Transform supported source layouts into export-ready panel data."""

    def __init__(self, options: TransformOptions, file_size_bytes: int) -> None:
        self.options = options
        self.file_size_bytes = file_size_bytes

    def transform(
        self,
        frame: FrameLike,
        raw_columns_by_name: Optional[Dict[str, str]] = None,
    ) -> pl.DataFrame:
        layout = detect_layout(
            frame=frame,
            requested_layout=self.options.layout,
            year_column=self.options.year_column,
            value_column=self.options.value_column,
            raw_columns_by_name=raw_columns_by_name,
        )
        print("Info: Using layout '{}'.".format(layout))

        handlers = {
            "wide_header_series": self._header_series_wide,
            "wide": self._wide,
            "long": self._long,
            "year_rows": self._year_rows,
        }

        try:
            handler = handlers[layout]
        except KeyError:
            raise ValueError("Unsupported layout: {}".format(layout))

        if layout == "wide_header_series":
            return handler(frame, raw_columns_by_name)  # type: ignore[misc]
        return handler(frame)  # type: ignore[misc]

    def _header_series_wide(
        self,
        frame: FrameLike,
        raw_columns_by_name: Optional[Dict[str, str]],
    ) -> pl.DataFrame:
        id_column = resolve_id_column(frame, self.options.id_column)

        header_columns = []
        for column in get_columns(frame):
            if column == id_column:
                continue
            raw_name = (
                raw_columns_by_name.get(column, column)
                if raw_columns_by_name
                else column
            )
            if parse_header_series_column(raw_name):
                header_columns.append(column)

        if not header_columns:
            raise ValueError(
                "No header-encoded year/series columns found for "
                "wide_header_series layout."
            )

        frame = frame.select([id_column] + header_columns)
        self._guard_unpivot(len(header_columns))

        frame = frame.unpivot(
            index=[id_column],
            on=header_columns,
            variable_name="__Header__",
            value_name="Value",
        )

        if raw_columns_by_name:
            frame = frame.with_columns(
                pl.col("__Header__")
                .map_elements(
                    lambda value: raw_columns_by_name.get(value, value),
                    return_dtype=pl.Utf8,
                )
                .alias("__Raw_Header__")
            )
        else:
            frame = frame.with_columns(pl.col("__Header__").alias("__Raw_Header__"))

        frame = frame.with_columns(
            [
                pl.col("__Raw_Header__")
                .str.extract(HEADER_SERIES_PATTERN.pattern, group_index=1)
                .cast(pl.Int32, strict=False)
                .alias("Year"),
                pl.col("__Raw_Header__")
                .str.extract(HEADER_SERIES_PATTERN.pattern, group_index=2)
                .alias("Series"),
                pl.col("__Raw_Header__")
                .str.extract(HEADER_SERIES_PATTERN.pattern, group_index=3)
                .alias("Series_Code"),
            ]
        ).drop(["__Header__", "__Raw_Header__"])

        frame = cast_year_and_value(frame, "Year", "Value")

        if not self._can_pivot(frame):
            print(
                "Info: Skipping eager pivot due to memory guard; keeping long-form panel."
            )
            ordered = [id_column, "Year", "Series", "Series_Code", "Value"]
            ordered = [column for column in ordered if column in get_columns(frame)]
            return collect_frame(frame.select(ordered))

        self._guard_pivot(frame, multiplier=3.0 if self.options.safe_mode else 2.5)

        frame = frame.with_columns(
            pl.when(pl.col("Series_Code").is_not_null() & (pl.col("Series_Code") != ""))
            .then(
                pl.concat_str(
                    [
                        pl.col("Series"),
                        pl.lit(" ["),
                        pl.col("Series_Code"),
                        pl.lit("]"),
                    ]
                )
            )
            .otherwise(pl.col("Series"))
            .alias("Series_Key")
        )

        return self._pivot_and_sanitise(
            frame=frame,
            index=[id_column, "Year"],
            columns="Series_Key",
        )

    def _wide(self, frame: FrameLike) -> pl.DataFrame:
        frame = self._drop_code_columns(frame)

        series_column = resolve_column_name(
            frame,
            requested=self.options.series_column,
            fallbacks=SERIES_ALIASES,
            required=False,
            label="series column",
        )

        excluded = {series_column} if series_column else set()
        id_column = resolve_id_column(
            frame,
            self.options.id_column,
            exclude=excluded,
        )

        id_columns = [id_column]
        if series_column:
            id_columns.append(series_column)

        value_columns = [
            column
            for column in get_columns(frame)
            if column not in id_columns and is_year_like(column)
        ]
        if not value_columns:
            available = ", ".join(get_columns(frame)[:20])
            raise ValueError(
                "No year-like value columns found to unpivot. Available columns: {}".format(
                    available
                )
            )

        frame = frame.select(id_columns + value_columns)
        self._guard_unpivot(len(value_columns))
        frame = frame.unpivot(
            index=id_columns,
            on=value_columns,
            variable_name="Year",
            value_name="Value",
        )
        frame = cast_year_and_value(frame, "Year", "Value")

        if not series_column or series_column not in get_columns(frame):
            return collect_frame(frame)

        if self.options.safe_mode and MemoryManager.available_ram_mb() < max(
            2048, self.options.minimum_free_ram_mb * 2
        ):
            raise MemoryError(
                "Refusing pivot in --safe-mode: insufficient RAM headroom for eager pivot."
            )

        frame = frame.rename({series_column: "Series"})
        if not self._can_pivot(frame):
            print(
                "Info: Skipping eager pivot due to memory guard; keeping long-form panel."
            )
            return collect_frame(frame)

        self._guard_pivot(frame, multiplier=3.5 if self.options.safe_mode else 3.0)
        return self._pivot_and_sanitise(
            frame=frame,
            index=[id_column, "Year"],
            columns="Series",
        )

    def _long(self, frame: FrameLike) -> pl.DataFrame:
        frame = self._drop_code_columns(frame)

        year_column = resolve_column_name(
            frame,
            requested=self.options.year_column,
            fallbacks=YEAR_ALIASES,
            label="year column",
        )
        value_column = resolve_column_name(
            frame,
            requested=self.options.value_column,
            fallbacks=VALUE_ALIASES,
            exclude={year_column},
            label="value column",
        )
        series_column = resolve_column_name(
            frame,
            requested=self.options.series_column,
            fallbacks=SERIES_ALIASES,
            exclude={year_column, value_column},
            required=False,
            label="series column",
        )

        excluded = {year_column, value_column}
        if series_column:
            excluded.add(series_column)

        id_column = resolve_id_column(
            frame,
            self.options.id_column,
            exclude=excluded,
        )

        keep_columns = [id_column, year_column, value_column]
        if series_column:
            keep_columns.append(series_column)

        frame = frame.select(keep_columns).rename(
            {year_column: "Year", value_column: "Value"}
        )
        if series_column and series_column != "Series":
            frame = frame.rename({series_column: "Series"})

        frame = cast_year_and_value(frame, "Year", "Value")
        if "Series" not in get_columns(frame):
            return collect_frame(frame)

        if not self._can_pivot(frame):
            print(
                "Info: Skipping eager pivot due to memory guard; keeping long-form panel."
            )
            return collect_frame(frame)

        self._guard_pivot(frame, multiplier=3.0 if self.options.safe_mode else 2.5)
        return self._pivot_and_sanitise(
            frame=frame,
            index=[id_column, "Year"],
            columns="Series",
        )

    def _year_rows(self, frame: FrameLike) -> pl.DataFrame:
        year_column = resolve_column_name(
            frame,
            requested=self.options.year_column,
            fallbacks=YEAR_ALIASES,
            label="year column",
        )

        entity_columns = [
            column for column in get_columns(frame) if column != year_column
        ]
        if not entity_columns:
            raise ValueError("No entity columns found in year_rows layout.")

        output_id_column = sanitise_one(self.options.id_column)
        frame = frame.select([year_column] + entity_columns)
        self._guard_unpivot(len(entity_columns))
        frame = frame.unpivot(
            index=[year_column],
            on=entity_columns,
            variable_name=output_id_column,
            value_name="Value",
        )

        if year_column != "Year":
            frame = frame.rename({year_column: "Year"})

        frame = cast_year_and_value(frame, "Year", "Value")
        return collect_frame(frame)

    def _drop_code_columns(self, frame: FrameLike) -> FrameLike:
        columns = get_columns(frame)
        to_drop = [
            column for column in ("Country_Code", "Series_Code") if column in columns
        ]
        return frame.drop(to_drop) if to_drop else frame

    def _guard_unpivot(self, value_column_count: int) -> None:
        multiplier = max(2.0, min(8.0, value_column_count / 4))
        MemoryManager.ensure_headroom(
            stage="unpivot",
            input_size_bytes=self.file_size_bytes,
            multiplier=multiplier,
            minimum_free_mb=self.options.minimum_free_ram_mb,
            safe_mode=self.options.safe_mode,
        )

    def _guard_pivot(self, frame: FrameLike, multiplier: float) -> None:
        MemoryManager.ensure_headroom(
            stage="pivot",
            input_size_bytes=estimate_frame_bytes(frame, self.file_size_bytes),
            multiplier=multiplier,
            minimum_free_mb=max(
                self.options.minimum_free_ram_mb,
                1024 if self.options.safe_mode else self.options.minimum_free_ram_mb,
            ),
            safe_mode=self.options.safe_mode,
        )

    def _can_pivot(self, frame: FrameLike) -> bool:
        return MemoryManager.allow_pivot(
            frame=frame,
            fallback_bytes=self.file_size_bytes,
            minimum_free_ram_mb=self.options.minimum_free_ram_mb,
            safe_mode=self.options.safe_mode,
        )

    @staticmethod
    def _pivot_and_sanitise(
        frame: FrameLike,
        index: List[str],
        columns: str,
    ) -> pl.DataFrame:
        pivoted = pivot_eager(
            frame=frame,
            index=index,
            columns=columns,
            values="Value",
            aggregate_function="mean",
        )
        names = sanitise(pivoted.columns)
        return pivoted.rename(dict(zip(pivoted.columns, names)))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class PanelPipeline:
    """Coordinate reading, normalisation, layout detection and transformation."""

    def __init__(
        self,
        read_options: ReadOptions,
        transform_options: TransformOptions,
    ) -> None:
        self.reader = SourceReader(read_options)
        self.transform_options = transform_options

    def process(self, path: str) -> pl.DataFrame:
        source = self.reader.read(path)

        try:
            original_columns = get_columns(source.frame)
            sanitised_columns = sanitise(original_columns)
            raw_columns_by_name = dict(zip(sanitised_columns, original_columns))
            frame = source.frame.rename(dict(zip(original_columns, sanitised_columns)))

            transformer = PanelTransformer(
                options=self.transform_options,
                file_size_bytes=source.file_size_bytes,
            )
            return transformer.transform(
                frame,
                raw_columns_by_name=raw_columns_by_name,
            )
        finally:
            source.cleanup()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class Exporter:
    def __init__(
        self,
        stata_version: int,
        overwrite: bool,
        minimum_free_ram_mb: int,
        safe_mode: bool,
    ) -> None:
        self.stata_version = stata_version
        self.overwrite = overwrite
        self.minimum_free_ram_mb = minimum_free_ram_mb
        self.safe_mode = safe_mode

    @staticmethod
    def prepare(data: pl.DataFrame) -> pl.DataFrame:
        output = data

        if "Country" in output.columns and "Country_Name" in output.columns:
            output = output.drop("Country_Name")
        elif "Country_Name" in output.columns:
            output = output.rename({"Country_Name": "Country"})

        if "Year" not in output.columns:
            for column in output.columns:
                if str(column).lower() in {"year", "time", "date"}:
                    output = output.rename({column: "Year"})
                    break

        preferred = [
            column for column in ("Country", "Year") if column in output.columns
        ]
        remainder = [column for column in output.columns if column not in preferred]
        return output.select(preferred + remainder) if preferred else output

    @classmethod
    def preview(cls, data: pl.DataFrame, rows: int = DEFAULT_PREVIEW_ROWS) -> None:
        output = cls.prepare(data)

        print("\n=== Preview of export data ===")
        print("Rows: {}".format(output.height))
        print("Columns: {}".format(len(output.columns)))
        print("Column names:")
        print(", ".join(output.columns))
        print("\nFirst {} rows:".format(min(rows, output.height)))
        print(output.head(rows))
        print("=== End preview ===\n")

    def write(self, data: pl.DataFrame, base: str, file_format: str) -> None:
        try:
            extension = OUTPUT_EXTENSIONS[file_format]
        except KeyError as exc:
            raise ValueError(
                "Unsupported output format: {}".format(file_format)
            ) from exc

        output_path = "{}.{}".format(base, extension)

        if os.path.exists(output_path) and not self.overwrite:
            print(
                "Skipping {}: file already exists. Use --overwrite.".format(output_path)
            )
            return

        if (
            self.safe_mode
            and file_format == "rdata"
            and MemoryManager.available_ram_mb()
            < max(2048, self.minimum_free_ram_mb * 2)
        ):
            raise MemoryError(
                "Refusing export to {} in --safe-mode: insufficient RAM for conversion.".format(
                    file_format
                )
            )

        estimated_bytes = max(data.estimated_size(), 1)
        MemoryManager.ensure_headroom(
            stage="export to {}".format(file_format),
            input_size_bytes=estimated_bytes,
            multiplier=3.0 if file_format == "rdata" else 1.5,
            minimum_free_mb=max(
                self.minimum_free_ram_mb,
                1024 if file_format == "rdata" else self.minimum_free_ram_mb,
            ),
            safe_mode=self.safe_mode,
        )

        writers = {
            "dta": self._write_dta,
            "sav": self._write_sav,
            "rdata": self._write_rdata,
            "parquet": self._write_parquet,
        }
        writer = writers[file_format]

        output_directory = os.path.dirname(os.path.abspath(output_path))
        if not os.path.isdir(output_directory):
            raise FileNotFoundError(
                "Output directory does not exist: {}".format(output_directory)
            )

        descriptor, temporary_path = tempfile.mkstemp(
            prefix=".dtabnk_",
            suffix=".{}".format(extension),
            dir=output_directory,
        )
        os.close(descriptor)
        os.remove(temporary_path)

        try:
            writer(data, temporary_path)

            if not os.path.isfile(temporary_path):
                raise RuntimeError("writer did not create an output file")
            if os.path.getsize(temporary_path) == 0:
                raise RuntimeError("writer created an empty output file")

            os.replace(temporary_path, output_path)
            print("Wrote: {}".format(output_path))
        except Exception as exc:
            try:
                os.remove(temporary_path)
            except OSError:
                pass
            raise RuntimeError("Error writing {}: {}".format(output_path, exc)) from exc

    def _write_dta(self, data: pl.DataFrame, output_path: str) -> None:
        pyreadstat = ensure_package("pyreadstat")
        try:
            pyreadstat.write_dta(
                data,
                output_path,
                version=self.stata_version,
            )
        except (TypeError, AttributeError):
            pandas_frame = polars_to_pandas(data)
            try:
                pyreadstat.write_dta(
                    pandas_frame,
                    output_path,
                    version=self.stata_version,
                )
            finally:
                del pandas_frame
                gc.collect()

    @staticmethod
    def _write_sav(data: pl.DataFrame, output_path: str) -> None:
        pyreadstat = ensure_package("pyreadstat")
        try:
            pyreadstat.write_sav(data, output_path)
        except (TypeError, AttributeError):
            pandas_frame = polars_to_pandas(data)
            try:
                pyreadstat.write_sav(pandas_frame, output_path)
            finally:
                del pandas_frame
                gc.collect()

    @staticmethod
    def _write_rdata(data: pl.DataFrame, output_path: str) -> None:
        if shutil.which("R") is None and not os.environ.get("R_HOME"):
            raise RuntimeError(
                "R is required for --rdata but no R installation was found. "
                "Install R or set R_HOME, then try again."
            )

        ensure_package("rpy2")
        pandas_frame = normalise_time_column_name(polars_to_pandas(data))

        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri
        from rpy2.robjects.conversion import localconverter

        try:
            with localconverter(ro.default_converter + pandas2ri.converter):
                ro.globalenv["df"] = pandas_frame
            ro.globalenv["outfile"] = output_path
            ro.r("save(df, file=outfile)")
        finally:
            try:
                ro.r("rm(df, outfile); gc()")
            except Exception:
                pass
            del pandas_frame
            gc.collect()

    @staticmethod
    def _write_parquet(data: pl.DataFrame, output_path: str) -> None:
        data.write_parquet(output_path, compression="zstd")


def normalise_time_column_name(pandas_frame):
    for column in list(pandas_frame.columns):
        if str(column).lower() in {"year", "time", "date"}:
            if column != "Year":
                pandas_frame = pandas_frame.rename(columns={column: "Year"})
            break
    return pandas_frame


def polars_to_pandas(data: pl.DataFrame):
    """Convert to pandas only when an exporter actually requires it."""
    ensure_package("pandas")
    ensure_package("pyarrow")
    return data.to_pandas()


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "dtabnk: convert World Bank Open Data CSV/Excel files to panel datasets "
            "in Stata (default), SPSS, R and/or Parquet formats."
        )
    )

    parser.add_argument(
        "files",
        nargs="*",
        help="Input files (.csv, .xlsx, .xls).",
    )

    add_output_arguments(parser)
    add_layout_arguments(parser)
    add_memory_arguments(parser)
    add_input_arguments(parser)

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files without prompting.",
    )
    parser.add_argument(
        "--license",
        "--licence",
        dest="licence",
        action="store_true",
        help="Print software licence information and exit.",
    )

    return parser


def add_output_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("output")
    group.add_argument("--sav", action="store_true", help="Output SPSS/PSPP .sav file.")
    group.add_argument("--rdata", action="store_true", help="Output R .RData file.")
    group.add_argument(
        "--parquet-out",
        action="store_true",
        help="Output Parquet .parquet file.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Output all available formats (Stata, SPSS, R and Parquet).",
    )
    group.add_argument(
        "--out",
        nargs="*",
        help=(
            "Specify output filename(s) (default: input filename). The number "
            "of names must match the number of input files."
        ),
    )
    group.add_argument(
        "--stata",
        type=int,
        default=15,
        help="Specify Stata .dta version (11-15; default: 15).",
    )


def add_layout_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("layout")
    group.add_argument(
        "--id",
        default="Country_Name",
        help="Specify the entity ID column name (default: Country_Name).",
    )
    group.add_argument(
        "--layout",
        choices=["auto", "wide", "wide_header_series", "long", "year_rows"],
        default="auto",
        help=(
            "Specify input layout: auto, wide, wide_header_series, long or "
            "year_rows (default: auto)."
        ),
    )
    group.add_argument(
        "--year-col",
        default=None,
        help="Specify the year column for long or year_rows layouts.",
    )
    group.add_argument(
        "--value-col",
        default=None,
        help="Specify the value column for long layouts.",
    )
    group.add_argument(
        "--series-col",
        default=None,
        help="Specify the series column for wide or long layouts.",
    )


def add_memory_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("memory")
    group.add_argument(
        "--lazy",
        "--threshold",
        dest="lazy_threshold",
        type=int,
        default=None,
        help=(
            "Size threshold in MB for lazy CSV processing (default: automatic "
            "based on available RAM)."
        ),
    )
    group.add_argument(
        "--parquet",
        dest="parquet_threshold",
        type=int,
        default=None,
        help=(
            "Size threshold in MB for a Parquet intermediate (default: "
            "automatic based on available RAM)."
        ),
    )
    group.add_argument(
        "--min-free-ram",
        type=int,
        default=DEFAULT_MIN_FREE_RAM_MB,
        help="Minimum RAM in MB to retain as a safety reserve.",
    )
    group.add_argument(
        "--safe-mode",
        action="store_true",
        help=(
            "Use more conservative memory behaviour and stop before risky "
            "reshape/export steps."
        ),
    )


def add_input_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("input")
    group.add_argument(
        "--preview",
        action="store_true",
        help="Preview the export-shaped output before writing files.",
    )
    group.add_argument(
        "--preview-rows",
        type=int,
        default=DEFAULT_PREVIEW_ROWS,
        help="Number of preview rows to display (default: {}).".format(
            DEFAULT_PREVIEW_ROWS
        ),
    )
    group.add_argument(
        "--delimiter",
        default=",",
        help="Specify CSV delimiter (default: ',').",
    )
    group.add_argument(
        "--header-row",
        type=int,
        default=None,
        help="Override the detected CSV header row (0-based).",
    )


def validate_args(args: argparse.Namespace) -> None:
    if not args.files:
        raise SystemExit("Error: no input files provided. Use -h to view help.")

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

    if args.lazy_threshold is not None and args.lazy_threshold < 1:
        raise SystemExit("Error: --lazy/--threshold must be at least 1 MB.")

    if args.parquet_threshold is not None and args.parquet_threshold < 1:
        raise SystemExit("Error: --parquet must be at least 1 MB.")

    if len(args.delimiter.encode("utf-8")) != 1:
        raise SystemExit("Error: --delimiter must be a single-byte character.")

    if args.header_row is not None and args.header_row < 0:
        raise SystemExit("Error: --header-row must be 0 or greater.")


def output_formats(args: argparse.Namespace) -> List[str]:
    if args.all:
        return ["dta", "sav", "rdata", "parquet"]

    formats = ["dta"]
    if args.sav:
        formats.append("sav")
    if args.rdata:
        formats.append("rdata")
    if args.parquet_out:
        formats.append("parquet")
    return formats


def make_read_options(
    args: argparse.Namespace,
    multi_export: bool,
) -> ReadOptions:
    reshape_heavy = args.layout in {
        "auto",
        "wide",
        "wide_header_series",
        "long",
        "year_rows",
    }

    return ReadOptions(
        lazy_threshold_mb=args.lazy_threshold,
        parquet_threshold_mb=args.parquet_threshold,
        minimum_free_ram_mb=args.min_free_ram,
        safe_mode=args.safe_mode,
        delimiter=args.delimiter,
        header_row=args.header_row,
        reshape_heavy=reshape_heavy,
        multi_export=multi_export,
    )


def make_transform_options(args: argparse.Namespace) -> TransformOptions:
    return TransformOptions(
        id_column=args.id,
        layout=args.layout,
        year_column=args.year_col,
        value_column=args.value_col,
        series_column=args.series_col,
        minimum_free_ram_mb=args.min_free_ram,
        safe_mode=args.safe_mode,
    )


def process_inputs(args: argparse.Namespace) -> int:
    formats = output_formats(args)
    read_options = make_read_options(args, multi_export=len(formats) > 1)
    transform_options = make_transform_options(args)
    pipeline = PanelPipeline(read_options, transform_options)
    exporter = Exporter(
        stata_version=args.stata,
        overwrite=args.overwrite,
        minimum_free_ram_mb=args.min_free_ram,
        safe_mode=args.safe_mode,
    )

    failures = 0

    for index, input_file in enumerate(args.files):
        data = None
        export_data = None
        try:
            data = pipeline.process(input_file)

            if args.preview:
                exporter.preview(data, rows=args.preview_rows)

            export_data = exporter.prepare(data)
            base = args.out[index] if args.out else os.path.splitext(input_file)[0]

            export_failures = []
            for file_format in formats:
                try:
                    exporter.write(export_data, base, file_format)
                except Exception as exc:
                    export_failures.append((file_format, exc))
                    print(
                        "Error exporting {} as {}: {}".format(
                            input_file, file_format, exc
                        )
                    )

            if export_failures:
                failures += 1
                print("Completed with export errors: {}".format(input_file))
            else:
                print("Done: {}".format(input_file))
        except MemoryError as exc:
            print("Memory safety stop for {}: {}".format(input_file, exc))
            failures += 1
        except Exception as exc:
            print("Error processing {}: {}".format(input_file, exc))
            failures += 1
        finally:
            if export_data is not None:
                del export_data
            if data is not None:
                del data
            gc.collect()

    return failures


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.licence:
        print(LICENCE_TEXT)
        raise SystemExit(0)

    validate_args(args)
    failures = process_inputs(args)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
