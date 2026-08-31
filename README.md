# dtabnk

[![Python 3.8+](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![GPL-3.0-or-later](https://img.shields.io/badge/licence-GPL--3.0--or--later-A42E2B?logo=gnu&logoColor=white)](LICENSE)
[![Polars 1.0+](https://img.shields.io/badge/Polars-1.0%2B-CD792C?logo=polars&logoColor=white)](https://pola.rs/)

`dtabnk` is a memory-aware command-line tool for converting **World Bank Open Data CSV and Excel files into panel datasets**.

It supports Stata `.dta` output by default, with optional SPSS/PSPP `.sav`, R `.RData`, and Parquet `.parquet` output. It is designed for large datasets and low-RAM environments, while also supporting several common input layouts beyond the standard World Bank DataBank format.

## Features

### Conversion and Layouts

- **Multiple input formats**: reads `.csv`, `.xlsx`, and `.xls` files.
- **Multiple output formats**: writes Stata `.dta` by default, with optional SPSS/PSPP `.sav`, R `.RData`, and Parquet `.parquet` output.
- **Automatic layout detection**: detects `wide`, `wide_header_series`, `long`, and `year_rows` inputs.
- **Flexible entity mapping**: supports a custom entity column through `--id`, with fallback between common country-column names where possible.
- **Standardised export shape**: renames `Country_Name` to `Country` and standardises the time column as `Year` before export.
- **Variable sanitisation**: produces valid, unique statistical variable names and limits them to 32 characters for Stata compatibility.

### Performance and Memory Optimisation

- **RAM-aware processing**: adjusts processing thresholds according to available system memory.
- **Lazy CSV processing**: uses Polars lazy execution for larger CSV inputs where appropriate.
- **Parquet intermediates**: can use compressed Parquet as an intermediate representation for larger inputs.
- **Safe mode**: uses more conservative memory limits and refuses operations when estimated RAM headroom is insufficient.
- **Pivot guardrails**: can retain long-form data instead of attempting an unsafe eager pivot.
- **Low-memory reads**: uses lower-memory CSV reading paths where available.

### Data Cleaning

- **World Bank metadata removal**: strips footer rows such as `Data from database: ...` and `Last Updated: ...`.
- **Header detection**: detects and skips leading non-header rows in CSV files.
- **Year extraction**: extracts numeric years from headings such as `2015 [YR2015]`.
- **Header-series parsing**: recognises headings containing both year and indicator metadata, for example `2016 [YR2016] - GDP (current US$) [NY.GDP.MKTP.CD]`.
- **Year-column filtering**: in wide layouts, only year-like columns are reshaped into the panel time dimension.

### Safety and Convenience

- **Preview mode**: displays the export-shaped dataset before files are written.
- **Overwrite protection**: existing output files are skipped unless `--overwrite` is supplied.
- **Atomic output writing**: output is written to a temporary file first and moved into place only after a successful export.
- **Memory safety checks**: reshape and export operations are checked against available RAM before proceeding.
- **Dependency handling**: required Python packages are installed automatically when missing and `pip` is available; format-specific dependencies are loaded only when needed.
- **Excel fallbacks**: attempts compatible Excel engines when reading workbook files.

## Input Layouts

By default, `dtabnk` uses `--layout auto` and attempts to determine the input structure from its columns and headers.

### `wide`

Typical World Bank DataBank layout:

- one entity column, such as `Country_Name`
- an optional series column, such as `Series_Name`
- years spread across columns, such as `2010 [YR2010]` and `2011 [YR2011]`

### `wide_header_series`

Wide data where each value-column heading contains both the year and series information, for example:

```text
2016 [YR2016] - GDP (current US$) [NY.GDP.MKTP.CD]
```

### `long`

Already-long panel data containing:

- an entity column
- a year column
- a value column
- an optional series column

### `year_rows`

Matrix-style data containing:

- years in a dedicated column
- entities or variables spread across the remaining columns

### Automatic Detection Order

With `--layout auto`, detection proceeds in this order:

1. `wide_header_series` when a substantial share of non-ID columns match the supported year/series heading pattern
2. `long` when both a year-like and value-like column are present
3. `year_rows` when the first column is year-like and additional columns are present
4. `wide` as the fallback

For ambiguous or non-standard data, specify the layout manually with `--layout`.

## Requirements

- Python 3.8 or later
- `pip` only if missing Python dependencies need to be installed automatically
- Polars 1.0 or later; Polars 1.36.0 is not supported because of an upstream `unpivot` regression
- R is required only when exporting `.RData`

Core dependencies are checked at startup. Other dependencies, including Excel readers and statistical-format exporters, are loaded or installed only when required by the requested operation.

## Command-Line Options

| Flag | Description |
|------|-------------|
| `-h, --help` | Show the help message and exit. |
| `--sav` | Add SPSS/PSPP `.sav` output. |
| `--rdata` | Add R `.RData` output. |
| `--parquet-out` | Add Parquet `.parquet` output. |
| `--all` | Output Stata, SPSS/PSPP, R, and Parquet formats. |
| `--out` | Specify output base name(s). The number of names must match the number of input files. |
| `--id` | Entity ID column name. Default: `Country_Name`. |
| `--layout` | Input layout: `auto`, `wide`, `wide_header_series`, `long`, or `year_rows`. Default: `auto`. |
| `--year-col` | Year column name for `long` or `year_rows` layouts. |
| `--value-col` | Value column name for `long` layouts. |
| `--series-col` | Series column name for `wide` or `long` layouts. |
| `--stata` | Stata `.dta` version, from 11 to 15. Default: `15`. |
| `--lazy`, `--threshold` | Size threshold in MB for lazy CSV processing. Default: automatic based on available RAM. |
| `--parquet` | Size threshold in MB for use of a Parquet intermediate. Default: automatic based on available RAM. |
| `--min-free-ram` | Minimum RAM in MB to retain as a safety reserve. Default: `512`. |
| `--safe-mode` | Use more conservative memory behaviour and stop before risky reshape or export operations. |
| `--preview` | Preview the export-shaped output before writing files. |
| `--preview-rows` | Number of preview rows to display. Default: `10`. |
| `--delimiter` | CSV delimiter. Default: `,`. |
| `--header-row` | Override the detected CSV header row using a 0-based row number. |
| `--overwrite` | Replace existing output files. |
| `--licence`, `--license` | Print software licence information and exit. |

Stata output is always included unless the command exits before conversion. `--sav`, `--rdata`, and `--parquet-out` add those formats to the default `.dta` output; use `--all` to request every supported output format at once.

## Example Usage

```bash
# Display help
dtabnk -h

# Convert a CSV to Stata
dtabnk data.csv

# Convert to Stata, SPSS/PSPP, R, and Parquet
dtabnk data.csv --all

# Convert to Stata and Parquet
dtabnk data.csv --parquet-out

# Convert multiple files with custom output base names
dtabnk data1.csv data2.xlsx --out oingo boingo

# Write Stata version 13
dtabnk data.csv --stata 13

# Specify a custom entity column
dtabnk data.csv --id region

# Process an already-long dataset
dtabnk long.csv --layout long --id Country_Name --year-col Year --value-col Value

# Process a year-in-rows matrix
dtabnk matrix.csv --layout year_rows --year-col Year

# Process a file with year/series metadata embedded in its headers
dtabnk diff.xlsx --layout wide_header_series

# Preview the normalised output before writing
dtabnk data.csv --preview

# Set the lazy-processing threshold explicitly
dtabnk large.csv --threshold 128

# Set the Parquet-intermediate threshold explicitly
dtabnk large.csv --parquet 500

# Use conservative memory behaviour
dtabnk large.csv --safe-mode --preview

# Replace existing output files
dtabnk data.csv --overwrite

# Display licence information
dtabnk --licence
```

---

**🄯 Connor Baird, 2026**
