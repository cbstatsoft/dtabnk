# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Licence](https://img.shields.io/badge/licence-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![Polars](https://img.shields.io/badge/polars-fast-green.svg)](https://www.pola.rs/)

`dtabnk` is a memory-aware CLI tool to **convert World Bank Open Data CSV/Excel files to panel datasets** in STATA `.dta` (default), SPSS `.sav`, R `.RData`, and/or Parquet `.parquet` formats. It is designed to be able to process large datasets, even in low-RAM environments. Compatible with the default DataBank layout, with additional support for already-long panel-style files, year-in-rows matrix-style inputs, and wide files with year/series metadata embedded in column headers.

## Features

### Core Conversion
- **Multi-Format Export**: Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta` (default), SPSS/PSPP `.sav`, R `.RData`, and/or Parquet `.parquet`.
- **Flexible Layout Support**: Supports default wide DataBank files, already-long panel-style files, year-in-rows matrix-style inputs, and wide files with series metadata embedded in year/value column headers.
- **Format Detection**: Detects `long`, `wide`, `wide_header_series`, and `year_rows` input layouts automatically, including World Bank exports with year-and-indicator metadata embedded in column headers.
- **Variable Sanitisation**: Cleans column names (e.g. `'US$'` → `'USD'`, `'%'` → `'pct'`, `' '` → `'_'`).
- **Flexible Mapping**: Supports custom entity mapping via `--id` with intelligent fallback detection.
- **Standardised Export Shape**: Renames `Country_Name` to `Country` before export and standardises the time column as `Year`.

### Performance & Memory Optimisation
- **RAM-Aware Processing**: Adjusts processing strategy according to available system memory.
- **Parquet Intermediate**: For larger files, can convert inputs to compressed Parquet first to reduce memory usage and I/O overhead.
- **Lazy Loading**: Uses Polars' streaming/lazy engine for large CSV files where possible.
- **Eager Loading**: Uses fast, direct loading for smaller files to minimise overhead.
- **Safe Mode**: Can refuse memory-risky reshape/export steps when RAM headroom is too low.
- **Pivot Guardrails**: Can skip eager pivoting and keep data in long form when reshaping would be too memory-intensive.

### Data Cleaning
- **Footer Metadata Stripping**: Automatically detects and removes World Bank footer lines (e.g. `"Data from database:..."`, `"Last Updated:..."`).
- **Header Metadata Skipping**: Scans the top of CSV files to skip non-header metadata lines before reading data.
- **Year Extraction**: Extracts year values from headers such as `2015 [YR2015]`.
- **Header-Series Parsing**: Detects wide files whose headers combine year and indicator metadata, such as `2016 [YR2016] - GDP (current US$) [NY.GDP.MKTP.CD]`.
- **Column Sanitisation**: Ensures all column names are valid, unique, and compatible with statistical software.

### Safety & Convenience
- **Safe Overwrite Protection**: Refuses to overwrite existing files unless `--overwrite` is explicitly used.
- **Preview Mode**: Shows the export-shaped output in the console before writing files.
- **Auto-Dependency Installation**: Automatically installs missing Python packages (Polars, PyReadStat, etc.) if `pip` is available.
- **Graceful Fallbacks**: Automatically switches between `calamine` and `openpyxl` engines if one fails.

## Auto-Detection

By default, `dtabnk` attempts to detect the input layout automatically.

It can detect these layouts:

- **`long`**: explicit year and value columns, with an optional series column
- **`wide`**: one ID column, an optional separate series column, and year-like value columns such as `1960`, `2001`, or similar headers
- **`wide_header_series`**: World Bank-style wide files where each value column header encodes both the year and the indicator, for example `2016 [YR2016] - GDP (current US$) [NY.GDP.MKTP.CD]`
- **`year_rows`**: the first column is a year/time column and the remaining columns are entities or variables

Detection is based on column names and header patterns after column sanitisation.

The detection order is:

1. **`wide_header_series`** if a substantial share of non-ID columns match a `year - series [code]` pattern
2. **`long`** if both a year-like column and a value-like column are found
3. **`year_rows`** if the first column is year-like and there are additional columns
4. otherwise, the input falls back to **`wide`**

If the input structure is ambiguous or non-standard, the layout can still be specified manually with `--layout`.

## Command-Line Options

The following run options are available for `dtabnk`:

| Flag | Description |
|------|-------------|
| `-h, --help` | Show help message and exit. |
| `--sav` | Output SPSS/PSPP `.sav` file. |
| `--rdata` | Output R `.RData` file. |
| `--parquet-out` | Output Parquet `.parquet` file. |
| `--all` | Output all available formats (STATA, SPSS, R, Parquet). |
| `--out` | Specify the output filename(s) (default: input filename). Must match the number of input files. |
| `--id` | Specify the entity ID column name (default: `Country_Name`). Automatically falls back between `Country_Name` and `Country` where possible. |
| `--layout` | Specify input layout: `auto`, `wide`, `wide_header_series`, `long`, or `year_rows` (default: `auto`). |
| `--year-col` | Specify the year column for `long` or `year_rows` layouts. |
| `--value-col` | Specify the value column for `long` layouts. |
| `--series-col` | Specify the series column for `wide` or `long` layouts. |
| `--stata` | Specify STATA `.dta` version (11–15; default: 15). |
| `--lazy` | Size (MB) threshold to switch to lazy CSV processing (default: auto based on available RAM). |
| `--parquet` | Size (MB) threshold to enable Parquet intermediate processing (default: auto based on available RAM). |
| `--min-free-ram` | Minimum RAM (MB) to keep free as a safety reserve (default: 512). |
| `--safe-mode` | Use more conservative memory behaviour and stop before risky reshape/export steps. |
| `--preview` | Preview the export-shaped output in the console before writing files. |
| `--preview-rows` | Number of preview rows to display (default: 10). |
| `--delimiter` | Specify CSV delimiter (default: `,`). |
| `--header-row` | Override detected CSV header row (0-based). |
| `--overwrite` | Overwrite existing output files without prompting. |
| `--license`, `--licence` | Print software licence information and exit. |

### Example Usage

```bash
# Display help information
dtabnk

# Convert a single CSV to STATA format
dtabnk data.csv

# Convert to STATA, SPSS, R, and Parquet formats simultaneously
dtabnk data.csv --all

# Convert to Parquet only
dtabnk data.csv --parquet-out

# Convert multiple files with custom output names
dtabnk data1.csv data2.xlsx --out oingo boingo

# Convert to STATA version 13 format
dtabnk data.csv --stata 13

# Specify a custom entity column
dtabnk data.csv --id region

# Process an already-long dataset
dtabnk data.csv --layout long --id Country_Name --year-col Year --value-col Value

# Process a year-in-rows matrix-style file
dtabnk data.csv --layout year_rows --year-col Year

# Process a wide file with year/series metadata embedded in column headers
dtabnk diff.xlsx --layout wide_header_series

# Preview output before export
dtabnk data.csv --preview

# Force overwrite of existing output files
dtabnk data.csv --overwrite

# View licence information
dtabnk --licence
dtabnk --license
```
---

**🄯** Connor Baird, 2026
