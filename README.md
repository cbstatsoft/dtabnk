# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Licence](https://img.shields.io/badge/licence-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![Polars](https://img.shields.io/badge/polars-fast-green.svg)](https://www.pola.rs/)

`dtabnk` is a memory-efficient CLI tool to **convert World Bank Open Data CSV/Excel files to panel datasets** in STATA (default), SPSS, and/or R formats. It is designed to be able to process large datasets, even in low-RAM environments. Compatible with the default DataBank layout, with *experimental* support for other common row/column layouts.

## Features

### Core Conversion
- **Multi-Format Export**: Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta` (default), SPSS/PSPP `.sav`, and/or R `.RData`.
- **Flexible Layout Support**: Supports default wide DataBank files, already-long panel-style files, and year-in-rows matrix-style inputs.
- **Variable Sanitisation**: Cleans column names (e.g. `'US$'` → `'USD'`, `'%'` → `'pct'`, `' '` → `'_'`).
- **Flexible Mapping**: Supports custom entity mapping via `--id` with intelligent fallback detection.
- **Standardised Export Shape**: Renames `Country_Name` to `Country` before export and standardises the time column as `Year`.

### Performance & Memory Optimisation
- **RAM-Aware Processing**: Adjusts processing strategy according to available system memory.
- **Parquet Intermediate**: For large files, can convert inputs to compressed Parquet first to reduce memory usage and I/O overhead.
- **Lazy Loading**: Uses Polars' streaming/lazy engine for large CSV files where possible.
- **Eager Loading**: Uses fast, direct loading for smaller files to minimise overhead.
- **Safe Mode**: Can refuse memory-risky reshape/export steps when RAM headroom is too low.

### Data Cleaning
- **Footer Metadata Stripping**: Automatically detects and removes World Bank footer lines (e.g. `"Data from database:..."`, `"Last Updated:..."`).
- **Header Metadata Skipping**: Scans the top of CSV files to skip non-header metadata lines before reading data.
- **Year Extraction**: Extracts year values from headers such as `2015 [YR2015]`.
- **Column Sanitisation**: Ensures all column names are valid, unique, and compatible with statistical software.

### Safety & Convenience
- **Safe Overwrite Protection**: Refuses to overwrite existing files unless `--overwrite` is explicitly used.
- **Preview Mode**: Shows the export-shaped output in the console before writing files.
- **Auto-Dependency Installation**: Automatically installs missing Python packages (Polars, PyReadStat, etc.) if `pip` is available.
- **Graceful Fallbacks**: Automatically switches between `calamine`, `fastexcel`, and `openpyxl` engines if one fails.

## Command-Line Options

The following run options are available for `dtabnk`:

| Flag | Description |
|------|-------------|
| `-h, --help` | Show help message and exit. |
| `--sav` | Output SPSS/PSPP `.sav` file. |
| `--rdata` | Output R `.RData` file. |
| `--all` | Output all available formats (STATA, SPSS, R). |
| `--out` | Specify the output filename(s) (default: input filename). Must match the number of input files. |
| `--id` | Specify the entity ID column name (default: `Country_Name`). Automatically falls back between `Country_Name` and `Country` where possible. |
| `--layout` | Specify input layout: `auto`, `wide`, `long`, or `year_rows` (default: `auto`). |
| `--year-col` | Specify the year column for `long` or `year_rows` layouts. |
| `--value-col` | Specify the value column for `long` layouts. |
| `--series-col` | Specify the series column for `wide` or `long` layouts. |
| `--stata` | Specify STATA `.dta` version (11–15; default: 15). |
| `--threshold` | Size (MB) threshold to switch to lazy CSV processing (default: auto based on available RAM). |
| `--parquet` | Size (MB) threshold to enable Parquet Intermediate processing (default: auto based on available RAM). |
| `--min-free-ram` | Minimum RAM (MB) to keep free as a safety reserve. |
| `--safe-mode` | Use more conservative memory behaviour and stop before risky reshape/export steps. |
| `--preview` | Preview the export-shaped output in the console before writing files. |
| `--preview-rows` | Number of preview rows to display (default: 10). |
| `--delimiter` | Specify CSV delimiter (default: `,`). |
| `--header-row` | Override detected CSV header row (0-based). |
| `--overwrite` | Overwrite existing output files without prompting. |
| `--license` | Print software licence information and exit. |

### Example Usage

```bash
# Display help information
dtabnk

# Convert a single CSV to STATA format
dtabnk data.csv

# Convert to STATA, SPSS, and R formats simultaneously
dtabnk data.csv --all

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

# Preview output before export
dtabnk data.csv --preview

# Force overwrite of existing output files
dtabnk data.csv --overwrite

# Process a large file with Parquet intermediate
dtabnk huge_data.xlsx --parquet 500 --all

# View licence information
dtabnk --license
```
---

**🄯 Connor Baird, 2026**
