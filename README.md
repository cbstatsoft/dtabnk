# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/) [![License](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html) [![Polars](https://img.shields.io/badge/polars-fast-green.svg)](https://www.pola.rs/)

`dtabnk` is a memory-efficient CLI tool to **convert World Bank OpenData CSV/Excel files to panel datasets** in STATA (default), SPSS, and/or R formats. It is designed to be able to process large datasets even in low-RAM environments. Compatible with the default DataBank layout.

## Features

### Core Conversion
- **Multi-Format Export**: Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta` (default), SPSS/PSPP `.sav`, and/or R `.RData`.
- **Variable Sanitisation**: Cleans column names (e.g., `'US$'` → `'USD'`, `'%'` → `'pct'`, `' '` → `'_'`).
- **Flexible Mapping**: Supports custom entity (`--id`) and time (`--time`) variable names with intelligent fallback detection.

### Performance & Memory Optimisation
- **Parquet Intermediate**: For files >500MB, automatically converts `.xlsx`/`.xls` to a compressed Parquet file first to drastically reduce memory usage and I/O time during processing.
- **Lazy Loading**: Uses `Polars`' streaming engine for `.csv`files >100MB to process datasets larger than available RAM.
- **Eager Loading**: Uses fast, direct loading for smaller files (<100MB) to minimise overhead.

### Data Cleaning
- **Footer Metadata Stripping**: Automatically detects and removes World Bank footer lines (e.g., "Data from database:...", "Last Updated:...").
- **Header Metadata Skipping**: Scans the top of CSV files to skip non-header metadata lines before reading data.
- **Column Sanitisation**: Ensures all column names are valid, unique, and compatible with statistical software.

### Safety & Convenience
- **Safe Overwrite Protection**: Refuses to overwrite existing files unless `--overwrite` is explicitly used.
- **Auto-Dependency Installation**: Automatically installs missing Python packages (Polars, PyReadStat, etc.) if `pip` is available.
- **Graceful Fallbacks**: Automatically switches between `fastexcel` and `openpyxl` engines if one fails.

## Command-Line Options

The following run options are available for `dtabnk`:

| Flag | Description |
|------|-------------|
| `-h, --help` | Show help message and exit. |
| `--sav` | Output SPSS/PSPP `.sav` file. |
| `--rdata` | Output R `.RData` file. |
| `--all` | Output all available formats (STATA, SPSS, R). |
| `--out` | Specify the output filename(s) (default: input filename). Must match the number of input files. |
| `--id` | Specify the entity ID column name (default: `Country`). Automatically maps `Country_Name` to `Country`. |
| `--time` | Specify the time variable name (default: `Year`). Automatically extracts years from column headers. |
| `--stata` | Specify STATA `.dta` version (8–15; default: 15). |
| `--parquet` | Size (MB) threshold to enable Parquet Intermediate processing (default: 500). |
| `--overwrite` | Overwrite existing output files without prompting. |
| `--license` | Print software license information and exit. |

### Example Usage

```bash
# Display help information
dtabnk

# Convert a single CSV to STATA format
dtabnk data.csv

# Convert to STATA, SPSS, and R formats simultaneously
dtabnk data.csv --all

# Convert multiple files with custom output names
dtabnk data.csv data.xlsx --out oingo boingo

# Convert to STATA version 13 format
dtabnk data.csv --stata 13

# Specify custom entity and time columns
dtabnk data.csv --id region --time period

# Force overwrite of existing output files
dtabnk data.csv --overwrite

# Process a large Excel file using Parquet Intermediate (>500MB)
dtabnk huge_data.xlsx --parquet 500 --all

# View license information
dtabnk --license
```

*(C) Connor Baird 2026*
