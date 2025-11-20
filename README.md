
# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.7.17%2B-blue.svg)](https://www.python.org/downloads/release/python-3717/)  [![License](https://img.shields.io/badge/license-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.html)  

`dtabnk` is a CLI tool to **convert World Bank OpenData CSV/Excel files to panel datasets** in STATA (default), SPSS, and/or R formats. It is compatible with the default DataBank layout.

## Features
- Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta` (default), SPSS/PSPP `.sav`, and/or R `.RData` panel datasets.
- Generates a new column in `.dta` files with entity ID numbers, as STATA does not support using strings as entity names.
- Sanitises variable names (e.g. 'US$' → 'USD', '%' → 'pct', ' ' → '_').
- Allows custom entity (`--id`) and time (`--time`) variables.
- Perform Durbin–Wu–**Hausman test(s)** on input variable(s).
- Duplicate output filename handling via autorename or overwrite.
- Preview output files(s) directly in the console.
- Dependencies can be automatically installed by dtabnk if `pip` is installed
## Command-Line Options

The following run options are available for `dtabnk`:

| Flag              | Description                                                                                                 |
|-------------------|-------------------------------------------------------------------------------------------------------------|
| `-h, --help`      | Show help message and exit.                                                                                 |
| `--sav`           | Output SPSS/PSPP `.sav` file.                                                                               |
| `--rdata`         | Output R `.RData` file.                                                                                     |
| `--all`           | Output all available formats (STATA, SPSS, R).                                                              |
| `--out    `       | Specify the output filename(s) (default: input filename).                                                   |
| `--id`            | Specify the entity (default: `Country`).<br>DataBank 'Country Name' converted to 'Country' automatically.   |
| `--time`          | Specify the time variable (default: `Year`).<br>Letters etc. removed automatically.                         |
| `--stata`         | Specify STATA version `.dta` output (8–15; default: 15).<br>STATA can read `.dta` files prepared for older versions.|
| `--license`       | Print software license information in stdout.                                                               |
| `--quiet`         | Suppress command outputs in the terminal unless user input is required.                                     |
| `--overwrite`     | Overwrite existing file(s) without prompting for confirmation.                                              |
| `--preview`       | Print the first 5 lines of the output file(s) in stdout.                                                    |
| `--hausman`       | Perform the Hausman test iteratively, using each variable as the dependent variable in turn, and testing it against all other variables as independent variables.  |
| `--dep`           | Specify post-sanitised dependent variable name for Hausman test(s).                                         |
| `--indep`         | Specify post-sanitised independent variable name(s) for Hausman test(s)                                     |

### Example usage:

```bash
# display help information
dtabnk

# convert to STATA 15+ .dta format
dtabnk data.csv

# convert to STATA 15+ .dta format and perform Hausman test on data.csv using Gini_index as the dependent variable and all other variables as the independent variables:
dtabnk data.csv --hausman --dep Gini_index

# convert data.csv and data.xlsx to oingo.dta and boingo.dta respectively
dtabnk data.csv data.xlsx --out oingo boingo

# convert data.csv and data.xlsx to STATA 15+ .dta format, dtabnk will ask if you wish to overwrite, rename to data_1, or skip data.xlsx (O/R/S)
dtabnk data.csv data.xlsx

# convert to SPSS/PSPP and R formats
dtabnk data.csv --sav --rdata

# convert to STATA 13+ .dta format
dtabnk data.csv --stata 13

# convert to all formats
dtabnk data.csv --all

# specify custom output filename
dtabnk data.csv --out custom_filename

# specify custom entity and time variables
dtabnk data.csv --id region --time period

# preview the first 5 lines of the output file(s)
dtabnk data.csv --preview
```
## Installation
`dtabnk.py` can run on any system with Python 3 installed. It can also be installed and uninstalled on Unix-like operating systems using the appropriate `.sh` scripts.
```bash
git clone https://github.com/cbstatsoft/dtabnk.git
chmod a+x ./dtabnk/install.sh
./dtabnk/install.sh

chmod a+x ./dtabnk/uninstall.sh
./dtabnk/uninstall.sh
```
`dtabnk.py` can be executed directly on Unix-like operating systems without invoking `python`/`python3` as the interpreter.
```bash
git clone https://github.com/cbstatsoft/dtabnk.git
cd dtabnk
mv dtabnk.py dtabnk
chmod a+x dtabnk
./dtabnk
```
## Dependencies
- ≥Python 3.17.7 (This is the earliest version I have personally tested that worked)
- [colorama](https://github.com/tartley/colorama)   BSD 3-Clause License
- [numpy](https://github.com/numpy/numpy)           BSD License
- [openpyxl](https://github.com/shshe/openpyxl)	    MIT License
- [pandas](https://github.com/pandas-dev/pandas)    BSD 3-Clause License
- [pyreadstat](https://github.com/Roche/pyreadstat) Apache License Version 2
- [rpy2](https://github.com/rpy2/rpy2)              GNU General Public License Version 2
- [statsmodels](https://github.com/statsmodels/statsmodels) BSD 3-Clause license
- [scipy](https://github.com/scipy/scipy)            BSD 3-Clause License

*Copyright (C) 2025 Connor Baird*
