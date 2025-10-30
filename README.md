
# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.6%2B-blue.svg)](https://www.python.org/downloads/release/python-360/)  [![License](https://img.shields.io/badge/license-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.txt)  

`dtabnk` is a CLI tool to **convert World Bank OpenData CSV/Excel files to panel datasets** in STATA, SPSS, and/or R formats. It is compatible with the default DataBank layout.

## Features
- Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta`, SPSS/PSPP `.sav`, and/or R `.RData` panel datasets.
- Allows custom entity (`--id`) and time (`--time`) variables.
- Quiet mode and file overwrite handling
- Preview output(s) directly in the console.
- Dependencies can be automatically installed by dtabnk if `pip` is installed
## Command-Line Options

The following run options are available for `dtabnk`:

| Flag              | Description                                                                                                 |
|-------------------|-------------------------------------------------------------------------------------------------------------|
| `-h, --help`      | Show help message and exit.                                                                             |
| `--sav`           | Output SPSS/PSPP `.sav` file.                                                                               |
| `--rdata`         | Output R `.RData` file.                                                                                     |
| `--all`           | Output all available formats (STATA, SPSS, R).                                                              |
| `--out OUT`       | Specify the output filename (default: input filename).                                                      |
| `--id ID`         | Specify the entity (default: `Country`; the entity name is converted to the default format).                 |
| `--time TIME`     | Specify the time variable (default: `Year`; a numerical year is scraped from the variable).                 |
| `--stata STATA`   | Specify STATA version `.dta` output (8–15; default: 15).<br>STATA can read `.dta` files prepared for older versions. |
| `--license`       | Display software license information.                                                                  |
| `--quiet`         | Suppress command output to the terminal.                                                   |
| `--overwrite`     | Overwrite existing file(s) without prompting for confirmation.                                               |
| `--preview`       | Print the first 5 lines of the output file(s) to stdout (for previewing output).                            |

### Example Usage:

```bash
# Display help information
dtabnk (--help)

# Output to SPSS and R formats
dtabnk data.csv --sav --rdata

# Specify output in STATA 13.0
dtabnk data.csv --stata 13

# Convert and output all formats
dtabnk data.csv --all

# Specify custom output filename
dtabnk data.csv --out custom_filename

# Specify custom entity and time variable
dtabnk data.csv --id Region --time Period

# Preview the first 5 lines of the output file
dtabnk data.csv --preview
```
## Installation
`dtabnk` is a Python 3 script that can also be installed on Unix-like operating systems using the `INSTALL.sh` script.
Run:
```bash
git clone []
cd dtabnk
./dtabank.py
```
Feel free to remove the `.py` file extension it's just there for Windows users.

Install:
```bash
git clone []```
cd dtabnk
sudo ./INSTALL.sh
```


## Dependencies (Bundled in `.exe`)
- ≥Python 3.6
- colorama
- pandas
- pyreadstat
- rpy2
## License
This program is free software: you can redistribute it and/or modify it under the terms of the **GNU Lesser General Public License** as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with this program. If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

Copyright (C) 2025 Connor Baird




