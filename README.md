
# dtabnk

[![Python Version](https://img.shields.io/badge/python-3.6%2B-blue.svg)](https://www.python.org/downloads/release/python-360/)  [![License](https://img.shields.io/badge/license-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.txt)  

`dtabnk` is a CLI tool to **convert World Bank OpenData CSV/Excel files to panel datasets** in STATA (default), SPSS, and/or R formats. It is compatible with the default DataBank layout.

## Features
- Converts `.csv`/`.xlsx`/`.xls` files to STATA `.dta` (default), SPSS/PSPP `.sav`, and/or R `.RData` panel datasets.
- Generates a new column in `.dta` files with entity ID numbers, as STATA does not support using strings as entity names.
- Allows custom entity (`--id`) and time (`--time`) variables.
- Duplicate output filename handling via autorename or overwrite.
- Preview output files(s) directly in the console.
- Dependencies can be automatically installed by dtabnk if `pip` is installed
## Command-Line Options

The following run options are available for `dtabnk`:

| Flag              | Description                                                                                                 |
|-------------------|-------------------------------------------------------------------------------------------------------------|
| `-h, --help`      | Show help message and exit.                                                                             |
| `--sav`           | Output SPSS/PSPP `.sav` file.                                                                               |
| `--rdata`         | Output R `.RData` file.                                                                                     |
| `--all`           | Output all available formats (STATA, SPSS, R).                                                              |
| `--out`       | Specify the output filename(s) (default: input filename).                                                      |
| `--id`         | Specify the entity (default: `Country`).<br>DataBank 'Country Name' converted to 'Country' automatically.                 |
| `--time`     | Specify the time variable (default: `Year`).<br>Letters etc. removed automatically.                 |
| `--stata`   | Specify STATA version `.dta` output (8–15; default: 15).<br>STATA can read `.dta` files prepared for older versions. |
| `--license`       | Print software license information in stdout.                                                                  |
| `--quiet`         | Suppress command outputs in the terminal.                                                   |
| `--overwrite`     | Overwrite existing file(s) without prompting for confirmation.                                               |
| `--preview`       | Print the first 5 lines of the output file(s) in stdout.                            |

### Example usage:

```bash
# display help information
dtabnk (--help)

# convert to STATA 15+ .dta format
dtabnk data.csv

# convert data.csv and data.xlsx to foo.dta and bar.dta respectively
dtabnk data.csv data.xlsx --out data1 data2

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
- ≥Python 3.6
- colorama
- pandas
- pyreadstat
- rpy2
## License
This program is free software: you can redistribute it and/or modify it under the terms of the **GNU Lesser General Public License** as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with this program. If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

Copyright (C) 2025 Connor Baird
