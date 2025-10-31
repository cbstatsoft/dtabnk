#!/usr/bin/env python3

"""
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

Copyright (C) 2025 Connor Baird

Used Libraries:
    colorama <https://github.com/tartley/colorama>: BSD 3-Clause License
    pandas <https://github.com/pandas-dev/pandas>: BSD 3-Clause License
    pyreadstat <https://github.com/Roche/pyreadstat>: Apache License Version 2
    rpy2 <https://github.com/rpy2/rpy2>: GNU General Public License Version 2
"""

import subprocess
import sys
import os
import argparse


# Dependency check
def install_package(package_name):
    print(f"Installing {package_name}...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
    except subprocess.CalledProcessError as e:
        print_err(f"Failed to install {package_name}. Error: {e}")
        sys.exit(1)


def check_and_install_packages():
    required = ["pandas", "pyreadstat", "rpy2", "colorama"]
    missing = []

    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"Missing package(s): {', '.join(missing)}")
        if input("Install now? (Y/n): ").strip().lower() or "y" == "y":
            for pkg in missing:
                install_package(pkg)


check_and_install_packages()

import pandas as pd
import pyreadstat
import rpy2.robjects as ro
from rpy2.robjects import r
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

# Colour setup
try:
    from colorama import Fore, Style, init as color_init

    color_init()
    COLOUR_OK, COLOUR_WARN, COLOUR_ERR, COLOUR_RESET = (
        Fore.GREEN,
        Fore.YELLOW,
        Fore.RED,
        Style.RESET_ALL,
    )
except ImportError:
    COLOUR_OK = COLOUR_WARN = COLOUR_ERR = COLOUR_RESET = ""


# Utility print functions
def printv(msg, quiet=False):
    if not quiet:
        print(msg)


def print_warn(msg, quiet=False):
    if not quiet:
        print(f"{COLOUR_WARN}WARNING: {msg}{COLOUR_RESET}")


def print_err(msg, quiet=False):
    print(f"{COLOUR_ERR}ERROR: {msg}{COLOUR_RESET}")


def print_ok(msg, quiet=False):
    if not quiet:
        print(f"{COLOUR_OK}{msg}{COLOUR_RESET}")


# Column name sanitiser
def sanitise_column_names(columns, max_length=32, quiet=False):
    # Currency symbols mapping to currency codes
    currency_map = {
        "US$": "USD",
        "$": "USD",  # US Dollar
        "£": "GBP",  # British Pound
        "€": "EUR",  # Euro
        "¥": "JPY",  # Japanese Yen
        "₹": "INR",  # Indian Rupee
        "$CA": "CAD",  # Canadian Dollar
        "A$": "AUD",  # Australian Dollar
        "₣": "CHF",  # Swiss Franc
        "₩": "KRW",  # South Korean Won
    }

    reserved_keywords = {
        "if",
        "in",
        "using",
        "with",
        "for",
        "sum",
        "replace",
        "keep",
        "drop",
        "by",
        "end",
        "scalar",
        "byte",
        "int",
        "long",
        "float",
        "double",
        "str",
        "gen",
        "egen",
        "local",
        "global",
        "label",
    }
    sanitised = []
    seen = set()

    for col in columns:
        original_col = col

        # Replace currency symbols with corresponding currency codes
        for symbol, currency in currency_map.items():
            col = col.replace(symbol, currency)

        # Clean up column name (remove non-alphanumeric chars except underscore)
        col = "".join(
            ch if ch.isalnum() or ch == "_" else ""
            for ch in col.strip()
            .replace(" ", "_")
            .replace("%", "pct")
            .replace("-", "_")
        )

        # Ensure the column name starts with a letter
        if not col or not col[0].isalpha():
            col = "v_" + col

        # Truncate long names and handle STATA reserved words
        if len(col) > max_length:
            truncated_col = col[:max_length]
            if not quiet:
                print_warn(
                    f"Column '{original_col}' exceeds {max_length} chars. Suggested '{truncated_col}'"
                )
            # Ask user if they want to enter a custom name
            new_col = input(
                f"Column '{original_col}' exceeds {max_length} characters. Enter custom name (or press Enter to accept default '{truncated_col}'): "
            ).strip()
            col = new_col if new_col else truncated_col

        if col.lower() in reserved_keywords:
            col += "_var"

        # Ensure uniqueness of column names
        base = col
        counter = 1
        while col.lower() in seen:
            col = f"{base}_{counter}"
            counter += 1
        seen.add(col.lower())
        sanitised.append(col)

    return sanitised


# File safety
def sanitise_filename(filename, max_length=255, quiet=False):
    basename, ext = os.path.splitext(os.path.basename(filename))
    full_path = os.path.abspath(filename)
    if len(full_path) > max_length:
        (
            print_warn(
                f"Full path exceeds {max_length} chars. Suggesting '{basename}'", quiet
            )
            if not quiet
            else None
        )
    if len(basename) > max_length:
        basename = basename[:max_length]
        (
            print_warn(
                f"Filename '{basename}' too long. Suggesting '{basename}'", quiet
            )
            if not quiet
            else None
        )
    return basename + ext


def confirm_overwrite(output_file, quiet=False, overwrite=False):
    if os.path.exists(output_file) and not overwrite:
        user_input = (
            input(f"The file {output_file} exists. Overwrite? (Y/n): ").strip().lower()
        )
        return (
            quiet or user_input != "n"
        )  # Return True if input is anything other than "n" (no)
    return True


# Conversion functions
def convert_to_stata(
    df,
    output_file,
    id_var="Country",
    time_var="Year",
    stata_version=15,
    quiet=False,
    overwrite=False,
):
    output_file = sanitise_filename(output_file, quiet=quiet)
    if not output_file.endswith(".dta"):
        output_file += ".dta"
    if confirm_overwrite(output_file, quiet, overwrite):
        try:
            df_to_save = df.copy()
            if not pd.api.types.is_numeric_dtype(df_to_save[id_var]):
                df_to_save["ID"] = df_to_save[id_var].astype("category").cat.codes + 1
            else:
                df_to_save["ID"] = df_to_save[id_var]
            pyreadstat.write_dta(df_to_save, output_file, version=stata_version)
            print_ok(f"STATA {stata_version}+ .dta file written: {output_file}", quiet)
        except Exception as e:
            print_err(f"STATA file write failed: {e}", quiet)


def convert_to_spss(df, output_file, quiet=False, overwrite=False):
    output_file = sanitise_filename(output_file, quiet=quiet)
    if not output_file.endswith(".sav"):
        output_file += ".sav"
    if confirm_overwrite(output_file, quiet, overwrite):
        try:
            pyreadstat.write_sav(df, output_file)
            print_ok(f"SPSS/PSPP .sav file written: {output_file}", quiet)
        except Exception as e:
            print_err(f"SPSS/PSPP write failed: {e}", quiet)


def convert_to_rdata(df, output_file, quiet=False, overwrite=False):
    output_file = sanitise_filename(output_file, quiet=quiet)
    if not output_file.endswith(".RData"):
        output_file += ".RData"
    if confirm_overwrite(output_file, quiet, overwrite):
        try:
            with localconverter(ro.default_converter + pandas2ri.converter):
                ro.globalenv["df"] = df
            r(f"save(df, file='{output_file}')")
            print_ok(f"R .RData file written: {output_file}", quiet)
        except Exception as e:
            print_err(f"RData write failed: {e}", quiet)


# Data preparation
def convert_dataframe(input_file, id_var="Country", time_var="Year", quiet=False):
    if input_file.endswith(".csv"):
        data = pd.read_csv(input_file)
    elif input_file.endswith((".xlsx", ".xls")):
        data = pd.read_excel(input_file)
    else:
        raise ValueError("Unsupported file format: use .csv or .xlsx")

    data = data.dropna(how="all").replace("..", pd.NA)
    data.columns = sanitise_column_names(data.columns, quiet=quiet)
    if "Country_Name" in data.columns:
        data.rename(columns={"Country_Name": "Country"}, inplace=True)

    id_vars = [id_var, "Series_Name"] if "Series_Name" in data.columns else [id_var]
    data_long = data.drop(
        columns=["Country_Code", "Series_Code"], errors="ignore"
    ).melt(id_vars=id_vars, var_name=time_var, value_name="Value")

    data_long[time_var] = pd.to_numeric(
        data_long[time_var].astype(str).str.extract(r"(\d{4})")[0], errors="coerce"
    )
    data_long["Value"] = pd.to_numeric(data_long["Value"], errors="coerce")
    if "Series_Name" in data_long.columns:
        data_long.rename(columns={"Series_Name": "Series"}, inplace=True)

    if "Series" in data_long.columns:
        df_wide = data_long.pivot_table(
            index=[id_var, time_var], columns="Series", values="Value", aggfunc="mean"
        )
        df_wide.columns = sanitise_column_names(df_wide.columns, quiet=quiet)
        df_wide.reset_index(inplace=True)
    else:
        df_wide = data_long.copy()

    return df_wide


def preview_file(file_path, num_rows=5):
    try:
        # Check for .dta (STATA) file
        if file_path.endswith(".dta"):
            # Use pyreadstat to read .dta file (STATA)
            df, meta = pyreadstat.read_dta(file_path)
            if isinstance(df, pd.DataFrame):
                print_ok(f"Preview of {file_path}:")
                print(df.head(num_rows))
            else:
                print_err(f"Failed to read .dta file properly: {file_path}")
                df = None

        # Check for .sav (SPSS) file
        elif file_path.endswith(".sav"):
            # Use pyreadstat to read .sav file (SPSS)
            df, meta = pyreadstat.read_sav(file_path)
            if isinstance(df, pd.DataFrame):
                print_ok(f"Preview of {file_path}:")
                print(df.head(num_rows))
            else:
                print_err(f"Failed to read .sav file properly: {file_path}")
                df = None

        # Check for .RData file
        elif file_path.endswith(".RData"):
            # Load the RData file
            ro.r["load"](file_path)
            # Access the object from the global environment (assuming it's named 'df' in R)
            r_obj = ro.globalenv["df"]

            # Convert the R object to a pandas DataFrame (check if it's already a DataFrame)
            with localconverter(ro.default_converter + pandas2ri.converter):
                try:
                    df = pandas2ri.rpy2py(r_obj)
                except Exception as e:
                    print_err(f"Converting R object to pandas DataFrame: {e}")
                    df = None

            # If the conversion was successful and it's a DataFrame, show the preview
            if isinstance(df, pd.DataFrame):
                print_ok(f"Preview of {file_path}:")
                print(df.head(num_rows))
            else:
                print_err(
                    f"Failed to convert R object to pandas DataFrame for {file_path}"
                )
                df = None

        else:
            print_err(f"Unsupported file format: {file_path}")

        return df  # Return the DataFrame (or None if it couldn't be loaded)

    except Exception as e:
        print_err(f"previewing file {file_path}: {e}")


def confirm_overwrite_or_rename(output_file, quiet=False, overwrite=False):
    if os.path.exists(output_file) and not overwrite:
        user_input = (
            input(
                f"The file {output_file} exists. Do you want to (O)verwrite, (R)ename, or (S)kip? (O/R/S): "
            )
            .strip()
            .lower()
        )

        if user_input == "o":
            return True  # User chose to overwrite
        elif user_input == "r":
            # Ask for new name or automatically rename by appending a suffix
            base, ext = os.path.splitext(output_file)
            counter = 1
            new_file = f"{base}_{counter}{ext}"
            while os.path.exists(new_file):
                counter += 1
                new_file = f"{base}_{counter}{ext}"

            print(f"Renaming to {new_file}")
            return new_file  # Return the new filename with suffix
        elif user_input == "s":
            print(f"Skipping {output_file}")
            return None  # User chose to skip, return None
        else:
            print(f"Invalid choice. Skipping {output_file}")
            return None  # Invalid input, skipping
    return True  # No conflict, proceed as normal


# CLI
def main():
    parser = argparse.ArgumentParser(
        description="Convert World Bank OpenData CSV/Excel to panel dataset in STATA (default), SPSS and/or R format(s). Compatible with default DataBank layout."
    )
    # Modify input_file to accept multiple files
    parser.add_argument("input_files", nargs="*", help="Input CSV/Excel file(s)")
    parser.add_argument("--sav", action="store_true", help="Output SPSS/PSPP .sav file")
    parser.add_argument("--rdata", action="store_true", help="Output R .RData file")
    parser.add_argument("--all", action="store_true", help="Output all formats")
    parser.add_argument(
        "--out",
        nargs="*",
        help="Specify output filename(s)",
    )
    parser.add_argument(
        "--id",
        default="Country",
        help="Specify entity (default: Country; 'Country Name' changed to 'Country' automatically)",
    )
    parser.add_argument(
        "--time",
        default="Year",
        help="Specify time variable (default: Year; Letters etc. removed from variable)",
    )
    parser.add_argument(
        "--stata",
        type=int,
        default=15,
        help="Specify STATA version .dta output (8–15) (default: 15; STATA can read .dta files prepared for older versions)",
    )
    parser.add_argument(
        "--license",
        action="store_true",
        help="This software's license information",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress stdout")
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite file(s) without prompting"
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print first 5 lines of output file(s) to stdout",
    )

    args = parser.parse_args()
    quiet, overwrite = args.quiet, args.overwrite

    if args.license:
        print(__doc__) if not quiet else None
        return

    if not args.input_files:
        parser.print_help()
        return

    # Check if the number of --out files matches the number of input files
    if args.out and len(args.out) != len(args.input_files):
        print_err("The number of --out filenames must match the number of input files.")
        return

    # Formats check
    if args.all:
        formats = ["dta", "sav", "rdata"]
    else:
        formats = []
        if args.sav:
            formats.append("sav")
        if args.rdata:
            formats.append("rdata")
        if not formats:
            formats.append("dta")  # Default to STATA format if no other is specified

    # Loop over all input files
    for i, input_file in enumerate(args.input_files):
        print_ok(f"Processing file: {input_file}", quiet)

        # Check if a custom output filename is provided, otherwise use input file's base name
        base = args.out[i] if args.out else os.path.splitext(input_file)[0]

        # Convert the dataframe to the appropriate format
        df = convert_dataframe(
            input_file, id_var=args.id, time_var=args.time, quiet=quiet
        )

        if "dta" in formats:
            output_file = f"{base}.dta"
            result = confirm_overwrite_or_rename(
                output_file, quiet=quiet, overwrite=overwrite
            )
            if result is True:
                convert_to_stata(
                    df,
                    output_file,
                    id_var=args.id,
                    time_var=args.time,
                    stata_version=args.stata,
                    quiet=quiet,
                    overwrite=overwrite,
                )
                if args.preview:
                    preview_file(output_file)
            elif result is not None:
                # The file was renamed
                convert_to_stata(
                    df,
                    result,
                    id_var=args.id,
                    time_var=args.time,
                    stata_version=args.stata,
                    quiet=quiet,
                    overwrite=overwrite,
                )
                if args.preview:
                    preview_file(result)

        if "sav" in formats:
            output_file = f"{base}.sav"
            result = confirm_overwrite_or_rename(
                output_file, quiet=quiet, overwrite=overwrite
            )
            if result is True:
                convert_to_spss(df, output_file, quiet=quiet, overwrite=overwrite)
                if args.preview:
                    preview_file(output_file)
            elif result is not None:
                # The file was renamed
                convert_to_spss(df, result, quiet=quiet, overwrite=overwrite)
                if args.preview:
                    preview_file(result)

        if "rdata" in formats:
            output_file = f"{base}.RData"
            result = confirm_overwrite_or_rename(
                output_file, quiet=quiet, overwrite=overwrite
            )
            if result is True:
                convert_to_rdata(df, output_file, quiet=quiet, overwrite=overwrite)
                if args.preview:
                    preview_file(output_file)
            elif result is not None:
                # The file was renamed
                convert_to_rdata(df, result, quiet=quiet, overwrite=overwrite)
                if args.preview:
                    preview_file(result)

    print_ok("All files processed.", quiet)


if __name__ == "__main__":
    main()
