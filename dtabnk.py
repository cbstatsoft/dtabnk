#!/usr/bin/env python3
# (C) Connor Baird 2026 GNU GPL-3.0-or-later

import argparse
import os
import sys
import subprocess
import gc

REQ = ["polars", "pyreadstat", "rpy2", "openpyxl", "fastexcel"]

def ensure():
    missing = []
    for p in REQ:
        try:
            __import__(p)
        except ImportError:
            missing.append(p)
    if missing:
        print(f"Installing missing packages: {', '.join(missing)}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        except subprocess.CalledProcessError:
            sys.exit("Failed to install dependencies.")

ensure()

import polars as pl
import pyreadstat
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

DEFAULT_LAZY_THRESHOLD = 100
DEFAULT_PARQUET_THRESHOLD = 500

LICENSE_TEXT = """
GNU General Public Licence v3.0 or later

This programme is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public Licence as published by
the Free Software Foundation, either version 3 of the Licence, or
(at your option) any later version.

This programme is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public Licence for more details.

You should have received a copy of the GNU General Public Licence
along with this programme.  If not, see <https://www.gnu.org/licenses/>.

(C) Connor Baird 2026
"""

def find_header_row(path):
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if ',' in line and any(c.isalnum() for c in line):
                    parts = line.split(',')
                    if len(parts) > 1:
                        if not parts[0].strip().replace('.', '').replace('-', '').isdigit():
                            return i
        return 0
    except Exception:
        return 0

def strip_bottom_metadata(df):
    if df.height == 0:
        return df
    first_col = df.columns[0]
    pattern_db = r"(?i)^Data from database:"
    pattern_updated = r"(?i)^Last Updated:"
    is_metadata = (
        pl.col(first_col).str.contains(pattern_db) |
        pl.col(first_col).str.contains(pattern_updated)
    )
    return df.filter(~is_metadata)

def sanitise(cols, max_len=32):
    seen, out = set(), []
    for c in cols:
        c = str(c).replace(" ", "_").replace("%", "pct")
        c = "".join(ch for ch in c if ch.isalnum() or ch == "_")
        if not c or not c[0].isalpha():
            c = "v_" + c
        c = c[:max_len]
        base, i = c, 1
        while c in seen:
            c = f"{base}_{i}"; i += 1
        seen.add(c); out.append(c)
    return out

def collect_with_engine(lf):
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)

def process_file(path, id_var, time_var, lazy_thresh, parquet_thresh):
    ext = os.path.splitext(path)[1].lower()
    file_size = os.path.getsize(path)
    
    use_parquet = file_size > parquet_thresh * 1024 * 1024
    use_lazy = file_size > lazy_thresh * 1024 * 1024
    
    lf = None
    temp_parquet_path = None

    try:
        if use_parquet:
            print(f"Large file ({file_size / 1024 / 1024:.1f} MB). Using Parquet Intermediate for efficiency...")
            temp_parquet_path = path + ".parquet.tmp"
            
            try:
                if ext == ".csv":
                    df_src = pl.read_csv(path, skip_rows=find_header_row(path))
                elif ext in [".xlsx", ".xls"]:
                    try:
                        df_src = pl.read_excel(path, engine="fastexcel")
                    except Exception:
                        df_src = pl.read_excel(path, engine="openpyxl")
                else:
                    raise ValueError("Unsupported format")
                
                df_src.write_parquet(temp_parquet_path, compression="zstd")
                del df_src
                gc.collect()
                lf = pl.scan_parquet(temp_parquet_path)
                print("Parquet intermediate conversion complete.")
            except Exception as e:
                print(f"Parquet intermediate failed: {e}. Falling back.")
                lf = None

        if lf is None:
            if ext == ".csv":
                if use_lazy:
                    try:
                        lf = pl.scan_csv(path, skip_rows=find_header_row(path))
                    except AttributeError:
                        print("Lazy CSV unsupported. Using eager.")
                        lf = pl.read_csv(path, skip_rows=find_header_row(path))
                else:
                    lf = pl.read_csv(path, skip_rows=find_header_row(path))
            elif ext in [".xlsx", ".xls"]:
                try:
                    lf = pl.read_excel(path, engine="fastexcel")
                except Exception:
                    lf = pl.read_excel(path, engine="openpyxl")
            else:
                raise ValueError("Unsupported format")

        
        if isinstance(lf, pl.LazyFrame):
            df_temp = collect_with_engine(lf)
            df_temp = strip_bottom_metadata(df_temp)
            lf = df_temp
        else:
            lf = strip_bottom_metadata(lf)

        schema = lf.columns if isinstance(lf, pl.DataFrame) else collect_with_engine(lf.head(0)).columns
        new_cols = sanitise(schema)
        lf = lf.rename(dict(zip(schema, new_cols)))

        actual_id_var = id_var
        if id_var == "Country" and "Country_Name" in lf.columns:
            actual_id_var = "Country_Name"
        elif id_var not in lf.columns:
            matches = [c for c in lf.columns if c.lower() == id_var.lower()]
            if matches:
                actual_id_var = matches[0]
                print(f"Info: Using '{actual_id_var}' for ID column '{id_var}'.")
            else:
                avail = ", ".join(list(lf.columns)[:10])
                if len(lf.columns) > 10: avail += "..."
                raise ValueError(f"ID column '{id_var}' not found.\nAvailable: {avail}")

        actual_time_var = time_var
        if time_var == "Year":
            year_like = [c for c in lf.columns if any(ch.isdigit() for ch in c) and c not in [actual_id_var]]
            if year_like:
                actual_time_var = year_like[0]
                print(f"Info: Using '{actual_time_var}' for Time column '{time_var}'.")
        elif time_var not in lf.columns:
            matches = [c for c in lf.columns if c.lower() == time_var.lower()]
            if matches:
                actual_time_var = matches[0]
                print(f"Info: Using '{actual_time_var}' for Time column '{time_var}'.")
            else:
                avail = ", ".join(list(lf.columns)[:10])
                if len(lf.columns) > 10: avail += "..."
                raise ValueError(f"Time column '{time_var}' not found.\nAvailable: {avail}")

        has_series = "Series_Name" in lf.columns
        id_vars = [actual_id_var] + (["Series_Name"] if has_series else [])
        drop_cols = [c for c in ["Country_Code", "Series_Code"] if c in lf.columns]
        if drop_cols:
            lf = lf.drop(drop_cols)

        value_vars = [c for c in lf.columns if c not in id_vars]
        if not value_vars:
            raise ValueError("No value columns found to unpivot.")

        lf = lf.unpivot(index=id_vars, on=value_vars, variable_name=actual_time_var, value_name="Value")
        lf = lf.with_columns([
            pl.col(actual_time_var).cast(pl.Utf8).str.extract(r"(\d{4})").cast(pl.Int32, strict=False).alias(actual_time_var),
            pl.when(pl.col("Value") == "..").then(None).otherwise(pl.col("Value")).cast(pl.Float64, strict=False).alias("Value")
        ])

        if has_series:
            lf = lf.rename({"Series_Name": "Series"})
            lf = lf.pivot(values="Value", index=[actual_id_var, actual_time_var], on="Series", aggregate_function="mean")
            p_schema = lf.columns if isinstance(lf, pl.DataFrame) else collect_with_engine(lf.head(0)).columns
            p_new = sanitise(p_schema)
            lf = lf.rename(dict(zip(p_schema, p_new)))

        if isinstance(lf, pl.LazyFrame):
            try:
                return collect_with_engine(lf)
            except Exception as e:
                print(f"Streaming failed, falling back to eager: {e}")
                return lf.collect()
        return lf

    finally:
        if temp_parquet_path and os.path.exists(temp_parquet_path):
            try:
                os.remove(temp_parquet_path)
            except Exception:
                pass

def write(df, base, fmt, stata_version, overwrite=False):
    pdf = df.to_pandas()
    
    time_col_found = False
    for col in pdf.columns:
        if col.lower() in ["year", "time", "date"] or (col.startswith("v_") and any(c.isdigit() for c in col)):
            if col != "year":
                pdf = pdf.rename(columns={col: "year"})
                time_col_found = True
            break
    
    output_path = base + "." + fmt
    
    if os.path.exists(output_path) and not overwrite:
        print(f"Skipping {output_path}: File already exists. Use --overwrite.")
        del pdf
        gc.collect()
        return

    try:
        if fmt == "dta":
            try: pyreadstat.write_dta(pdf, output_path, version=stata_version)
            except: pyreadstat.write_dta(pdf, output_path)
        elif fmt == "sav":
            pyreadstat.write_sav(pdf, output_path)
        elif fmt == "rdata":
            with localconverter(ro.default_converter + pandas2ri.converter):
                ro.globalenv["df"] = pdf
            ro.r(f"save(df, file='{output_path.replace(chr(39), chr(92)+chr(39))}')")
    except Exception as e:
        print(f"Error writing {output_path}: {e}")
    finally:
        del pdf
        gc.collect()

def main():
    p = argparse.ArgumentParser(description="WB Data Converter (Safe, Efficient & Robust)")
    p.add_argument("files", nargs="*", help="Input files (CSV/Excel)")
    p.add_argument("--sav", action="store_true", help="Output SPSS (.sav)")
    p.add_argument("--rdata", action="store_true", help="Output R (.RData)")
    p.add_argument("--all", action="store_true", help="Output all formats")
    p.add_argument("--out", nargs="*", help="Output filenames (must match input count)")
    p.add_argument("--id", default="Country", help="ID variable name")
    p.add_argument("--time", default="Year", help="Time variable name")
    p.add_argument("--stata", type=int, default=15, help="STATA .dta version (11-15)")
    p.add_argument("--threshold", type=int, default=DEFAULT_LAZY_THRESHOLD, help="Size (MB) to switch to lazy mode")
    p.add_argument("--parquet", type=int, default=DEFAULT_PARQUET_THRESHOLD, help="Size (MB) to use Parquet Intermediate")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    p.add_argument("--license", action="store_true", help="Display licence and exit")
    
    args = p.parse_args()

    if args.license:
        print(LICENSE_TEXT)
        sys.exit(0)

    if not args.files:
        p.print_help()
        sys.exit("Error: No input files provided. Use --license to view license or provide files.")

    if args.out and len(args.out) != len(args.files):
        sys.exit("Error: Number of output files (--out) must match number of input files.")

    formats = ["dta"]
    if args.all: formats = ["dta", "sav", "rdata"]
    else:
        if args.sav: formats.append("sav")
        if args.rdata: formats.append("rdata")

    for i, f in enumerate(args.files):
        try:
            df = process_file(f, args.id, args.time, args.threshold, args.parquet)
            if df is None: continue
            
            base = args.out[i] if args.out else os.path.splitext(f)[0]
            for fmt in formats:
                write(df, base, fmt, args.stata, overwrite=args.overwrite)
            
            print(f"Done: {f}")
            del df
            gc.collect()
        except Exception as e:
            print(f"Error processing {f}: {e}")
            continue

if __name__ == "__main__":
    main()
