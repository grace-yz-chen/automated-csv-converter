import pandas as pd
import numpy as np
import re
import os
import sys
import argparse
import csv
from dateutil.parser import parse
from datetime import datetime
from pandas import Series
from pandas.errors import ParserError
from utils import sanitize_pg_table_name, sanitize_pg_column_name, \
    is_missing, clean_cell, guess_column_type, CUSTOM_NA_VALUES

# python converter.py "c:/example/combination.csv" [--no-header]
# python converter.py combination.csv [--no-header]
# "--no-header" means if the file has no header row, the program will generate default column names
# If the file has header row, the parameter "--no-header" is not needed
parser = argparse.ArgumentParser(description="CSV to PostgreSQL converter")
parser.add_argument("csv_file", type=str, help="Path to the CSV file")
parser.add_argument('--no-header', action='store_true', help='Set this flag if CSV file does NOT have a header row')
args = parser.parse_args()

# File name can be full path or file name
# If inputting file name, the program will look for the file in the current path
if os.path.isabs(args.csv_file):
    csv_path = args.csv_file
else:
    csv_path = os.path.join(os.getcwd(), args.csv_file)

# Check if the file is CSV
if not csv_path.lower().endswith(".csv"):
    print("\033[91m[ERROR] Input file must be a CSV file.\033[0m")
    sys.exit(1)

# Use the file name as output SQL file name, output SQL file will be in the same path
source_table = os.path.splitext(os.path.basename(csv_path))[0]
table_name = sanitize_pg_table_name(source_table)
# sql_output_path = os.path.join(os.getcwd(), f"{table_name}.sql")
sql_output_path = os.path.join(os.path.dirname(csv_path), f"{table_name}.sql")

# Record the start of conversion
start_time = datetime.now()
print(f"\033[92m[START] Conversion started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\033[0m")

# Check if CSV file column mismatch
with open(csv_path, newline='', encoding='utf-8') as f:
    column_mismatch = False
    reader = csv.reader(f)
    expected_cols = len(next(reader))
    for i, row in enumerate(reader, start=2):
        if len(row) != expected_cols:
            print(f"\033[91m[ERROR] Line {i}: Expected {expected_cols} columns but found {len(row)} columns\033[0m")
            column_mismatch = True
    if column_mismatch:
        print(f"\033[91m[ERROR] Column count mismatch detected. Aborting.\033[0m")
        sys.exit(1)

# Generate SQL file
with open(sql_output_path, "w", encoding="utf-8") as tf:

    csv_path = args.csv_file 
    try:
        if args.no_header:
            df = pd.read_csv(csv_path, header=None,
                na_values=list(CUSTOM_NA_VALUES), keep_default_na=True, dtype=str,
                sep=",", quotechar='"', encoding="utf-8", skipinitialspace=True)
            df.columns = [f"Column{i+1}" for i in range(df.shape[1])]
        else:
            df = pd.read_csv(csv_path,
                na_values=list(CUSTOM_NA_VALUES), keep_default_na=True, dtype=str,
                sep=",", quotechar='"', encoding="utf-8", skipinitialspace=True)
    except ParserError as e:
        print("\033[91m[ERROR] Failed to parse the CSV file.\033[0m")
        print("Please check the file for the following common issues:")
        print("- Mismatched or missing commas, quotes, or delimiters")
        print("- If a value contains a comma, enclose it in double quotes (e.g., \"value, with, commas\")")
        exit(1)
    except Exception as e:
        print(f"\033[91m[ERROR] Unexpected error while reading the file: {e}\033[0m")
        exit(1)
    
    for col in df.columns:
        df[col] = df[col].map(clean_cell).map(lambda x: None if is_missing(x) else x)
    
    # Clean table name
    table_name: str = sanitize_pg_table_name(source_table)
    sql_create_table: str = (
        f'DROP TABLE IF EXISTS {table_name};\n'
        f'CREATE TABLE {table_name} (\n'
    )

    # Generate DDL
    column_names = []
    column_types = []

    # Format column names
    df.columns = [sanitize_pg_column_name(col) for col in df.columns]

    for column_name in df.columns:
        column_type: str = guess_column_type(df[column_name], not args.no_header)
        sql_create_table += f'    {column_name} {column_type},\n'
        column_names.append(f'{column_name}')
        column_types.append(column_type)

    sql_create_table = sql_create_table.rstrip(",\n") + "\n);\n"

    # Generate DDL statement
    tf.write(sql_create_table)
    
    # Generate DML insert statement
    sql_insert_data = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES \n"

    # Use list to store values and join them later for efficiency
    value_list = []
    
    # Convert data values or format according to target column data type
    # Iterator every row in the CSV
    for row in df.itertuples(index=False, name=None):
        values = []
        # Iterator every column in the row
        for i in range(len(df.columns)):
            # If the value is in NA values list, replace this value with NULL  
            if is_missing(row[i]):
                values.append("NULL")
                continue
            if column_types[i] == "TIMETZ":
                time_val = pd.to_datetime(row[i], errors='coerce')
                if pd.notna(time_val):
                    if time_val.tzinfo is None:
                        time_val = time_val.tz_localize('UTC')
                    formatted_time = time_val.strftime('%H:%M:%S%z')
                    tz_part = formatted_time[-5:]
                    tz_with_colon = tz_part[:3] + ":" + tz_part[3:]
                    formatted_time = formatted_time[:-5] + tz_with_colon
                    values.append(f"'{formatted_time}'")
                else:
                    values.append('NULL')
            elif column_types[i] == "TIME":
                time_val = pd.to_datetime(row[i], errors='coerce')
                if pd.notna(time_val):
                    if time_val.tzinfo is not None:
                        time_val = time_val.tz_convert('UTC').replace(tzinfo=None)
                    formatted_time = time_val.strftime('%H:%M:%S')
                    values.append(f"'{formatted_time}'")
                else:
                    values.append('NULL')
            elif column_types[i] == "DATE":                
                # By default it's day-first, unless the date starts with "yyyy"
                if re.match(r'^\d{4}', row[i]):
                    date_val = pd.to_datetime(row[i], errors='coerce', dayfirst=False)
                else:
                    date_val = pd.to_datetime(row[i], errors='coerce', dayfirst = True)
                values.append(f"'{date_val.date()}'" if pd.notna(date_val) else 'NULL')
            elif column_types[i] == "TIMESTAMPTZ":
                timestamp_val = pd.to_datetime(row[i], errors='coerce')
                if pd.notna(timestamp_val):
                    if timestamp_val.tzinfo is None:
                        timestamp_val = timestamp_val.tz_localize('UTC')
                    else:
                        timestamp_val = timestamp_val.tz_convert('UTC')
                    # formatted_timestamp = timestamp_val.strftime('%Y-%m-%d %H:%M:%S %z')
                    # formatted_timestamp = timestamp_val.strftime('%Y-%m-%d %H:%M:%S.%f %z')[:29]
                    formatted_timestamp = timestamp_val.isoformat(timespec='microseconds')
                    values.append(f"'{formatted_timestamp}'")
                else:
                    values.append('NULL')
            elif column_types[i] == "TIMESTAMP":    
                # By default it's day-first, unless the date starts with "yyyy"
                if re.match(r'^\d{4}', row[i]):                    
                    timestamp_val = pd.to_datetime(row[i], errors='coerce', dayfirst=False)
                else:
                    timestamp_val = pd.to_datetime(row[i], errors='coerce', dayfirst = True)
                if pd.notna(timestamp_val):
                    # formatted_timestamp = timestamp_val.strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
                    formatted_timestamp = timestamp_val.strftime('%Y-%m-%d %H:%M:%S.%f')
                    values.append(f"'{formatted_timestamp}'")
                else:
                    values.append('NULL')
            elif column_types[i] in ("INTEGER", "BIGINT", "NUMERIC"):
                val = str(row[i]).replace(",", "").strip()
                values.append(f"'{val}'")
            elif column_types[i] == "BOOLEAN":
                val = str(row[i]).strip().lower()
                if val in ("true", "t", "yes", "y", "1"):
                    values.append("TRUE")
                elif val in ("false", "f", "no", "n", "0"):
                    values.append("FALSE")
                else:
                    values.append("NULL")
            else:
                val = str(row[i]).replace("'", "''")
                values.append(f"'{val}'")
        value_list.append(f"({', '.join(values)})")

    sql_insert_data += ",\n".join(value_list) + ";\n"

    # Write DML
    tf.write(sql_insert_data)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"\033[92m[END] Successfully converted {len(df)} rows in {duration:.2f} seconds\033[0m")