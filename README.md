# Automated CSV Converter

The Automated CSV-to-PostgreSQL Converter parses CSV files into the corresponding PostgreSQL **DDL** and **DML** statements.  
It accurately detects data types (including mixed-type columns) and handles missing values.  

The tool provides both a **command-line version** and a **GUI desktop version**.  
Source code includes:
- `converter.py`
- `gui.py`
- `utils.py`

These files must be placed in the same folder.

## Environment
- Python: **3.8+** must be installed
- Windows or Ubuntu Operating system

## Python Dependencies

- pip install pandas numpy python-dateutil shapely (Windows)
- pip3 install pandas numpy python-dateutil shapely (Linux)

## How to Run Command-line Version

- python converter.py [input_csv] [--no-header] (Windows)
- python3 converter.py [input_csv] [--no-header] (Linux / macOS)

[input_csv] can be either a relative file name such as "combination.csv", or an absolute path such as "c:/example/combination.csv". 
The parameter "--no-header" is optional, and it indicates that the CSV file has no header row.

## How to Run GUI Version

python converter.py sample.csv --no-header (Windows)
python3 converter.py sample.csv --no-header (Linux / macOS)










