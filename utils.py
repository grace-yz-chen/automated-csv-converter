import pandas as pd
import re
import warnings
from dateutil.parser import parse
from datetime import datetime
from pandas import Series
from shapely import wkt
from shapely import wkb
import binascii

# NA values list
CUSTOM_NA_VALUES = {"", "na", "n/a", "null", "none", "nan", "-", "--", "#na", "#n/a", "#null"}
WEEKDAY_VALUES = (r"(Sunday|Sun|Monday|Mon|Tuesday|Tue|Wednesday|Wed|Thursday|Thu|Friday|Fri|Saturday|Sat)")
MONTH_VALUES = (r"(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|"
    r"Jul|August|Aug|September|Sep|October|Oct|November|Nov|December|Dec)")     

# Warnings, to ensure the warning not to be duplicated for the same value. 
warnings.filterwarnings("ignore", category=UserWarning)
warnings = []
already_warned = set()

# Format target PostgreSQL table name
def sanitize_pg_table_name(filename: str) -> str | None:
    """Sanitize PostgreSQL table names"""
    table_name = filename.lower().replace(".csv", "")
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
    # Remove leading invalid characters
    table_name = re.sub(r"^[^a-zA-Z_]+", "", table_name)
    return table_name[:63] if table_name else "default_table"

# Format target PostgreSQL table name
def sanitize_pg_column_name(column_name: str) -> str | None:
    """Sanitize PostgreSQL column names"""
    column_name = column_name.strip().lower()
    column_name = re.sub(r"[^a-zA-Z0-9_]", "_", column_name)
    column_name = re.sub(r"^[^a-zA-Z_]+", "", column_name)
    return column_name[:63] if column_name else "default_column"

# Recognize NA values
def is_missing(val) -> bool:    
    if pd.isna(val):
        return True
    val_str = str(val).strip().lower()
    return val_str in CUSTOM_NA_VALUES or val_str == "" 

# Check if GEOMETRY is in WKB format
def is_geometry_wkb(val: str) -> bool:
    # val = val.strip()
    # return bool(re.fullmatch(r"^[0-9A-Fa-f]{16,}$", val)) and val.startswith("01")
    if not isinstance(val, str):
        return False
    try:
        wkb.loads(binascii.unhexlify(val))
        return True
    except Exception:
        return False

# Check if GEOMETRY is in WKT format
def is_geometry_wkt(key: tuple, val: str) -> bool:
    if not isinstance(val, str):
        return False
    try:
        wkt.loads(val)
        return True
    except Exception:
        if re.match(r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\b", val, re.IGNORECASE):
            if key and key not in already_warned:
                print(
                    f"\033[93m[WARNING] Invalid GEOMETRY format at row {key[0]}, column '{key[1]}', "
                    f"value: '{val}'.\033[0m"
                )
                already_warned.add(key)
        return False

def is_geometry(key: tuple, val: str) -> bool:
    return is_geometry_wkt(key, val) or is_geometry_wkb(val)

# Check if the column is GEOMETRY type
def is_geometry_column(column: pd.Series, has_header: bool = True) -> bool:
    non_null = column.dropna().astype(str)
    if non_null.empty:
        return False    
    for idx, val in non_null.items():
        row_number = idx + (2 if has_header else 1)
        key = (row_number, column.name)
        if not is_geometry(key, val):
            return False
    return True

# Remove invalid characters or symbols from the value
def clean_cell(val):
    if pd.isna(val):
        return val
    return (
        str(val)
        .strip(" \t\n\r")
        # Non-breaking space
        .replace('\u00A0', '')
        # Zero-width space
        .replace('\u200B', '')
        # BOM
        .replace('\uFEFF', '')
        # Full-width space
        .replace('\u3000', '')
        # Line separator
        .replace('\u2028', '')
        # Paragraph separator
        .replace('\u2029', '')
    )

# Check if the column is VARCHAR or TEXT type
def is_string_type(column: Series) -> str | None:
    cleaned_col = column.dropna().astype(str).map(clean_cell).map(str.strip)
    cleaned_col = cleaned_col[~cleaned_col.isin(CUSTOM_NA_VALUES.union({""}))]        
    if cleaned_col.empty:
        return "TEXT"        
    max_length = cleaned_col.map(len).max()
    if max_length <= 50:
        return "VARCHAR(50)" 
    elif max_length <= 100:
        return "VARCHAR(100)" 
    elif max_length <= 250:
        return "VARCHAR(250)" 
    else:
        return "TEXT"

# Verify if the value is DATE type by regular expression
def check_date_pattern(key: tuple, val: str) -> bool:    
    row_number, col_number = key            
    date_patterns_digit = [
        # dd [-/.] mm [-/.] yy
        r"^(?P<day>\d{1,2})\s*([-/.]|\s)\s*(?P<month>\d{1,2})\s*([-/.]|\s)\s*(?P<year>\d{2})$",
        # yyyy [-/.] mm [-/.] dd
        r"^(?P<year>\d{4})\s*([-/.]|\s)\s*(?P<month>\d{1,2})\s*([-/.]|\s)\s*(?P<day>\d{1,2})$",  
        # "dd [-/.] mm [-/.] yyyy"
        r"^(?P<day>\d{1,2})\s*([-/.]|\s)\s*(?P<month>\d{1,2})\s*([-/.]|\s)\s*(?P<year>\d{4})$",
    ]        
    for pattern in date_patterns_digit:
        match = re.compile(pattern).fullmatch(val.strip())                  
        if match:
            # If the DATE format is 2-digit year, it should satisfy the format of day-month-year.
            parts = match.groupdict()
            day = int(parts["day"])
            month = int(parts["month"])             
            if not (month >= 1 and month <= 12) or not (day >= 1 and day <= 31):                     
                if key not in already_warned:
                    print(
                        f"\033[93m[WARNING] Ambiguous date format at row {row_number}, column '{col_number}', "
                        f"value: '{val}'. Day should not be in the middle.\033[0m"
                    )
                    already_warned.add(key)
                    return False
            return True
    # Check if the value match the rest of correct DATE format.
    # textual weekday and textual month      
    date_patterns_text = [
        (r"^" + WEEKDAY_VALUES + r"\s*,?\s*" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s*(st|nd|rd|th)\s*,?\s*(\d{2}|\d{4})$"),
        (r"^" + WEEKDAY_VALUES + r"\s*,?\s*" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s+(\d{2}|\d{4})$"),
        (r"^" + WEEKDAY_VALUES + r"\s*,?\s*" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s*,?\s*(\d{2}|\d{4})$"),
        (r"^" + WEEKDAY_VALUES + r"\s*([-./]|\s)\s*" + MONTH_VALUES + r"\s*([-./]|\s)\s*(\d{1,2})\s*(st|nd|rd|th)?\s*([-./]|\s)\s*(\d{2}|\d{4})$"),
        (r"^" + WEEKDAY_VALUES + r"\s*([-.,/]|\s)?\s*(\d{1,2})\s*(st|nd|rd|th)?\s*([-.,/]|\s)?\s*" + MONTH_VALUES + r"\s*([-.,/]|\s)?\s*(\d{2}|\d{4})$"),
        (r"^" + WEEKDAY_VALUES + r"?\s*([-.,/]|\s)?\s*(\d{1,2})\s*(st|nd|rd|th)?\s*of\s*" + MONTH_VALUES + r"\s*([-.,/]|\s)?\s*(\d{2}|\d{4})\s*$"),
        (r"^" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s*(st|nd|rd|th)\s*,?\s*(\d{2}|\d{4})$"),
        (r"^" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s+(\d{2}|\d{4})$"),
        (r"^" + MONTH_VALUES + r"\s*,?\s*(\d{1,2})\s*,?\s*(\d{2}|\d{4})$"),
        (r"^" + MONTH_VALUES + r"\s*([-./]|\s)\s*(\d{1,2})\s*(st|nd|rd|th)?\s*([-./]|\s)\s*(\d{2}|\d{4})$"),
        (r"^(\d{1,2})\s*(st|nd|rd|th)?\s*([-.,/]|\s)?\s*" + MONTH_VALUES + r"\s*([-.,/]|\s)?\s*(\d{2}|\d{4})$"),
        (r"^(\d{4})\s*([-.,/]|\s)?\s*" + MONTH_VALUES + r"\s*([-.,/]|\s)?\s*(\d{1,2})\s*(st|nd|rd|th)?$"),
        (r"^(\d{4})\s*([-.,/]|\s)?\s*(\d{1,2})\s*(st|nd|rd|th)?\s*of\s*" + MONTH_VALUES + r"$")
    ]
    for pattern in date_patterns_text:
        match = re.compile(pattern, re.IGNORECASE).fullmatch(val.strip())
        if match:
            return True
    return False

# Verify if the value is TIMESTAMP or TIMESTAMPTZ type by regular expression
# check_tz: True means checking if the value is TIMESTAMPTZ
#           False means checking if the value is TIMESTAMP
def check_timestamp_pattern(key: tuple, val: str, check_tz: bool) -> bool:
    row_number, col_number = key
    # hh:mm:ss[.sss] or hh:mm or hh
    hh_mm_ss = r"\s*(\d{1,2})(:(\d{1,2}))?(:(\d{1,2})(\.\d+)?)?"
    tz = r"(?:Z|[+-]\d{2}(?::?\d{2}))"
    # hh_mm_ss = r"\d{1,2}:\d{1,2}:\d{1,2}"
    date_patterns_text = [
        r"^\d{1,2}\s*(st|nd|rd|th)?\s*" + MONTH_VALUES + r"\s*(\d{2}|\d{4})\s*,\s*\d{1,2}\s*:\s*\d{1,2}\s+UTC$",
    ]
    for pattern in date_patterns_text:            
        match = re.compile(pattern, re.IGNORECASE).fullmatch(val.strip())        
        if match:
            return True
    date_patterns_digit = [
        # yyyy [-/.] mm [-/.] dd hh:mm:ss[.sss]
        r"^(?P<year>\d{4})(?:[-/.]|\s)(?P<month>\d{1,2})(?:[-/.]|\s)(?P<day>\d{1,2})(\s*,\s*|\s+|\s*T\s*)" 
            + hh_mm_ss + (tz if check_tz else r"") + r"(\s*(AM|PM))?$",
        # dd [-/.] mm [-/.] yyyy hh:mm:ss[.sss]
        r"^(?P<day>\d{1,2})(?:[-/.]|\s)(?P<month>\d{1,2})(?:[-/.]|\s)(?:\d{4})(\s*,\s*|\s+|\s*T\s*)" 
            + hh_mm_ss + (tz if check_tz else r"") + r"(\s*(AM|PM))?$",
        # yy [-/.] mm [-/.] dd hh:mm:ss[.sss]
        r"^(?P<year>\d{2})(?:[-/.]|\s)(?P<month>\d{1,2})(?:[-/.]|\s)(?P<day>\d{1,2})(\s*,\s*|\s+|\s*T\s*)" 
            + hh_mm_ss + (tz if check_tz else r"") + r"(\s*(AM|PM))?$",
        # dd [-/.] mm [-/.] yy hh:mm:ss[.sss]
        r"^(?P<day>\d{1,2})(?:[-/.]|\s)(?P<month>\d{1,2})(?:[-/.]|\s)(?P<year>\d{2})(\s*,\s*|\s+|\s*T\s*)" 
            + hh_mm_ss + (tz if check_tz else r"") + r"(\s*(AM|PM))?$",
    ]
    for pattern in date_patterns_digit:            
        match = re.compile(pattern, re.IGNORECASE).fullmatch(val.strip())        
        if match:
            parts = match.groupdict()
            day = int(parts["day"])
            month = int(parts["month"])
            if not (month >= 1 and month <= 12) or not (day >= 1 and day <= 31):
                if key not in already_warned:
                    print(
                        f"\033[93m[WARNING] Ambiguous date format at row {row_number}, column '{col_number}', "
                        f"value: '{val}'. Day should not be in the middle.\033[0m"
                    )
                    already_warned.add(key)
                    return False
            return True    
    return False

# Verify if the value is TIME or TIMETZ type by regular expression
# check_tz: True means checking if the value is TIMETZ
#           False means checking if the value is TIME
def check_time_pattern(key: tuple, val: str, check_tz: bool) -> bool:
    if not ":" in val:  
        return False
    row_number, col_number = key
    # hh:mm:ss[.sss] or hh:mm or hh
    hh_mm_ss = r"(\d{1,2})(:(\d{1,2}))?(:(\d{1,2})(\.\d*)?)?"
    tz = r"(?:Z|[+-]\d{2}(?::?\d{2}))"
    # hh_mm_ss = r"\d{1,2}:\d{1,2}:\d{1,2}"
    date_patterns = [
        # hh:mm:ss[.sss]
        hh_mm_ss + (tz if check_tz else r"") + r"(\s*(AM|PM))?$",
    ]
    for pattern in date_patterns:
        match = re.compile(pattern, re.IGNORECASE).fullmatch(val.strip())
        if match:
            return True    
    return False

# Check if the column is DATE or TAMESTAMP or TIME type
def is_date_time_column(column: Series, has_header: bool = True) -> str | None:
    total_non_null = column.dropna().shape[0]
    if total_non_null == 0:
        return None
    # Convert each value to string type
    column_str = column.dropna().astype(str) 
    if column_str.empty:
        return None
    found_invalid = False
    contains_timetz = False
    contains_time = False
    contains_timestamptz = False
    contains_timestamp = False
    contains_date = False
    all_period_format = False
    # Iterate each row
    for idx, val in column_str.items():
        # Get the row number
        row_number = idx + (2 if has_header else 1)
        key = (row_number, column.name) 
        # If there is already warning for this value, stop checking           
        if key in already_warned:
            found_invalid = True
            continue               
        val = val.strip()    
        # Check if all the value is time period format, for example 00:00-01:00
        period_pattern = r"^\d{1,2}:\d{1,2}\s*-\s*\d{1,2}:\d{1,2}$"
        match = re.compile(period_pattern, re.IGNORECASE).fullmatch(val.strip())
        if match:
            return None
        try:
            # Try to convert the value to DATE or DATETIME or TIME type
            parse(val, fuzzy=False)
            # print("PASS")
            # Check further by regular expression
            if check_timestamp_pattern(key, val, True) == True:
                contains_timestamptz = True
            elif check_timestamp_pattern(key, val, False) == True: 
                contains_timestamp = True 
            elif check_date_pattern(key, val) == True:
                contains_date = True
            elif check_time_pattern(key, val, True) == True:
                contains_timetz = True
            elif check_time_pattern(key, val, False) == True:
                contains_time = True
            else:
                found_invalid = True 
        except:
            # print("INVALID" + val)
            found_invalid = True
    if found_invalid:
        return None
    # If the column contains mix of time zone and non-time zone format, return NULL
    # Because it's not possible to define the time zone of TIME and TIMESTAMP.          
    if sum([contains_timetz, contains_time, contains_timestamptz, contains_timestamp]) >= 2:            
        return None 
    # If the column contains DATE and time zone format, return NULL
    # Because it's not possible to define the time zone of DATE.
    # If the column contains both DATE and TIME, return NULL too.
    if contains_date and sum([contains_timetz, contains_timestamptz, contains_time]) >= 1:
        return None 
    # If the column only contains TIMETZ, return TIMETZ type
    if contains_timetz and not contains_timestamp and not contains_date \
        and not contains_time and not contains_timestamptz:
        return "TIMETZ"
    # If the column only contains TIME, return TIME type
    if contains_time and not contains_timestamp and not contains_date \
        and not contains_date and not contains_timestamptz: 
        return "TIME"
    # If the column only contains DATE, return DATE type
    if contains_date and not contains_timestamp and not contains_timetz \
        and not contains_time and not contains_timestamptz: 
        return "DATE"    
    # If the column only contains DATE and TIMESTAMP, return TIMESTAMP type    
    if (contains_timestamp or contains_timestamp and contains_date) and not contains_timetz \
        and not contains_time and not contains_timestamptz: 
        return "TIMESTAMP"
    # If the column only contains DATE and TIMESTAMP, return TIMESTAMP type    
    if (contains_timestamptz or contains_timestamptz and contains_date) and not contains_timetz \
        and not contains_time and not contains_timestamp: 
        return "TIMESTAMPTZ"       
    return None
 
# Check if the column is BOOLEAN type
def is_boolean_column(column: Series) -> bool:
    if column.isna().all():
        return False
    column_str = column.dropna().astype(str).str.strip().str.lower()
    valid_boolean_values = {"true", "false", "yes", "no", "y", "n", "t", "f", "1", "0"}
    return column_str.isin(valid_boolean_values).all()

# Check if the column is INTEGER or NUMERIC type
def is_numeric_column(column: pd.Series) -> str | None:
    parsed_values = []
    is_date_column = True
    is_credit_card_column = True
    for val in column:
        # Ignore NA values 
        if pd.isna(val) or str(val).strip().lower() in CUSTOM_NA_VALUES:
            continue          
        val_str = str(val).strip()  
        # If the value starts with 0, the type should be VARCHAR or TEXT
        if val_str.isdigit() and val_str.startswith('0') and len(val_str) > 1:
            return None
        thousand_sep_pattern = re.compile(r"^\d{1,3}(?:,\d{3})+(?:\.\d+)?$")
        if thousand_sep_pattern.match(val_str):
            val_str = val_str.replace(',', '')               
        try:
            if '.' in val_str:
                parsed_values.append(float(val_str))
            else:
                parsed_values.append(int(val_str))
        except ValueError:
            return None
        # If all the value is yyyymmdd format, it's VARCHAR.
        if is_date_column:
            if len(val_str) != 8 or val_str.startswith("-"):
                is_date_column = False
            else:
                try:
                    parse(val_str, fuzzy=False)
                except ValueError:
                    is_date_column = False
        # Check if all the values in the column is credit card number length.
        if is_credit_card_column and (not 16 <= len(val_str) <= 19 or val_str.startswith("-")):
            is_credit_card_column = False   
    # If all value in the column is DATE format or credit card length, return VARCHAR. 
    if is_date_column or is_credit_card_column:
        return "VARCHAR(50)"    
    # If all the values cannot be parsed to INTEGER or NUMERIC, return None
    if not parsed_values:
        return None
    # Calculate the max INTEGER or DECIMAL value
    max_val = max([v for v in parsed_values if isinstance(v, (int, float))], default=None)    
    if all(isinstance(v, int) or pd.isna(v) for v in parsed_values):
        if abs(max_val) <= 32767:
            return "SMALLINT"
        elif abs(max_val) <= 2147483647:
            return "INTEGER"
        elif abs(max_val) <= 9223372036854775807:
            return "BIGINT"
        else:
            return "NUMERIC"
    return "NUMERIC"

# Guess column type
def guess_column_type(column: Series, has_header: bool = True) -> str | None:
    if is_geometry_column(column):
        return "GEOMETRY"
    numeric_type = is_numeric_column(column)
    if numeric_type != None:
        return numeric_type
    if pd.api.types.is_bool_dtype(column) or is_boolean_column(column):
        return "BOOLEAN"
    if pd.api.types.is_object_dtype(column) or pd.api.types.is_string_dtype(column):            
        datetime_type = is_date_time_column(column, has_header=has_header)
        if datetime_type != None:
            return datetime_type      
        else:
            return is_string_type(column)
    return "TEXT"