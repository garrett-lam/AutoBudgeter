import pandas as pd
from datetime import datetime, timedelta
import re
import logging
from typing import Dict

logger = logging.getLogger(__name__)  

def get_previous_month():
    """Returns the previous month as a string in YYYY-MM format."""
    today = datetime.today()
    last_day_last_month = today.replace(day=1) - timedelta(days=1)
    return last_day_last_month.strftime("%Y-%m")

def get_last_days_of_previous_two_months():
    """Returns the last day of the month before previous month and the last day of the previous month in MM/DD/YYYY format."""
    today = datetime.today()
    last_day_prev_month = today.replace(day=1) - timedelta(days=1)
    last_day_month_before_prev = last_day_prev_month.replace(day=1) - timedelta(days=1)
    return pd.to_datetime(last_day_month_before_prev.strftime("%m/%d/%Y")), pd.to_datetime(last_day_prev_month.strftime("%m/%d/%Y")) 

def get_previous_month_date_range():
    """Returns the first and last date of the previous month in MM/DD/YYYY format."""
    today = datetime.today()
    last_day_last_month = today.replace(day=1) - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return pd.to_datetime(first_day_last_month.strftime("%m/%d/%Y")), pd.to_datetime(last_day_last_month.strftime("%m/%d/%Y"))

############################################################################################################
## Transformations 
############################################################################################################
def normalize_column_names(df: pd.DataFrame, column_mapping: Dict[str, str]):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    return df.rename(columns=column_mapping)

def normalize_categories(df: pd.DataFrame, category_mapping: Dict[str, str]):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    
    df['Category'] = df['Category'].map(category_mapping).fillna(df['Category'])
    return df

def filter_transaction_date(df: pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    df = df.copy()
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], format="%m/%d/%Y")

    start_date, end_date = get_previous_month_date_range()
    return df[(df["Transaction Date"] >= start_date) & (df["Transaction Date"] <= end_date)].astype(str)

def clean_merchant(text: str):
    """Cleans merchant names by removing common POS prefixes, numbers, and extra spaces."""
    if not isinstance(text, str) or text.strip() == "":
        return "UNKNOWN"  # Default empty descriptions to 'Others'
    
    # Remove leading/trailing spaces
    text = text.strip().lower()  
    # Remove unwanted payment processor prefixes
    text = re.sub(r'^(tst\s*\*|sq\s*\*|\*|bps\s*\*)\s*', '', text, flags=re.IGNORECASE)
    # Remove trailing numbers and special characters ONLY at the end
    text = re.sub(r'[\s#\d]+$', '', text)
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    return text

def format_merchant(df: pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    df['Merchant'] = (
        df['Merchant'].astype(str)
        .apply(clean_merchant)
        .str.title()
    )
    return df

def format_amount(df: pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="raise").abs().astype(str)
    return df

def filter_columns(df: pd.DataFrame, column_mapping: Dict[str, str]):
    """Keeps only columns that are in the provided column mapping values."""
    return df[list(column_mapping.values())]