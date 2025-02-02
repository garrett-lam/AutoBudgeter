import pandas as pd 
import utils
import logging

COLUMN_MAPPING = {
    "CHASE": {
        "Transaction Date": "Transaction Date",
        "Description": "Merchant",
        "Category": "Category",
        "Amount": "Amount",
    },
    "CAPITAL_ONE": {
        "Date": "Transaction Date",
        "Description": "Merchant",
        "Category": "Category",
        "Amount": "Amount",
    },
}

CATEGORY_MAPPING = {
    "CHASE": {
        "Food & Drink": "Dining",
        "Groceries": "Groceries",
        "Entertainment": "Entertainment",
        "Travel": "Travel",
        "Shopping": "Merchandise",
        "Health & Wellness": "Healthcare",
    },
    "CAPITAL_ONE": {
        "Dining": "Dining",
        "Grocery": "Groceries",
        "Entertainment": "Entertainment",
        "Travel": "Travel",
        "Other Travel": "Travel",
        "Merchandise": "Merchandise",
        "Healthcare": "Healthcare",
        "Other": "Others",
    },
}

# Chase includes payments to card in its transactions, so we need to filter them out
def filter_for_payments(df: pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    return df[df["Type"] == "Sale"]

def process_chase_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Processes Chase transactions by normalizing, filtering, and formatting the data."""
    logging.info("Starting Chase transaction processing...")
    df = (
        df.pipe(utils.normalize_column_names, column_mapping=COLUMN_MAPPING["CHASE"])
        .pipe(utils.normalize_categories, category_mapping=CATEGORY_MAPPING["CHASE"])
        .pipe(utils.filter_transaction_date)
        .pipe(filter_for_payments)
        .pipe(utils.format_merchant)
        .pipe(utils.format_amount)
        .pipe(utils.filter_columns, column_mapping=COLUMN_MAPPING["CHASE"])
    )
    logging.info("Chase transaction processing completed successfully.")
    return df  # Return the processed DataFrame

def process_capital_one_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Processes Capital One transactions by normalizing, filtering, and formatting the data."""
    logging.info("Starting Capital One transaction processing...")

    df = (
        df.pipe(utils.normalize_column_names, column_mapping=COLUMN_MAPPING["CAPITAL_ONE"])
        .pipe(utils.normalize_categories, category_mapping=CATEGORY_MAPPING["CAPITAL_ONE"])
        .pipe(utils.filter_transaction_date)
        .pipe(utils.format_merchant)
        .pipe(utils.format_amount)
        .pipe(utils.filter_columns, column_mapping=COLUMN_MAPPING["CAPITAL_ONE"])
    )

    logging.info("Capital One transaction processing completed successfully.")
    return df  # Return the processed DataFrame


