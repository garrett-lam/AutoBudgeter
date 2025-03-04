import pandas as pd 
import utils as utils
import logging

logger = logging.getLogger(__name__)  

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
        "Personal": "Healthcare"
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


# --- Chase -----------------------------------------------------------------------------

# Chase includes payments to card in its transactions, so we need to filter them out
def filter_chase_payments(df: pd.DataFrame):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    df = df[df["Type"] != "Payment"]
    return df

def process_chase_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Processes Chase transactions by normalizing, filtering, and formatting the data."""
    logger.info("Starting Chase transaction processing...")
    df = (
        df.pipe(utils.normalize_column_names, column_mapping=COLUMN_MAPPING["CHASE"])
        .pipe(utils.normalize_categories, category_mapping=CATEGORY_MAPPING["CHASE"])
        .pipe(utils.filter_transaction_date)
        .pipe(filter_chase_payments)
        .pipe(utils.format_merchant)
        .pipe(utils.format_amount)
        .pipe(utils.filter_columns, column_mapping=COLUMN_MAPPING["CHASE"])
    )
    logger.info("Chase transaction processing completed successfully.")
    df['Provider'] = 'Chase'
    return df  # Return the processed DataFrame

# --- Capital One -------------------------------------------------------------------------

def process_capital_one_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Processes Capital One transactions by normalizing, filtering, and formatting the data."""
    logger.info("Starting Capital One transaction processing...")

    df = (
        df.pipe(utils.normalize_column_names, column_mapping=COLUMN_MAPPING["CAPITAL_ONE"])
        .pipe(utils.normalize_categories, category_mapping=CATEGORY_MAPPING["CAPITAL_ONE"])
        .pipe(utils.filter_transaction_date)
        .pipe(utils.format_merchant)
        .pipe(utils.format_amount)
        .pipe(utils.filter_columns, column_mapping=COLUMN_MAPPING["CAPITAL_ONE"])
    )
    df['Provider'] = 'Capital One'
    logger.info("Capital One transaction processing completed successfully.")
    return df  # Return the processed DataFrame

# --- Bilt / Wells Fargo ---------------------------------------------------------------

# def add_header_to_bilt_transactions(df: pd.DataFrame) -> pd.DataFrame:
#     """Adds a header to Bilt transactions."""
#     if not isinstance(df, pd.DataFrame):
#         raise TypeError("Input must be a pd.DataFrame.")
#     df.columns = ["Transaction Date", "Amount", "Category", "Merchant"]
#     return df

def filter_bilt_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Filters out payment transactions from Bilt transactions."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    df = df[df["Category"] != "Payment"]
    return df

def order_bilt_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorders columns for Bilt transactions."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pd.DataFrame.")
    return df[["Transaction Date", "Merchant", "Category", "Amount"]]

# BILT doesn't have categories or a header
def process_bilt_transactions(df: pd.DataFrame) -> pd.DataFrame:

    logger.info("Starting Bilt transaction processing...")
    df = (
        df.pipe(filter_bilt_payments)
        .pipe(utils.filter_transaction_date)
        .pipe(utils.format_merchant)
        .pipe(utils.format_amount)
        .pipe(order_bilt_columns)
    )
    df['Provider'] = 'Bilt'
    logger.info("Bilt transaction processing completed successfully.")
    return df


# if __name__ == "__main__":
#     # For testing the functions in this script
#     df = pd.read_csv("/Users/garrettlam/Downloads/feb_transactions/CreditCard1.csv", keep_default_na=False, dtype=str)
#     df = process_bilt_transactions(df)
#     print(df)

#     df = pd.read_csv("/Users/garrettlam/Downloads/feb_transactions/Chase0823_Activity20250201_20250228_20250304.CSV", keep_default_na=False, dtype=str)
#     df = process_chase_transactions(df)
#     print(df)

#     df = pd.read_csv("/Users/garrettlam/Downloads/feb_transactions/Capital-One-Spending-Insights-Transactions_fake202502.csv", keep_default_na=False, dtype=str)
#     df = process_capital_one_transactions(df)
#     print(df)