# import pandas as pd
# import utils
# import logging

# CAPITAL_ONE_MAPPING = {
#     "Date": "Transaction Date",
#     "Description": "Merchant",
#     "Category": "Category",
#     "Amount": "Amount",
# }

# CAPITAL_ONE_CATEGORY_MAPPING = {
#     "Dining": "Dining",
#     "Grocery": "Groceries",
#     "Entertainment": "Entertainment",
#     "Travel": "Travel",
#     "Other Travel": "Travel",
#     "Merchandise": "Merchandise",
#     "Healthcare": "Healthcare",
#     "Other": "Others",
# }

# def process_capital_one_transactions(df: pd.DataFrame) -> pd.DataFrame:
#     """Processes Capital One transactions by normalizing, filtering, and formatting the data."""
#     logging.info("Starting Capital One transaction processing...")

#     df = (
#         df.pipe(utils.normalize_column_names, column_mapping=CAPITAL_ONE_MAPPING)
#         .pipe(utils.normalize_categories, category_mapping=CAPITAL_ONE_CATEGORY_MAPPING)
#         .pipe(utils.filter_transaction_date)
#         .pipe(utils.format_merchant)
#         .pipe(utils.format_amount)
#         .pipe(utils.filter_columns, column_mapping=CAPITAL_ONE_MAPPING)
#     )

#     logging.info("Capital One transaction processing completed successfully.")
#     return df  # Return the processed DataFrame


# # if __name__ == "__main__":
# #     df = pd.read_csv(
# #         "/Users/garrettlam/Downloads/Capital-One-Spending-Insights-Transactions.csv",
# #         keep_default_na=False,
# #         dtype=str,
# #     )

# #     df = (
# #         df.pipe(utils.normalize_column_names, column_mapping=CAPITAL_ONE_MAPPING)
# #         .pipe(utils.normalize_categories, category_mapping=CAPITAL_ONE_CATEGORY_MAPPING)
# #         .pipe(utils.filter_transaction_date)
# #         .pipe(utils.format_merchant)
# #         .pipe(utils.format_amount)
# #         .pipe(utils.filter_columns, column_mapping=CAPITAL_ONE_MAPPING)
# #     )
# #     print(df)
# #     print(df.dtypes)
