# import pandas as pd
# import utils
# import logging

# CHASE_MAPPING = {
#     "Transaction Date": "Transaction Date",
#     "Description": "Merchant",
#     "Category": "Category",
#     "Amount": "Amount",
# }

# CHASE_CATEGORY_MAPPING = {
#     "Food & Drink": "Dining",
#     "Groceries": "Groceries",
#     "Entertainment": "Entertainment",
#     "Travel": "Travel",
#     "Shopping": "Merchandise",
#     "Health & Wellness": "Healthcare",
# }

# # Chase includes payments to card in its transactions, so we need to filter them out
# def filter_for_payments(df: pd.DataFrame):
#     if not isinstance(df, pd.DataFrame):
#         raise TypeError("Input must be a pd.DataFrame.")
#     return df[df["Type"] == "Sale"]

# def process_chase_transactions(df: pd.DataFrame) -> pd.DataFrame:
#     """Processes Chase transactions by normalizing, filtering, and formatting the data."""
#     logging.info("Starting Chase transaction processing...")
#     df = (
#         df.pipe(utils.normalize_column_names, column_mapping=CHASE_MAPPING)
#         .pipe(utils.normalize_categories, category_mapping=CHASE_CATEGORY_MAPPING)
#         .pipe(utils.filter_transaction_date)
#         .pipe(filter_for_payments)
#         .pipe(utils.format_merchant)
#         .pipe(utils.format_amount)
#         .pipe(utils.filter_columns, column_mapping=CHASE_MAPPING)
#     )
#     logging.info("Chase transaction processing completed successfully.")
#     return df  # Return the processed DataFrame

# # if __name__ == "__main__":
# #     df = pd.read_csv(
# #         "/Users/garrettlam/Downloads/Chase0823_Activity20250101_20250131_20250201.CSV",
# #         keep_default_na=False,
# #         dtype=str,
# #     )

# #     df = (
# #         df.pipe(utils.normalize_column_names, column_mapping=CHASE_MAPPING)
# #         .pipe(utils.normalize_categories, category_mapping=CHASE_CATEGORY_MAPPING)
# #         .pipe(utils.filter_transaction_date)
# #         .pipe(filter_for_payments)
# #         .pipe(utils.format_merchant)
# #         .pipe(utils.format_amount)
# #         .pipe(utils.filter_columns, column_mapping=CHASE_MAPPING)
# #     )
# #     print(df)
# #     print(df.dtypes)
