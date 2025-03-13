
COLUMN_MAPPING = {
    "CHASE": {
        "Transaction Date": "transaction_date",
        "Description": "merchant",
        "Category": "category",
        "Amount": "amount",
    },
    "CAPITAL_ONE": {
        "Date": "transaction_date",
        "Description": "merchant",
        "Category": "category",
        "Amount": "amount",
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
