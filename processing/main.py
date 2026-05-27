def clean_dividend(raw_json : dict):
    # Create a try and except block to handle cases where the incoming data
    # is an invalid type to be casted to a float. Instead of crashing the 
    # whole container, the except block will hanlde any "bad" values.
    try:
        raw_value = raw_json.get("cash amount", 0)
        cash_amount = float(raw_value)
    except (ValueError, TypeError):
        print(f"[Skip] Invalid cash_amount: {raw_json.get("cash_amount")}")
        return None

    # Make sure we aren't getting cash_amounts that don't make sense logically
    if cash_amount <= 0:
        return None
    
    # Standardize the stock ticker to make sure we aren't treating the same stock differently
    ticker = str(raw_json.get("ticker", "")).strip().upper()
    
    currency = str(raw_json.get("currency", "USD")).strip().upper()

    # Schema table
    return {
        "ticker" : ticker,
        "cash amount" : cash_amount,
        "currency" : currency,
        "ex_dividend_date" : raw_json.get("ex_dividend_date")
    }
