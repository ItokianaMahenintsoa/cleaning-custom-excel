from typing import Dict, Any
import pandas as pd

def apply_cleaning_values(df: pd.DataFrame, rules: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    remove_empty_headers = False
    if isinstance(rules, dict):
        header_rule = rules.get("___remove_empty_headers___")
        if isinstance(header_rule, dict):
            remove_empty_headers = header_rule.get("enabled", False)
    if remove_empty_headers:
        #
        df.columns = df.columns.astype(str)
        
        columns_to_drop = []
        for col in df.columns:
            col_str = str(col).strip()
            if (col_str == "" or col_str.lower() in ["nan", "none", "null"] or col_str.startswith("Unnamed:")):
                columns_to_drop.append(col)
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

    return df