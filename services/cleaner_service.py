from typing import Dict, Any
import pandas as pd

def apply_cleaning_values(df: pd.DataFrame, rules: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    if not isinstance(rules, dict):
        return df

    # 1. Suppression des colonnes sans entête
    if rules.get("___remove_empty_headers___", {}).get("enabled", False):
        df.columns = df.columns.astype(str)
        columns_to_drop = [
            col for col in df.columns 
            if str(col).strip() == "" 
            or str(col).strip().lower() in ["nan", "none", "null"] 
            or str(col).strip().startswith("Unnamed:")
        ]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

    # 2. Règles par colonne
    for col, col_rules in rules.items():
        if col == "___remove_empty_headers___" or col not in df.columns:
            continue
        if not isinstance(col_rules, dict):
            continue

        # Nettoyage de base
        df[col] = df[col].astype(str).str.strip()

        if col_rules.get("remove_spaces"):
            df[col] = df[col].str.replace(r"\s+", "", regex=True)

        if col_rules.get("to_upper"):
            df[col] = df[col].str.upper()
        elif col_rules.get("to_lower"):
            df[col] = df[col].str.lower()

        # Mapping classique (Sexe, Situation M, ...)
        if "mapping" in col_rules and isinstance(col_rules["mapping"], dict):
            mapping_dict = col_rules["mapping"]
            df[col] = df[col].map(mapping_dict).fillna(df[col])

        # Standardisation automatique
        if col_rules.get("standardize_auto", False):
            temp = df[col].astype(str).str.strip()
            value_counts = temp.value_counts(dropna=True)
            mapping = {}

            for val in temp.dropna().unique():
                val_str = str(val).strip()
                if val_str.lower() in ["nan", "", "none"]:
                    continue
                    
                val_clean = val_str.upper()
                
                # Trouver les valeurs similaires (même forme en majuscule)
                similar = [v for v in value_counts.index 
                          if str(v).strip().upper() == val_clean]
                print(f"[DEBUG] Value: '{val}' | Similar: {similar}")
                
                if similar:
                    most_common = value_counts.loc[similar].idxmax()
                    mapping[val] = most_common
                else:
                    mapping[val] = val

            # Appliquer le mapping
            df[col] = temp.map(mapping).fillna(temp)

    return df