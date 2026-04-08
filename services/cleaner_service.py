from typing import Dict, Any
import pandas as pd
import re

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
        df[col] = df[col].astype(str)
        if col_rules.get("strip_whitespace"):
            df[col] = df[col].str.strip()

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

        if col_rules.get("to_date", False):
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if col_rules.get("date_format"):
                format_str = col_rules.get("date_format", "%Y-%m-%d")
                df[col] = df[col].dt.strftime(format_str).where(df[col].notna(), None)

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

        if col_rules.get("type_heure", False):
            temp = pd.to_datetime(df[col], errors="coerce")
            df[col] = temp.apply(lambda x: x if x in [36, 40] else 40)

        if col_rules.get("numeric_only", False):
           df[col] = df[col].astype(str).str.replace(r"\s+", "", regex=True)
           df[col] = df[col].str.replace(r"[^0-9]", "", regex=True)
           df[col] = df[col].replace("", None)

        if col_rules.get("validate_email", False):
            email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

            def is_valid_email(email):
                if pd.isna(email) or str(email).strip() == "" or str(email).lower == "nan":
                    return None
                email = str(email).strip()
                if re.match(email_regex, email):
                    return email.lower()
                else:
                    return None 
                
            df[col] = df[col].apply(is_valid_email)
        
               
        if col_rules.get("format_phone_mg", False):
            print(f"[DEBUG] Formatage numéro téléphone malgache sur colonne : {col}")

            def format_malagasy_phone(phone):
                if pd.isna(phone) or str(phone).strip() == "" or str(phone).lower() == "nan":
                    return None
               
            
                phone_str = str(phone).strip()
                digits = re.sub(r'[^0-9]', '', phone_str)   
                
             
                if len(digits) > 10:
                    digits = digits[-10:]   
                
              
                if digits.startswith('0'):
                    digits = digits[1:]
                
              
                if len(digits) == 9:        
                    return "+261" + digits
                elif len(digits) == 10 and digits.startswith('261'):
                    return "+" + digits     
                else:
                    return None            

            df[col] = df[col].apply(format_malagasy_phone)
        
    return df

