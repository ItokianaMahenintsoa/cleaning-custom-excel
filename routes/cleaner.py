from fastapi import APIRouter, HTTPException, File, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from io import BytesIO
import pandas as pd
import numpy as np
import json
from services.cleaner_service import apply_cleaning_values 
cleaner_router = APIRouter(
    tags=["Cleaner"]
)

@cleaner_router.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")
    try : 
        contents = await file.read()
        excel_file = BytesIO(contents)

        if file.filename.lower().endswith('.csv'):
            df = pd.read_csv(excel_file)
        else :
            df = pd.read_excel(excel_file)

        df_preview = df.head(10).replace([np.nan, np.inf, -np.inf], None)
        for col in df_preview.select_dtypes(include=['bool', 'boolean']).columns:
            df_preview[col] = df_preview[col].where(df_preview[col].notna(), None).astype('object')


        preview = df_preview.to_dict(orient='records')
        columns_info = {}
        for col in df.columns:
            col_series = df[col]
            columns_info[col] = {
                "dtype": str(col_series.dtype),
                "has_nan": bool(col_series.isna().any()),      
                "unique_values": int(col_series.nunique())
            }
        columns_info = jsonable_encoder(columns_info)
        preview = jsonable_encoder(preview)

        return  JSONResponse(
            content={
                "filename" : file.filename,
                "rows": len(df),
                "columns": list(df.columns),
                "columns_info": columns_info,
                "data": preview

            }
        )
    except Exception as e :
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")

@cleaner_router.post("/clean-excel")
async def clean_excel(
    file: UploadFile = File(...),
    cleaning_rules: str = Form(...)
):
    if not file.filename.lower().endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(status_code=400, detail="Fichier Excel ou CSV requis.")

    try:
        contents = await file.read()
        data_file = BytesIO(contents)

        if file.filename.lower().endswith('.csv'):
            df = pd.read_csv(data_file)
        else:
            df = pd.read_excel(data_file)

        
        if isinstance(cleaning_rules, str):
            cleaned = cleaning_rules.strip()
            cleaned = ''.join(cleaned.split())
            
           
            if cleaned.endswith("}}") and cleaned.count("}") > cleaned.count("{"):
                cleaned = cleaned[:-1]   # retire la dernière }

 

            try:
                rules = json.loads(cleaned)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail="cleaning_rules n'est pas un JSON valide.")
        else:
            rules = cleaning_rules

        df_cleaned = apply_cleaning_values(df, rules)

        preview = df_cleaned.head(10).replace([np.nan, np.inf, -np.inf], None).to_dict(orient='records')
        preview = jsonable_encoder(preview)

        return JSONResponse(content={
            "filename": file.filename,
            "original_columns": len(df.columns),
            "cleaned_columns": len(df_cleaned.columns),
            "cleaned_data": preview,
            "message": "Nettoyage terminé avec succès"
        })

    except Exception as e:
        print("[DEBUG] Global error:", str(e))
        raise HTTPException(status_code=500, detail=f"Error cleaning file: {str(e)}")

