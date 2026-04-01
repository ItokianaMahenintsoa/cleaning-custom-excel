from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from io import BytesIO
import pandas as pd
import numpy as np

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

        df = df.replace([np.nan, np.inf, -np.inf], None)
        data = df.to_dict(orient='records')
        json_compatible_data = jsonable_encoder(data)
        return  JSONResponse(
            content={
                "filename" : file.filename,
                "rows": len(df),
                "columns": len(df.columns),
                "data": json_compatible_data

            }
        )
    except Exception as e :
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")