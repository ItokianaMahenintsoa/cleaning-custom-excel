from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from routes.cleaner import cleaner_router

app = FastAPI()

origins = ["*"]
app.add_middleware( 
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(cleaner_router, prefix="/cleaner")

if __name__ == "__main__" :
    uvicorn.run("main:app", host="0.0.0.0", port=3001, reload=True)