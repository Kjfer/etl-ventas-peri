from supabase import create_client
from dotenv import load_dotenv
import os
import pandas as pd
from logger import get_logger

logger = get_logger("LOAD")

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def load(df: pd.DataFrame):
    logger.info(f"Cargando registros en Supabase: {len(df)}")

    data = df.to_dict(orient="records")

    supabase.table("transactions").upsert(
        data,
        on_conflict="date,business_id,amount,reference"
    ).execute()

    logger.info("Carga mensual completada (sin duplicados)")
