import os
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from extract import extract_sheet
from transform import (
    transform_ventas_peri_collection,
    transform_expenses_sheet
)
from load import load
from logger import get_logger

logger = get_logger("PIPELINE")

load_dotenv()

def run_pipeline(year=None, month=None):
    # =========================
    # DEFINICIÓN DE PERIODO
    # =========================
    today = date(2025,10,1)
    if today.month == 1:
        target_year = today.year - 1
        target_month = 12
    else:
        target_year = today.year
        target_month = today.month - 1

    logger.info(
        f"===== ETL MENSUAL | Periodo: {target_year}-{target_month:02d} ====="
    )


    # =========================
    # HOJA 1 – VENTAS
    # =========================
    logger.info("Procesando hoja de VENTAS Peri Collection")

    df_sales_pc_raw = extract_sheet(
        os.getenv("PERSYS_SHEET_ID"),
        os.getenv("WORKSHEET_NAME_1"),
        "sales",
        target_year,
        target_month
    )

    if df_sales_pc_raw.empty:
        logger.warning("No hay ventas este mes")
        df_sales_pc_final = pd.DataFrame()
    else:
        df_sales_pc_final = transform_ventas_peri_collection(df_sales_pc_raw)
"""""
    # =========================
    # HOJA 2 – EGRESOS / GASTOS
    # =========================
 
    logger.info("Procesando hoja de VENTAS Peri Institute")

    df_expenses_raw = extract_sheet(
        os.getenv("SHEET_ID_EXPENSES"),
        os.getenv("WORKSHEET_EXPENSES"),
        "expenses",
        year,
        month
    )

    if df_expenses_raw.empty:
        logger.warning("No hay egresos este mes")
        df_expenses_final = pd.DataFrame()
    else:
        df_expenses_final = transform_expenses_sheet(df_expenses_raw)

    # =========================
    # CONSOLIDACIÓN FINAL
    # =========================
    df_final = pd.concat(
        [df_sales_final, df_expenses_final],
        ignore_index=True
    )

    logger.info(f"Total registros consolidados: {len(df_final)}")

    if df_final.empty:
        logger.warning("No hay datos para cargar este mes")
        return

    load(df_final)

    logger.info("===== ETL MENSUAL FINALIZADO CORRECTAMENTE =====")
"""

if __name__ == "__main__":
    run_pipeline()
