import os
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from extract import (
    extract_sheet_pc,
    extract_sheet_pi,
    extract_sheet_pi_2,
    extract_sheet_pi_3
)
from transform import (
    transform_ventas_peri_collection,
    transform_ventas_peri_institute,
    transform_ventas_peri_institute_2,
    transform_ventas_peri_institute_3
)
from load import load
from logger import get_logger

logger = get_logger("PIPELINE")

load_dotenv()

def run_pipeline(year=None, month=None):
    # =========================
    # DEFINICIÓN DE PERIODO
    # =========================
    today = date.today()
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
    # HOJA 1 – VENTAS PC
    # =========================
    logger.info("Procesando hoja de VENTAS Peri Collection")

    df_sales_pc_raw = extract_sheet_pc(
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

    # =========================
    # HOJA 2 – ventas PI
    # =========================
 
    logger.info("Procesando hoja de VENTAS Peri Institute")

    df_sales_pi_raw = extract_sheet_pi(
        os.getenv("PROTO_INSTITUTE_ID"),
        os.getenv("WORKSHEET_NAME_2"),
        target_year,
        target_month
    )

    if df_sales_pi_raw.empty:
        logger.warning("No hay ingresos este mes")
        df_sales_pi_final = pd.DataFrame()
    else:
        df_sales_pi_final = transform_ventas_peri_institute(df_sales_pi_raw)

    # =========================
    # HOJA 3 – ventas hoja antigua PI
    # =========================
 
    logger.info("Procesando hoja de VENTAS Peri Institute 2")

    df_sales_pi2_raw = extract_sheet_pi_2(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_NAME_3"),
        target_year,
        target_month
    )

    if df_sales_pi2_raw.empty:
        logger.warning("No hay ingresos este mes")
        df_sales_pi2_final = pd.DataFrame()
    else:
        df_sales_pi2_final = transform_ventas_peri_institute_2(df_sales_pi2_raw)

    # =========================
    # HOJA 4 – ventas hoja matricula antigua PI
    # =========================
 
    logger.info("Procesando hoja de VENTAS Peri Institute 3")

    df_sales_pi3_raw = extract_sheet_pi_3(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_NAME_4"),
        target_year,
        target_month
    )

    if df_sales_pi3_raw.empty:
        logger.warning("No hay ingresos este mes")
        df_sales_pi3_final = pd.DataFrame()
    else:
        df_sales_pi3_final = transform_ventas_peri_institute_3(df_sales_pi3_raw)

        
    # =========================
    # CONSOLIDACIÓN FINAL
    # =========================
    df_final = pd.concat(
        [df_sales_pi_final, df_sales_pc_final, df_sales_pi2_final, df_sales_pi3_final],
        ignore_index=True
    )

    logger.info(f"Total registros consolidados: {len(df_final)}")

    if df_final.empty:
        logger.warning("No hay datos para cargar este mes")
        return

    load(df_final)

    logger.info("===== ETL MENSUAL FINALIZADO CORRECTAMENTE =====")


if __name__ == "__main__":
    run_pipeline()
