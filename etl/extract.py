import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from datetime import date

from logger import get_logger

logger = get_logger("EXTRACT")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_gspread_client():
    credentials = Credentials.from_service_account_info(
        json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
        scopes=SCOPES
    )
    return gspread.authorize(credentials)

def extract_sheet(sheet_id, worksheet_name, fuente, year, month):
    logger.info(f"Extrayendo datos | Sheet: {sheet_id}")

    ws = (
        get_gspread_client()
        .open_by_key(sheet_id)
        .worksheet(worksheet_name)
    )

    df = pd.DataFrame(ws.get_all_records())
    logger.info(f"Registros totales extraídos: {len(df)}")

    # =========================
    # NORMALIZACIÓN BÁSICA
    # =========================
    df.columns = df.columns.str.strip()

    # =========================
    # FILTRO POR ESTADO
    # =========================
    if "Estado" not in df.columns:
        logger.warning("La columna 'Estado' no existe en la hoja")
    else:
        total_antes = len(df)
        df = df[df["Estado"].str.upper().str.strip() == "ENVIADO"]
        logger.info(
            f"Filtro Estado=ENVIADO | "
            f"Antes: {total_antes} | Después: {len(df)}"
        )

    # =========================
    # CONVERSIÓN DE FECHA
    # =========================
    df["fecha"] = pd.to_datetime(df["FechaEntrega"], errors="coerce")

    # =========================
    # FILTRO MENSUAL
    # =========================
    total_antes = len(df)
    df = df[
        (df["fecha"].dt.year == year) &
        (df["fecha"].dt.month == month)
    ]

    logger.info(
        f"Filtro mensual {year}-{month:02d} | "
        f"Antes: {total_antes} | Después: {len(df)}"
    )

    # =========================
    # METADATA
    # =========================
    df["fuente"] = fuente

    # =========================
    # SAMPLE PARA VERIFICACIÓN
    # =========================
    if not df.empty:
        logger.info("Sample de registros extraídos:")
        logger.info(
            "\n" + df.head(5).to_string(index=False)
        )
    else:
        logger.warning("No hay registros luego de aplicar los filtros")

    return df

