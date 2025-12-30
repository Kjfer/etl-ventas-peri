from supabase import create_client
from dotenv import load_dotenv
import os
import pandas as pd
from logger import get_logger
from postgrest.exceptions import APIError

logger = get_logger("LOAD")

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def load(df: pd.DataFrame):
    logger.info(f"Cargando registros en Supabase: {len(df)}")
    # Mostrar columnas para ayudar a identificar claves foráneas
    logger.info(f"Columnas recibidas para carga: {df.columns.tolist()}")

    data = df.to_dict(orient="records")

    try:
        supabase.table("transactions").insert(
            data
        ).execute()
        logger.info("Carga mensual completada.")
    except Exception as e:
        # Loguear excepción de nivel superior
        logger.exception("Error en carga masiva a Supabase. Intentando inserción registro a registro para aislar conflicto.")

        # Intentar insertar registro por registro para identificar el conflictivo
        for idx, rec in enumerate(data):
            # Normalizar valores tipo numpy/pandas a nativos y convertir NaN a None
            rec_clean = {}
            for k, v in rec.items():
                try:
                    if pd.isna(v):
                        rec_clean[k] = None
                    elif hasattr(v, "item"):
                        rec_clean[k] = v.item()
                    else:
                        rec_clean[k] = v
                except Exception:
                    rec_clean[k] = v

            try:
                supabase.table("transactions").insert(rec_clean).execute()
            except Exception as e2:
                # Registrar el registro conflictivo con su índice y detalle del error
                logger.error(f"Registro conflictivo índice {idx}: {rec_clean}")
                logger.error(f"Error al insertar registro índice {idx}: {e2}")
                # Re-lanzar para que el proceso principal no continúe silenciosamente
                raise

        # Si todos los registros individuales pasan (improbable), relanzar la excepción original
        raise
