import pandas as pd
from logger import get_logger

logger = get_logger("TRANSFORM")

def transform_ventas_peri_collection(df):
    logger.info("Transformando hoja de VENTAS Peri Collection")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df

    # =========================
    # NORMALIZADOR DE CUENTAS
    # =========================
    ACCOUNT_MAP = {
        "BANCO DE LA NACIÓN": "Banco de la Nación",
        "SCOTIABANK": "Scotiabank",
        "INTERBANK": "Interbank",
        "YAPE": "Yape",
        "PLIN": "Plin",
        "BBVA": "BBVA",
        "BCP": "BCP",
        "TARJETA LINK": "Tarjeta LINK",
        "EN EFECTIVO": "En Efectivo"
    }

    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())

    # =========================
    # TRANSFORMACIÓN
    # =========================
    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.date,
        "type": "income",
        "business_id": "negocio1",
        "category_id": 1,
        "amount": df["TotalPedido"].astype(float),
        "description": "Venta de vestidos Peri Collection",
        "reference": None,
        "from_account": None,
        "to_account": df.get("MetodoPago").apply(normalize_account),
        "is_invoiced": False,
        "id_referenced": df["IdPedido"].astype(str)
    })

    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed





def transform_expenses_sheet(df):
    logger.info("Transformando hoja de EGRESOS")

    return pd.DataFrame({
        "date": df["fecha"].dt.date,
        "type": "expense",
        "business_id": df["negocio"],
        "category_id": df.get("categoria"),
        "amount": df["importe"].astype(float),
        "description": df.get("detalle"),
        "reference": df.get("comprobante"),
        "from_account": df.get("cuenta_origen"),
        "to_account": None,
        "is_invoiced": False
    })

