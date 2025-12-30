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
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "income",
        "business_id": "negocio1",
        "category_id": 1,
        "amount": df["TotalPedido"].astype(float).round(2),
        "description": "Venta de vestidos Peri Collection",
        "reference": None,
        "from_account": None,
        "to_account": df.get("MetodoPago").apply(normalize_account),
        "is_invoiced": False,
        "id_referenced": df["IdPedido"].astype(str),
        "currency": "PEN"
    })

    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(5).to_string(index=False)
    )

    return df_transformed





def transform_ventas_peri_institute(df):
    logger.info("Transformando hoja de ventas Peri Institute")

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
    "PAYPAL": "Paypal",
    "BANCO DE MÉXICO": "Cuentas México",
    "BANCO DE MEXICO": "Cuentas México",
    "BANCO DE ECUADOR": "Cuentas Ecuador",
    "BANCO DE COLOMBIA": "Cuentas Colombia",
    "BANCO DE CHILE": "Cuentas Chile",
    "OTROS": "Sin Especificar"
    }


    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())
    
        
    def currency_fixed(value):
        if value == "Banco de México" or value == "Banco de Mexico":
            return "MXN"
        elif value == "Banco de Ecuador":
            return "USD"
        elif value == "PayPal":
            return "USD"
        elif value == "Banco de Chile":
            return "CLP"
        else:
            return "PEN"

    

    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "income",
        "business_id": "negocio2",
        "category_id": 2,
        "amount": df["MONTO_P"].astype(float).round(2),
        "description": "Venta de cursos en vivo Peri Institute",
        "reference": None,
        "from_account": None,
        "to_account": df.get("METODO_P").apply(normalize_account),
        "is_invoiced": False,
        "id_referenced": df["CODIGO_PAGO"].astype(str),
        "currency": df["METODO_P"].apply(currency_fixed)
    })
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(5).to_string(index=False)
    )

    return df_transformed

def transform_ventas_peri_institute_2(df):
    logger.info("Transformando hoja de ventas Peri Institute")

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
    "PAYPAL": "Paypal",
    "BANCO DE MÉXICO": "Cuentas México",
    "BANCO DE MEXICO": "Cuentas México",
    "BANCO DE ECUADOR": "Cuentas Ecuador",
    "BANCO DE COLOMBIA": "Cuentas Colombia",
    "BANCO DE CHILE": "Cuentas Chile",
    "OTROS": "Sin Especificar"
    }


    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())
    def currency_fixed(value):
        if None:
            return "PEN"
        elif value == "Banco de México" or value == "Banco de Mexico":
            return "MXN"
        elif value == "Banco de Ecuador":
            return "USD"
        elif value == "Paypal":
            return "USD"
        elif value == "Banco de Chile":
            return "CLP"
        else:
            return "PEN"

    

    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "income",
        "business_id": "negocio2",
        "category_id": 2,
        "amount": df["col_3"].astype(float).round(2),
        "description": "Venta de cursos en vivo Peri Institute (A)",
        "reference": None,
        "from_account": None,
        "to_account": df.get("col_4").apply(normalize_account),
        "is_invoiced": False,
        "id_referenced": df["col_2"].astype(str),
        "currency": df["col_4"].apply(currency_fixed)
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

def transform_ventas_peri_institute_3(df):
    logger.info("Transformando hoja de ventas Peri Institute")

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
    "PAYPAL": "Paypal",
    "BANCO DE MÉXICO": "Cuentas México",
    "BANCO DE MEXICO": "Cuentas México",
    "BANCO DE ECUADOR": "Cuentas Ecuador",
    "BANCO DE COLOMBIA": "Cuentas Colombia",
    "BANCO DE CHILE": "Cuentas Chile",
    "OTROS": "Sin Especificar"
    }


    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())
    def currency_fixed(value):
        if None:
            return "PEN"
        elif value == "Banco de México" or value == "Banco de Mexico":
            return "MXN"
        elif value == "Banco de Ecuador":
            return "USD"
        elif value == "Paypal":
            return "USD"
        elif value == "Banco de Chile":
            return "CLP"
        else:
            return "PEN"

    

    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "income",
        "business_id": "negocio2",
        "category_id": 2,
        "amount": df["col_22"].astype(float).round(2),
        "description": "Venta de cursos en vivo Peri Institute (A-M)",
        "reference": None,
        "from_account": None,
        "to_account": df.get("col_24").apply(normalize_account),
        "is_invoiced": False,
        "id_referenced": df["col_11"].astype(str),
        "currency": df["col_24"].apply(currency_fixed)
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

