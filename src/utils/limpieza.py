# Ruta: src/utils/limpieza.py
import pandas as pd
import json
import numpy as np

def aplicar_contrato_y_limpiar(df: pd.DataFrame, json_path: str, context) -> pd.DataFrame:
    """
    Compara el DataFrame contra el contrato JSON, reporta orfandad a Dagster,
    y aplica conversiones de tipos seguros (Int64).
    """
    # 1. Estandarizar columnas del DataFrame crudo (a mayúsculas y sin espacios)
    df.columns = [str(col).strip().upper() for col in df.columns]
    columnas_csv = set(df.columns)

    # 2. Cargar el contrato JSON
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            datos_json = json.load(f)
        ids_json = set([str(item["id"]).strip().upper() for item in datos_json])
    except FileNotFoundError:
        context.log.warning(f"No se encontró el contrato JSON en {json_path}. Se omite validación.")
        ids_json = columnas_csv # Truco para no fallar si no hay JSON

    # 3. Conciliación y Reporte a Dagster (Tolerancia a fallos)
    en_csv_no_en_json = columnas_csv - ids_json
    en_json_no_en_csv = ids_json - columnas_csv

    if en_csv_no_en_json:
        context.log.warning(
            f"⚠️ COLUMNAS HUÉRFANAS EN CSV: Se encontraron {len(en_csv_no_en_json)} "
            f"columnas no documentadas en el JSON: {sorted(list(en_csv_no_en_json))}"
        )
    
    if en_json_no_en_csv:
        context.log.warning(
            f"⚠️ COLUMNAS FALTANTES: El JSON esperaba {len(en_json_no_en_csv)} "
            f"columnas que NO vinieron en este CSV: {sorted(list(en_json_no_en_csv))}"
        )

    # 4. Limpieza de Datos y Conversión a Int64
    for col in df.columns:
        # Reemplazar espacios vacíos absolutos por NaN reales de Pandas
        df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
        
        # Intentar convertir a Int64 (El tipo que soporta nulos)
        # Ignoramos errores para que las columnas de texto puro se queden como texto
        try:
            df[col] = pd.to_numeric(df[col]).astype('Int64')
        except (ValueError, TypeError):
            pass # Si falla (ej. es una columna de observaciones de texto), se queda igual

    return df