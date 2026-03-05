# Ruta: src/assets/enaho.py
import pandas as pd
import os
from dagster import asset, AssetExecutionContext
from src.utils.limpieza import aplicar_contrato_y_limpiar
import duckdb


@asset
def enaho_modulo_100_silver(context: AssetExecutionContext):
    """
    Ingesta la muestra Bronze, valida contra el JSON Contract y materializa en Silver.
    """
    # 1. Definir Rutas
    bronze_path = "data/bronze/enaho/2023/ENAHO_01_2023_100_muestra.csv"
    silver_path = "data/silver/enaho/2023/enaho_100_muestra.parquet"
    # IMPORTANTE: Cambia esta ruta a la ubicación real de tu JSON del módulo 100
    json_path = "config/diccionarios/enaho/2023/ENAHO_01_2023_100.json" 

    os.makedirs(os.path.dirname(silver_path), exist_ok=True)

    # 2. Extracción
    context.log.info(f"Leyendo archivo crudo: {bronze_path}")
    df = pd.read_csv(bronze_path, encoding='latin1', dtype=str) # Leer como texto inicialmente

    # 3. Transformación (Aplicar el Contrato)
    context.log.info("Iniciando validación de contrato y casteo a Int64...")
    df_limpio = aplicar_contrato_y_limpiar(df, json_path, context)

    # 4. Carga
    df_limpio.to_parquet(silver_path, index=False)
    
    # Metadatos para la UI de Dagster
    context.log.info(f"Procesamiento finalizado. Guardado en: {silver_path}")
    return f"Filas: {len(df_limpio)} | Columnas procesadas: {len(df_limpio.columns)}"



@asset
def enaho_modulo_100_gold(context: AssetExecutionContext, enaho_modulo_100_silver):
    """
    Capa Gold: Consume el archivo Parquet de la capa Silver y construye
    una tabla analítica materializada en la base de datos DuckDB local.
    """
    # Definimos las rutas de lectura y escritura
    silver_path = "data/silver/enaho/2023/enaho_100_muestra.parquet"
    db_path = "data/gold/inei_lakehouse.db"

    # Aseguramos que el directorio físico exista en tu disco duro
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    context.log.info("Conectando al motor analítico local DuckDB...")
    
    # DuckDB crea el archivo .db automáticamente si no existe
    conn = duckdb.connect(db_path)

    # Ejecutamos la transformación SQL. 
    # La instrucción CREATE OR REPLACE garantiza la idempotencia del pipeline.
    query = f"""
        CREATE OR REPLACE TABLE gold_enaho_modulo_100 AS
        SELECT *
        FROM read_parquet('{silver_path}')
    """
    conn.execute(query)

    # Auditoría de calidad post-materialización
    resultado = conn.execute("SELECT COUNT(*) FROM gold_enaho_modulo_100").fetchone()
    conn.close()

    context.log.info(f"Capa Gold materializada. Filas insertadas en la base de datos: {resultado[0]}")
    
    return f"Base de datos analítica lista en {db_path}"