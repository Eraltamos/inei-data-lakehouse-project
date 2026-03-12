import os
from dagster import asset, AssetExecutionContext, Config
from pydantic import Field
from typing import Dict
from src.utils.inei_downloader import IneiDownloader

# 1. Definimos la configuración, incluyendo tu diccionario semántico
class EnahoBronzeConfig(Config):
    url_zip: str = Field(
        default="https://www.inei.gob.pe/media/DATOS_ABIERTOS/ENAHO/DATA/2023.zip",
        description="URL directa del ZIP descubierta vía la API"
    )
    anio: str = Field(default="2023")
    
    # Aquí inyectamos el diccionario de mapeo que proporcionaste
    diccionario_mapeo: Dict[str, str] = Field(
        default={
            "Enaho01-2023-100.csv": "inei_2023_enaho01_100",
            "Enaho01-2023-200.csv": "inei_2023_enaho01_200",
            "Enaho01-2023-601.csv": "inei_2023_enaho01_601",
            "Enaho01-2023-602.csv": "inei_2023_enaho01_602",
            "Enaho01-2023-602A.csv": "inei_2023_enaho01_602a",
            "Enaho01-2023-602B.csv": "inei_2023_enaho01_602b",
            "Enaho01-2023-603.csv": "inei_2023_enaho01_603",
            "Enaho01-2023-604.csv": "inei_2023_enaho01_604",
            "Enaho01-2023-605.csv": "inei_2023_enaho01_605",
            "Enaho01-2023-606.csv": "inei_2023_enaho01_606",
            "Enaho01-2023-606D.csv": "inei_2023_enaho01_606d",
            "Enaho01-2023-607.csv": "inei_2023_enaho01_607",
            "Enaho01-2023-609.csv": "inei_2023_enaho01_609",
            "Enaho01-2023-610.csv": "inei_2023_enaho01_610",
            "Enaho01-2023-611.csv": "inei_2023_enaho01_611",
            "Enaho01-2023-612.csv": "inei_2023_enaho01_612",
            "Enaho01-2023-613.csv": "inei_2023_enaho01_613",
            "Enaho01-2023-613H.csv": "inei_2023_enaho01_613h",
            "Enaho01-2023-700.csv": "inei_2023_enaho01_700",
            "Enaho01-2023-700A.csv": "inei_2023_enaho01_700a",
            "Enaho01-2023-700B.csv": "inei_2023_enaho01_700b",
            "Enaho01-2023-800A.csv": "inei_2023_enaho01_800a",
            "Enaho01-2023-800B.csv": "inei_2023_enaho01_800b",
            "Enaho01A-2023-300.csv": "inei_2023_enaho01a_300",
            "Enaho01A-2023-400.csv": "inei_2023_enaho01a_400",
            "Enaho01a-2023-500.csv": "inei_2023_enaho01a_500",
            "Enaho01B-2023-1.csv": "inei_2023_enaho01b_1",
            "Enaho01B-2023-2.csv": "inei_2023_enaho01b_2",
            "Enaho02-2023-2000.csv": "inei_2023_enaho02_2000",
            "Enaho02-2023-2000A.csv": "inei_2023_enaho02_2000a",
            "Enaho02-2023-2100.csv": "inei_2023_enaho02_2100",
            "Enaho02-2023-2200.csv": "inei_2023_enaho02_2200",
            "Enaho02-2023-2300.csv": "inei_2023_enaho02_2300",
            "Enaho02-2023-2400.csv": "inei_2023_enaho02_2400",
            "Enaho02-2023-2500.csv": "inei_2023_enaho02_2500",
            "Enaho02-2023-2600.csv": "inei_2023_enaho02_2600",
            "Enaho02-2023-2700.csv": "inei_2023_enaho02_2700",
            "Enaho04-2023-1-Preg-1-a-13.csv": "inei_2023_enaho04_p1-13",
            "Enaho04-2023-2-Preg-14-a-22.csv": "inei_2023_enaho04_p14-22",
            "Enaho04-2023-3-Preg-23.csv": "inei_2023_enaho04_p23",
            "Enaho04-2023-4-Preg-24.csv": "inei_2023_enaho04_p24",
            "Enaho04-2023-5-Preg-25.csv": "inei_2023_enaho04_p25",
            "Sumaria-2023.csv": "inei_2023_enaho_sumaria_8g",
            "Sumaria-2023-12g.csv": "inei_2023_enaho_sumaria_12g"
        },
        description="Mapeo de nombres originales del INEI a nomenclatura Lakehouse"
    )

@asset(
    compute_kind="pandas",
    group_name="enaho_bronze",
    tags={"layer": "bronze", "domain": "enaho"}
)
def enaho_dataset_masivo_bronze(context: AssetExecutionContext, config: EnahoBronzeConfig) -> str:
    """
    Descarga el ZIP, filtra, renombra y transforma a Parquet protegiendo
    tipos de datos.
    """
    destino_bronze = f"data/bronze/enaho/{config.anio}"
    
    context.log.info(f"Iniciando ingesta Bronze para {len(config.diccionario_mapeo)} módulos de ENAHO {config.anio}")
    
    downloader = IneiDownloader(
        download_url=config.url_zip, 
        target_dir=destino_bronze
    )
    
    ruta_guardada = downloader.download_and_extract(mapeo_archivos=config.diccionario_mapeo)
    
    # Auditoría para la UI de Dagster
    archivos_parquet = [f for f in os.listdir(ruta_guardada) if f.endswith('.parquet')]
    context.log.info(f"Ingesta exitosa. {len(archivos_parquet)} archivos Parquet materializados.")
    
    # Enviamos metadatos a Dagster
    context.add_output_metadata({
        "archivos_generados": len(archivos_parquet),
        "ruta_almacenamiento": str(ruta_guardada),
        "modulos_ingestados": str([f.replace('.parquet', '') for f in archivos_parquet][:5]) + "..."
    })
    
    return str(ruta_guardada)