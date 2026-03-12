import requests
import zipfile
import os
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional

class IneiDownloader:
    """
    Descarga archivos masivos, extrae ZIPs anidados, filtra,
    renombra y convierte de CSV a Parquet de forma segura (sin inferir tipos).
    """
    def __init__(self, download_url: str, target_dir: str):
        self.download_url = download_url
        self.target_dir = Path(target_dir)
        self.headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }

    def _extraer_zips_anidados(self, directorio_base: Path):
        """Busca recursivamente archivos .zip dentro de la carpeta y los extrae."""
        for root, _, files in os.walk(directorio_base):
            for file in files:
                if file.lower().endswith('.zip'):
                    ruta_zip = Path(root) / file
                    try:
                        with zipfile.ZipFile(ruta_zip, 'r') as nested_z:
                            nested_z.extractall(Path(root))
                        os.remove(ruta_zip)
                    except zipfile.BadZipFile:
                        print(f"Advertencia: ZIP corrupto ignorado -> {file}")

    def _convertir_csv_a_parquet(self, csv_path: Path, parquet_path: Path):
        """
        Lee el CSV forzando todo a String para evitar pérdida de ceros a la izquierda
        (ej. UBIGEO) y lo guarda como Parquet optimizado.
        """
        try:
            # encoding='latin-1' es crucial para bases del INEI (maneja tildes/ñ)
            # dtype=str garantiza que NO se infieran números, protegiendo los códigos
            df = pd.read_csv(csv_path, encoding='latin-1', dtype=str, low_memory=False)
            df.to_parquet(parquet_path, index=False)
        except Exception as e:
            print(f"Error convirtiendo {csv_path.name}: {str(e)}")

    def download_and_extract(self, mapeo_archivos: Dict[str, str]) -> Path:
        """
        Descarga el ZIP, extrae todo, busca los CSVs definidos en el diccionario,
        los renombra, los convierte a Parquet y elimina la basura.
        """
        self.target_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = self.target_dir / "_temp_extract"
        temp_dir.mkdir(exist_ok=True)
        zip_path = temp_dir / "raw_data.zip"
        
        print("1. Descargando archivo masivo (Streaming)...")
        with requests.get(self.download_url, headers=self.headers, stream=True) as response:
            response.raise_for_status()
            with open(zip_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    
        print("2. Descomprimiendo archivos...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
        print("3. Buscando ZIPs anidados...")
        self._extraer_zips_anidados(temp_dir)
            
        print("4. Filtrando, renombrando y convirtiendo a Parquet...")
        archivos_procesados = 0
        
        for root, _, files in os.walk(temp_dir):
            for file in files:
                # Si el archivo exacto está en nuestro diccionario de mapeo
                if file in mapeo_archivos:
                    nuevo_nombre_base = mapeo_archivos[file]
                    csv_path = Path(root) / file
                    # Le añadimos la extensión .parquet
                    parquet_path = self.target_dir / f"{nuevo_nombre_base}.parquet"
                    
                    self._convertir_csv_a_parquet(csv_path, parquet_path)
                    archivos_procesados += 1
                    print(f"  -> Convertido: {file} -> {parquet_path.name}")
        
        print("5. Limpiando zona de aterrizaje (Eliminando PDFs y CSVs irrelevantes)...")
        shutil.rmtree(temp_dir)
        
        print(f"Proceso finalizado. {archivos_procesados} tablas Parquet generadas.")
        return self.target_dir