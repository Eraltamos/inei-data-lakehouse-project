# Ruta: src/__init__.py
from dagster import Definitions, load_assets_from_modules
from src.assets import enaho

# Esto carga mágicamente todas las funciones @asset que estén en enaho.py
todos_los_activos = load_assets_from_modules([enaho])

defs = Definitions(
    assets=todos_los_activos,
)