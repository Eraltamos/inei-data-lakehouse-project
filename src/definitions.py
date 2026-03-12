from dagster import Definitions, load_assets_from_package_module
from src import assets

# 1. Usaremos el descubrimiento dinámico de Assets
# Dagster escaneará de forma recursiva todo dentro de src/assets/ 
# y cargará cualquier función decorada con @asset o @multi_asset
all_assets = load_assets_from_package_module(assets)

# 2. Configuración de Recursos
# (Por ahora vacío, aquí inyectaremos DuckDB más adelante si es que no hay cambios mayores)
resources_config = {
    # "duckdb": TuDuckDBResource()
}

# 3. Definición del Proyecto
defs = Definitions(
    assets=all_assets,
    resources=resources_config,
    # Aquí en el futuro agregaremos schedules y sensors para automatización
)