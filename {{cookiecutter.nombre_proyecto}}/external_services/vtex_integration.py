# Importaciones de la biblioteca estándar de Python
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

# Importaciones propias
from utils.logger import logger_info, logger_debug, logger_error

# Importaciones de terceros
import pandas as pd
from tqdm import tqdm
import aiohttp
from tqdm.asyncio import tqdm_asyncio

def build_creation_date_param(days_back):
    """
    Construye el parámetro f_creationDate con un rango de fechas dinámico.

    :param days_back: Número de días hacia atrás desde hoy para la fecha inicial.
    :return: Cadena formateada para el parámetro f_creationDate.
    """
    # Fecha actual (día final del rango) con zona horaria UTC
    end_date = datetime.now(timezone.utc)
    # Fecha inicial (día inicial del rango)
    start_date = end_date - timedelta(days=days_back)

    # Formatear las fechas según el formato de VTEX
    start_date_str = start_date.strftime("%Y-%m-%dT02:00:00.000Z")
    end_date_str = end_date.strftime("%Y-%m-%dT01:59:59.999Z")

    # Construir el rango de fechas
    return f"creationDate:[{start_date_str} TO {end_date_str}]"

def inicializa_endpoint(base_url, params, app_key, app_token) -> list:
    """
    Inicializa un endpoint y recorre todas las páginas devolviendo los datos completos.
    
    :param bodega: Nombre de la bodega (para registros)
    :param base_url: URL base del endpoint de VTEX
    :param params: Diccionario con los parámetros de la consulta
    :param app_key: Clave de la API de VTEX
    :param app_token: Token de la API de VTEX
    :return: Lista completa de datos obtenidos
    """
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'X-VTEX-API-AppKey': app_key,
        'X-VTEX-API-AppToken': app_token,
    }

    all_data = []  # Lista para almacenar los datos de todas las páginas
    current_page = 1

    try:
        while True:
            # Actualizar los parámetros con la página actual
            params['page'] = current_page
            query_string = urlencode(params)
            url = f"{base_url}?{query_string}"

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                
                # Procesar la lista de datos
                if data.get('list'):
                    all_data.extend(data['list'])  # Agregar datos a la lista total

                # Revisar si hay más páginas
                paging = data.get('paging', {})
                if paging.get('currentPage') >= paging.get('pages', 1):
                    break  # Salir del bucle si estamos en la última página

                # Avanzar a la siguiente página
                current_page += 1
            else:
                logger_error.error(f"Error HTTP {response.status_code}: {response.text} | URL: {url}")
                break  # Salir en caso de error

    except requests.exceptions.RequestException as e:
        logger_error.error(f"Error de conexión: {str(e)} | URL: {base_url}")
    except ValueError as e:
        logger_error.error(f"Error de decodificación JSON: {str(e)} | URL: {base_url}")
    except Exception as e:
        logger_error.error(f"Error desconocido: {str(e)} | URL: {base_url}")

    return all_data

"""
days_back = 8  # Cambia esto para ajustar el rango dinámico
endpoint_list_orders_vtex = 'api/oms/pvt/orders/'

# Definición de parametros para el Endpoint de VTEX
estados_a_filtrar = 'ready-for-handling,handling'
# estados_a_filtrar = 'ready-for-handling'
f_creation_date = build_creation_date_param(days_back)
params = {
    'orderBy': 'authorizedDate,desc',
    'per_page': 100,
    'f_status': estados_a_filtrar,
    # 'f_creationDate': 'creationDate:[2024-09-30T02:00:00.000Z TO 2024-11-30T01:59:59.999Z]'
    'f_creationDate': f_creation_date
}
    
df_full_information = procesar_datos_bodegas(df_apis_vtex, endpoint_list_orders_vtex, estados_a_filtrar, f_creation_date, params, nombre_carpeta_exportacion, logger_info)
"""

async def fetch_inventory_async(session, base_url, headers, warehouseId):
    try:
        async with session.get(base_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()

                # Validar que 'balance' esté en la respuesta y sea una lista
                if 'balance' in data and isinstance(data['balance'], list):
                    # Buscar el warehouseId específico
                    matching_warehouse = next(
                        (entry for entry in data['balance'] if entry['warehouseId'] == warehouseId),
                        None
                    )
                    if matching_warehouse:
                        return matching_warehouse.get('totalQuantity', 'N/A')
                    else:
                        return f"No data for warehouseId {warehouseId}"
                else:
                    return "No balance data returned by VTEX"
            else:
                return f"Error {response.status}: {response.reason}"
    except Exception as e:
        return f"Exception occurred: {str(e)}"

async def list_inventory_by_sku_async(data_frame: pd.DataFrame):
    # Agregar columnas adicionales al DataFrame
    data_frame['DatosVTEX'] = 'DatosVtex->'
    data_frame['totalQuantity'] = None

    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in data_frame.iterrows():
            base_url = row['ApiCliente']
            warehouseId = row['warehouseId']
            app_key = row['AppKey']
            app_token = row['AppToken']

            headers = {
                'Accept': "application/json",
                'Content-Type': "application/json",
                'X-VTEX-API-AppKey': app_key,
                'X-VTEX-API-AppToken': app_token,
            }

            # Crear una tarea para cada solicitud
            tasks.append(fetch_inventory_async(session, base_url, headers, warehouseId))

        # Ejecutar las tareas de manera concurrente
        results = await tqdm_asyncio.gather(*tasks, desc="Consultando inventario de cada registro")

        # Asignar los resultados al DataFrame
        data_frame['totalQuantity'] = results

    return data_frame