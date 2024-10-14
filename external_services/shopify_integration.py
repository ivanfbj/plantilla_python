# Importaciones de la biblioteca estándar de Python
import time
import os
import sys

# Importaciones de terceros
import requests
from decouple import Config, UndefinedValueError, RepositoryEnv
import pandas as pd

# Importaciones propias
from utils.logger import logger_info, logger_debug, logger_error
from database.conexion_db import conectar_bd_sqlalchemy
from database.consultas import ejecutar_sp_consulta
from utils.clean_logs import clean_old_logs
from utils.utilidades import ruta_recurso


# Intentamos cargar las credenciales desde las variables de entorno
try:
    config = Config(RepositoryEnv(ruta_recurso('.env')))
    
    SHOPIFY_API_KEY = config('SHOPIFY_API_KEY')
    SHOPIFY_API_SECRET = config('SHOPIFY_API_SECRET')
    SHOPIFY_ACCESS_TOKEN = config('SHOPIFY_ACCESS_TOKEN')
    SHOPIFY_STORE_NAME = config('SHOPIFY_STORE_NAME')
except UndefinedValueError as e:
    logger_error.error(f"Error al obtener una de las variables de entorno: {e}")
    raise

BASE_URL = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-07"

def put_inventory_levels(headers: dict, producto: pd.Series, location_id: int):
    """
    Actualiza los niveles de inventario de un producto específico en Shopify a través de la API.
    
    Esta función realiza una solicitud POST a la API de Shopify para actualizar los niveles
    de inventario de un artículo en una ubicación específica.

    Parámetros
    ----------
    headers : dict
        Diccionario con los encabezados necesarios para la autenticación en la API de Shopify.
    producto : pd.Series
        Serie de pandas que contiene los datos del producto, incluidos el 'inventory_item_id' y la 'CantidadDisponible'.
    location_id : int
        ID de la ubicación donde se deben actualizar los niveles de inventario.

    Retorna
    -------
    dict o None
        Un diccionario con la respuesta de la API si la solicitud fue exitosa, o None si ocurrió un error.

    Excepciones
    -----------
    ValueError
        Si alguno de los valores de entrada no es válido.
    requests.exceptions.RequestException
        Si ocurre un error en la solicitud HTTP.
    Exception
        Si la API de Shopify devuelve un código de estado distinto a 200.

    Ejemplos
    --------
    headers = {"X-Shopify-Access-Token": "tu_access_token"}
    producto = pd.Series({"inventory_item_id": 1234567890, "CantidadDisponible": 10, "sku": "ABC123"})
    location_id = 987654321
    put_inventory_levels(headers, producto, location_id)
    # Retorna:
    # {
    #     "inventory_item_id": 1234567890,
    #     "location_id": 987654321,
    #     "available": 10
    # }
    """
    try:
        # Validar que location_id sea un entero positivo
        if not isinstance(location_id, int) or location_id <= 0:
            logger_error.error(f"El ID de la ubicación proporcionado no es válido: {location_id}")
            raise ValueError("El ID de la ubicación debe ser un número entero positivo.")
        
        # Validar que producto tenga los campos necesarios
        required_fields = ['inventory_item_id', 'CantidadDisponible', 'sku']
        for field in required_fields:
            if field not in producto:
                logger_error.error(f"Falta el campo requerido '{field}' en el producto: {producto}")
                raise ValueError(f"El campo '{field}' es requerido en el producto.")
        
        # Construimos el cuerpo de la petición POST
        payload = {
            "inventory_item_id": producto['inventory_item_id'],
            "location_id": location_id,
            "available": producto['CantidadDisponible']
        }

        # Realizamos la petición POST a la URL especificada
        url_update_inventory = f"{BASE_URL}/inventory_levels/set.json"
        time.sleep(1)
        response = requests.post(url_update_inventory, json=payload, headers=headers)

        # Verificar si la respuesta fue exitosa
        if response.status_code == 200:
            logger_info.info(f'Registro Actualizado: SKU: {producto["sku"]} - inventory_item_id: {producto["inventory_item_id"]} - location_id: {location_id} - Cantidad Disponible: {producto["CantidadDisponible"]}')
            return response.json()
        else:
            # Manejo de error si la API devuelve un código de estado distinto de 200
            logger_error.error(f"Error en la actualización: {response.status_code}, {response.text} -> Registro: SKU: {producto['sku']} - inventory_item_id: {producto['inventory_item_id']} - location_id: {location_id} - Cantidad Disponible: {producto['CantidadDisponible']}")
            raise Exception(f"Error al actualizar el inventario: {response.status_code}, {response.text}")

    except requests.exceptions.RequestException as req_err:
        # Manejo de errores de conexión y HTTP
        logger_error.error(f"Error en la solicitud HTTP: {req_err}")
        raise requests.exceptions.RequestException(f"Error en la solicitud HTTP: {req_err}")
    
    except ValueError as ve:
        # Manejar errores de valores no válidos
        logger_error.error(f"Error en los datos de entrada: {ve}")
        raise
    
    except Exception as e:
        # Manejar cualquier otro error inesperado
        logger_error.error(f"Error inesperado en put_inventory_levels: {e}")
        raise Exception(f"Ocurrió un error inesperado: {e}") from e
    
def get_inventory_levels(headers: dict, inventory_item_id: int) -> dict:
    """
    Obtiene los niveles de inventario de un ítem específico en Shopify a través de la API.
    
    Esta función realiza una solicitud GET a la API de Shopify para obtener los niveles
    de inventario de un artículo dado por su `inventory_item_id`. Si la solicitud es
    exitosa, retorna los datos en formato JSON.

    Parámetros
    ----------
    headers : dict
        Diccionario con los encabezados necesarios para la autenticación en la API de Shopify.
    inventory_item_id : int
        ID del artículo de inventario para el cual se desean obtener los niveles de inventario.

    Retorna
    -------
    dict
        Un diccionario con los niveles de inventario obtenidos desde la API de Shopify.

    Excepciones
    -----------
    ValueError
        Si los parámetros proporcionados no son válidos.
    requests.exceptions.RequestException
        Si ocurre un error en la solicitud HTTP.
    Exception
        Si la API de Shopify devuelve un código de estado distinto a 200.

    Ejemplos
    --------
    headers = {"X-Shopify-Access-Token": "tu_access_token"}
    inventory_item_id = 1234567890
    get_inventory_levels(headers, inventory_item_id)
    # Retorna:
    # {
    #     "inventory_levels": [...]
    # }
    """
    try:
        # Validar que el inventory_item_id sea un entero positivo
        if not isinstance(inventory_item_id, int) or inventory_item_id <= 0:
            logger_error.error(f"El ID de inventario proporcionado no es válido: {inventory_item_id}")
            raise ValueError("El ID de inventario debe ser un número entero positivo.")
        
        # Verificar que los headers contengan el token de autenticación requerido
        if not headers.get("X-Shopify-Access-Token"):
            logger_error.error("Falta el encabezado de autenticación 'X-Shopify-Access-Token'.")
            raise ValueError("Encabezado de autenticación 'X-Shopify-Access-Token' faltante.")

        url_inventory = f"{BASE_URL}/inventory_levels.json?inventory_item_ids={inventory_item_id}"

        time.sleep(1)
        # Realizar la solicitud a la API de Shopify
        response = requests.get(url_inventory, headers=headers)

        # Verificar que la solicitud fue exitosa
        if response.status_code == 200:
            data = response.json()
            return data
        
        else:
            # Si el código de estado no es 200, registrar el error y lanzar una excepción
            logger_error.error(f"Error en la API de Shopify (Status Code: {response.status_code}): {response.text}")
            raise Exception(f"Error en la API de Shopify: {response.status_code}")
    
    except requests.exceptions.RequestException as req_err:
        # Manejar errores de conexión y HTTP
        logger_error.error(f"Error en la solicitud HTTP: {req_err}")
        raise requests.exceptions.RequestException(f"Error en la solicitud HTTP: {req_err}")
    
    except Exception as e:
        # Manejar cualquier otro error inesperado
        logger_error.error(f"Error inesperado en get_inventory_levels: {e}")
        raise Exception(f"Ocurrió un error inesperado: {e}") from e

def parse_pagination_links(link_header: str) -> dict:
    """
    Parsea el encabezado de enlaces de paginación de una respuesta HTTP.

    Esta función recibe un encabezado de tipo 'Link' que contiene múltiples enlaces
    separados por comas. Cada enlace tiene un valor rel (por ejemplo, "next" o "previous")
    que indica el tipo de paginación. La función devuelve un diccionario que asocia 
    cada tipo de relación con su URL correspondiente.

    Parámetros
    ----------
    link_header : str
        El valor del encabezado 'Link' de una respuesta HTTP que contiene enlaces de paginación.

    Retorna
    -------
    dict
        Un diccionario donde las claves son las relaciones de los enlaces ('next', 'previous', etc.)
        y los valores son las URLs correspondientes.

    Excepciones
    -----------
    ValueError
        Si el formato del encabezado de enlace es incorrecto y no puede ser separado adecuadamente.

    Ejemplos
    --------
    link_header = '<https://api.example.com?page=2>; rel="next", <https://api.example.com?page=1>; rel="previous"'
    parse_pagination_links(link_header)
    # Retorna:
    # {
    #     "next": "https://api.example.com?page=2",
    #     "previous": "https://api.example.com?page=1"
    # }
    """
    try:
        if not link_header:
            logger_error.error("El encabezado de enlaces está vacío.")
            raise ValueError("El encabezado de enlaces está vacío.")

        links = link_header.split(", ")
        pagination_links = {}

        for link in links:
            # Asegurarse de que el enlace tiene la estructura esperada
            if ';' not in link:
                logger_error.error(f"Formato incorrecto en el enlace: '{link}'")
                raise ValueError(f"Formato incorrecto en el enlace: '{link}'")

            url, rel = link.split("; ")
            url = url.strip("<>")
            rel = rel.split("=")[1].strip('"')

            # Validar que tanto la URL como la relación (rel) sean válidas
            if not url or not rel:
                logger_error.error(f"El enlace o relación no son válidos: '{link}'")
                raise ValueError(f"El enlace o relación no son válidos: '{link}'")

            pagination_links[rel] = url

        return pagination_links

    except ValueError as ve:
        # Lanza un error detallado si el encabezado no tiene el formato correcto
        logger_error.error(f"Error al procesar el encabezado de enlaces: {ve}")
        raise ValueError(f"Error al procesar el encabezado de enlaces: {ve}")
    except Exception as e:
        # Captura cualquier otro error inesperado y lo lanza
        logger_error.error(f"Ocurrió un error inesperado: {e}")
        raise Exception(f"Ocurrió un error inesperado: {e}")

def get_product_page(url: str, headers: dict) -> tuple:
    """
    Obtiene una página de productos desde la API de Shopify.

    Esta función realiza una solicitud GET a la API de Shopify para obtener una lista
    de productos en formato JSON. Además, procesa los enlaces de paginación si están 
    presentes en los encabezados de la respuesta.

    Parámetros
    ----------
    url : str
        La URL de la API de Shopify para la solicitud de productos.
    headers : dict
        Diccionario con los encabezados necesarios para la autenticación en la API de Shopify.

    Retorna
    -------
    tuple
        Un par (data, pagination_links), donde `data` es una lista de productos y 
        `pagination_links` es un diccionario con los enlaces de paginación (o None si no hay paginación).

    Excepciones
    -----------
    ValueError
        Si la URL o los headers son inválidos.
    requests.exceptions.RequestException
        Si ocurre un error en la solicitud HTTP.
    Exception
        Si la API de Shopify devuelve un código de estado distinto a 200.

    Ejemplos
    --------
    url = "https://your-shopify-store.myshopify.com/admin/api/2021-07/products.json"
    headers = {"X-Shopify-Access-Token": "tu_access_token"}
    get_product_page(url, headers)
    # Retorna:
    # (data, pagination_links)
    """
    try:
        # Validar que la URL y los headers sean correctos
        if not isinstance(url, str) or not url:
            logger_error.error("La URL proporcionada no es válida.")
            raise ValueError("La URL proporcionada no es válida.")
        
        if not isinstance(headers, dict) or not headers.get("X-Shopify-Access-Token"):
            logger_error.error("Los headers proporcionados no son válidos o faltan los encabezados de autenticación.")
            raise ValueError("Los headers proporcionados no son válidos o faltan los encabezados de autenticación.")

        # Realizar la solicitud GET
        response = requests.get(url, headers=headers)

        # Verificar el código de estado de la respuesta
        if response.status_code == 200:
            # Obtener los datos en formato JSON
            data = response.json()

            # Obtener los enlaces de paginación desde los encabezados, si existen
            header_link_paginas = response.headers.get('link', None)
            if header_link_paginas:
                # Procesar los enlaces de paginación
                pagination_links = parse_pagination_links(header_link_paginas)
                logger_info.info(f"Paginación detectada. Procesando enlaces de paginación.")
                return data, pagination_links

            logger_info.info(f"Datos obtenidos exitosamente de la API de Shopify sin paginación.")
            return data, None

        else:
            # Registrar y lanzar excepción si la respuesta no es exitosa
            logger_error.error(f"Error al consumir la API de Shopify: {response.status_code}, {response.text}")
            raise Exception(f"Error al consumir la API de Shopify: {response.status_code}")

    except requests.exceptions.RequestException as req_err:
        # Manejo de errores de red y conexión
        logger_error.error(f"Error en la solicitud HTTP: {req_err}")
        raise requests.exceptions.RequestException(f"Error en la solicitud HTTP: {req_err}")
    
    except ValueError as ve:
        # Manejar errores de validación
        logger_error.error(f"Error en los parámetros de entrada: {ve}")
        raise
    
    except Exception as e:
        # Manejar cualquier otro error inesperado
        logger_error.error(f"Error inesperado en get_product_page: {e}")
        raise Exception(f"Ocurrió un error inesperado: {e}") from e


def get_all_products_pages(api_url: str, headers: dict) -> list:
    """
    Obtiene todos los productos paginados desde la API de Shopify.

    Esta función realiza múltiples solicitudes GET a la API de Shopify para obtener todas
    las páginas de productos disponibles, manejando la paginación automáticamente.

    Parámetros
    ----------
    api_url : str
        La URL base de la API de Shopify para la solicitud de productos.
    headers : dict
        Diccionario con los encabezados necesarios para la autenticación en la API de Shopify.

    Retorna
    -------
    list
        Una lista que contiene todos los productos obtenidos desde la API de Shopify.

    Excepciones
    -----------
    requests.exceptions.RequestException
        Si ocurre un error en alguna solicitud HTTP.
    Exception
        Si la API de Shopify devuelve un código de estado distinto a 200 en alguna solicitud.

    Ejemplos
    --------
    api_url = "https://your-shopify-store.myshopify.com/admin/api/2021-07/products.json"
    headers = {"X-Shopify-Access-Token": "tu_access_token"}
    all_products = get_all_products_pages(api_url, headers)
    # Retorna:
    # [ {...}, {...}, {...} ]  # Lista de productos
    """
    all_data = []
    current_url = api_url
    
    try:
        while current_url:
            # Obtener los datos de la página actual y los enlaces de paginación
            data, pagination_links = get_product_page(current_url, headers)

            # Agregar los datos de la página actual a la lista de resultados
            if 'products' in data:
                all_data.extend(data['products'])  # Asumimos que el endpoint devuelve un campo 'products'
                logger_info.info(f"Obtenidos {len(data['products'])} productos de la página {current_url}.")
            else:
                logger_error.error(f"El formato de los datos es inesperado en {current_url}.")
                raise Exception(f"El formato de los datos es inesperado en {current_url}.")

            # Verificar si hay una página siguiente en los enlaces de paginación
            if pagination_links and 'next' in pagination_links:
                current_url = pagination_links['next']
                logger_info.info(f"Siguiente página encontrada: {current_url}.")
            else:
                # No hay más páginas, salimos del bucle
                logger_info.info("No se encontraron más páginas. Finalizando la recolección de productos.")
                current_url = None

        return all_data
    
    except requests.exceptions.RequestException as req_err:
        logger_error.error(f"Error en la solicitud HTTP: {req_err}")
        raise requests.exceptions.RequestException(f"Error en la solicitud HTTP: {req_err}")
    
    except Exception as e:
        logger_error.error(f"Error inesperado al obtener productos: {e}")
        raise Exception(f"Ocurrió un error inesperado al obtener productos: {e}") from e


