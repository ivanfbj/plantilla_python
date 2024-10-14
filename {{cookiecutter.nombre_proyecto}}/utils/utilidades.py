# Imports de Python
import os
import re
import sys
import time
import json
from datetime import datetime
from typing import Callable
from email.message import EmailMessage
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Imports de terceros
import pandas as pd
import requests
from tqdm import tqdm

# Imports propios
from utils.bcolors import bcolors
# from utils.variables_entorno import obj_variables_entorno
from utils.logger import logger_info, logger_debug, logger_error

# Crear una instancia de variables_entorno
# dot_env = obj_variables_entorno


def ruta_recurso(nombre_archivo: str):
    """
    Devuelve la ruta de un archivo cuando se ejecuta desde el código fuente o desde el .exe creado con pyinstaller.

    Args:
        nombre_archivo (str): El nombre del archivo cuya ruta se quiere obtener.

    Returns:
        str: La ruta completa del archivo.
    """
    # Verificar si el script se está ejecutando desde un ejecutable de PyInstaller
    if getattr(sys, 'frozen', False):
        # En este caso, sys._MEIPASS contiene la ruta a la carpeta temporal
        base_path = sys._MEIPASS
    else:
        # Si no es un ejecutable de PyInstaller, obtener la ruta de la carpeta actual
        base_path = os.path.abspath(".")
    # Unir la ruta base con el nombre del archivo y devolverla
    return os.path.join(base_path, nombre_archivo)

def convert_list_to_data_frame(list_all_products: list) -> pd.DataFrame:
    """
    Convierte una lista de productos en un DataFrame de pandas.

    Esta función toma una lista de productos obtenidos de la API de Shopify y extrae 
    sus variantes, incluyendo campos como 'id', 'title', 'sku' e 'inventory_item_id', 
    para convertirlos en un DataFrame de pandas.

    Parámetros
    ----------
    list_all_products : list
        Lista de productos obtenidos desde la API de Shopify. Cada producto debe ser 
        un diccionario con un campo 'variants', que es una lista de variantes.

    Retorna
    -------
    pd.DataFrame
        Un DataFrame con los productos y variantes, incluyendo las columnas 'id', 
        'title', 'sku', e 'inventory_item_id'.

    Excepciones
    -----------
    ValueError
        Si la lista proporcionada no contiene la estructura esperada.
    
    Ejemplos
    --------
    products = [
        {'id': 123, 'title': 'Product 1', 'variants': [{'sku': 'SKU1', 'inventory_item_id': 111}]},
        {'id': 456, 'title': 'Product 2', 'variants': [{'sku': 'SKU2', 'inventory_item_id': 222}]}
    ]
    df = convert_list_to_data_frame(products)
    # Retorna un DataFrame con las columnas 'id', 'title', 'sku', 'inventory_item_id'
    """
    # Validar que la entrada es una lista
    if not isinstance(list_all_products, list):
        logger_error.error("El parámetro list_all_products no es una lista.")
        raise ValueError("Se esperaba una lista de productos.")

    productos_data = []

    try:
        # Recorrer todos los productos
        for producto in list_all_products:
            # Validar que cada producto tiene las claves 'id', 'title' y 'variants'
            if 'id' not in producto or 'title' not in producto or 'variants' not in producto:
                logger_error.error(f"Estructura de producto no válida: {producto}")
                raise ValueError(f"Producto sin la estructura esperada: {producto}")
            
            product_id = producto['id']
            title = producto['title']
            producto_variantes = producto['variants']

            # Validar que las variantes son una lista
            if not isinstance(producto_variantes, list):
                logger_error.error(f"Las variantes del producto {product_id} no son una lista.")
                raise ValueError(f"Variantes del producto {product_id} no tienen la estructura esperada.")

            # Recorrer todas las variantes del producto
            for variante in producto_variantes:
                # Validar que las variantes contienen 'sku' y 'inventory_item_id'
                if 'sku' not in variante or 'inventory_item_id' not in variante:
                    logger_error.error(f"Estructura de variante no válida: {variante}")
                    raise ValueError(f"Variante sin la estructura esperada: {variante}")

                sku = variante['sku']
                inventory_item_id = variante['inventory_item_id']
                
                # Agregar los datos a la lista como un diccionario
                productos_data.append({
                    'id': product_id,
                    'title': title,
                    'sku': sku,
                    'inventory_item_id': inventory_item_id
                })
        
        # Convertir la lista de diccionarios a un DataFrame de pandas
        df = pd.DataFrame(productos_data)
        logger_info.info(f"Se convirtieron {len(df)} productos/variantes en un DataFrame.")
        return df

    except ValueError as ve:
        logger_error.error(f"Error en la conversión de productos a DataFrame: {ve}")
        raise

    except Exception as e:
        logger_error.error(f"Error inesperado en convert_list_to_data_frame: {e}")
        raise Exception(f"Ocurrió un error inesperado en convert_list_to_data_frame: {e}") from e


# def exportar_informacion(lista_informacion: list, nombre_archivo: str):
#     if lista_informacion:
#         df = pd.DataFrame(lista_informacion)
#         print(f'{bcolors.WARNING} \t{len(df)} registros exportados al CSV {nombre_archivo}.')
        
#         # Obtener la fecha y hora actual
#         fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
        
#         nombre_carpeta_exportar = 'ArchivosExportados'
#         crear_carpeta(nombre_carpeta_exportar)
        
#         # Crear el nombre del archivo con la fecha y hora actual
#         nombre_archivo = f'{nombre_carpeta_exportar}/{nombre_archivo}_{fecha_actual}.csv'
        
#         df.to_csv(nombre_archivo, index=False, encoding='latin1',sep=';')


# def crear_carpeta(nombre_carpeta: str):
#     """
#     Función para crear una carpeta si no existe.
#     :param nombre_carpeta: El nombre de la carpeta a crear.
#     """
#     if not os.path.exists(nombre_carpeta):
#         os.makedirs(nombre_carpeta)
#         # print(f'Se ha creado la carpeta "{nombre_carpeta}"')
#     # else:
#     #     print(f'La carpeta "{nombre_carpeta}" ya existe')


# def guardar_log_ejecucion(mensaje_log_ejecucion: str, nombre_carpeta: str = 'LogEjecucion'):
#     """
#     Guarda un mensaje de error en un archivo de registro de ejecución.
#     Adicionalmente lo lleva a un atributo de la clase "variables_entorno" para que se pueda enviar por correo electronico.

#     Args:
#         mensaje_log_ejecucion (str): El mensaje de log que se va a guardar en el archivo de registro.
#         nombre_carpeta (str, opcional): El nombre de la carpeta donde se almacenará el archivo de registro. 
#             Por defecto es 'LogEjecucion'.

#     Returns:
#         None

#     Examples:
#         >>> guardar_log_ejecucion("Error al procesar los datos")
#         # Guarda el mensaje de error en un archivo de registro dentro de la carpeta 'LogEjecucion'.
#     """
#     fecha_actual = {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#     crear_carpeta(nombre_carpeta)
#     nombre_archivo = f"{nombre_carpeta}/LogEjecucion_{datetime.now().strftime('%Y-%m-%d')}.txt"
    
#     mensaje_completo = f'{fecha_actual} - {mensaje_log_ejecucion}\n' 
#     dot_env.concatenar_mensaje_correo(mensaje_completo)
    
#     with open(nombre_archivo, 'a', encoding='cp1252') as archivo:
#         archivo.write(mensaje_completo)


# def guardar_error(mensaje_error: str, nombre_carpeta: str = 'ErroresApp'):
#     """
#     Guarda un mensaje de error en un archivo de registro de errores y muestra un mensaje de alerta.

#     Args:
#         mensaje_error (str): El mensaje de error que se va a guardar en el archivo de registro.
#         nombre_carpeta (str, opcional): El nombre de la carpeta donde se almacenará el archivo de registro de errores.
#             Por defecto es 'ErroresApp'.

#     Returns:
#         None

#     Examples:
#         >>> guardar_error("Error crítico en la aplicación")
#         # Guarda el mensaje de error en un archivo de registro de errores dentro de la carpeta 'ErroresApp' 
#         # y muestra un mensaje de alerta.
#     """
#     fecha_actual = {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#     crear_carpeta(nombre_carpeta)
#     nombre_archivo = f"{nombre_carpeta}/ErrorApp_{datetime.now().strftime('%Y-%m-%d')}.txt"
#     with open(nombre_archivo, 'a', encoding='cp1252') as archivo:
#         archivo.write(f'{fecha_actual} - {mensaje_error}\n')
#     print(f'{bcolors.MESSAGE_FAIL}Se ha generado un error se debe verificar el archivo "{nombre_archivo}"{bcolors.RESET}')


# def inicializaEndpoint_get(Api_key: str, value_api_key: str, url: str):
#     """
#     Realiza una solicitud GET a un endpoint especificado con la clave de la API proporcionada.

#     Args:
#         Api_key (str): Nombre del encabezado de la API key.
#         value_api_key (str): Valor de la API key.
#         url (str): URL del endpoint al que se realizará la solicitud GET.

#     Returns:
#         dict or None: Si la solicitud es exitosa, devuelve los datos en formato JSON. 
#         En caso de error, devuelve None y registra el error en el archivo de registro.

#     Examples:
#         >>> inicializaEndpoint_get("Authorization", "api_key_value", "https://api.example.com/data")
#         # Realiza una solicitud GET al endpoint con la clave de la API proporcionada.
#     """
#     try:
#         headers = {
#             'Accept': "application/json",
#             'Content-Type': "application/json",
#             Api_key : value_api_key
#         }

#         response = requests.get(url, headers=headers)

#         if response.status_code == 200:
#             return response.json()
#         else:
#             error_message = f"Código Error: {response.status_code}. Detalle: {response.text}"
#             guardar_error(error_message)
#             # return error_message
#     except requests.exceptions.RequestException as e:
#         error_message = f"Error de conexión: {str(e)}"
#         guardar_error(error_message)
    
#     except ValueError as e:
#         error_message = f"Error de decodificación JSON: {str(e)}"
#         guardar_error(error_message)

#     except Exception as e:
#         error_message = f"Error desconocido: {str(e)}"
#         guardar_error(error_message)

#     return None  # Indica que no se pudo obtener datos


# def inicializaEndpoint_post(Api_key: str, value_api_key: str, url: str, datos: dict):
#     """
#     Realiza una solicitud POST a un endpoint especificado con la clave de la API proporcionada y los datos proporcionados.

#     Args:
#         Api_key (str): Nombre del encabezado de la API key.
#         value_api_key (str): Valor de la API key.
#         url (str): URL del endpoint al que se realizará la solicitud POST.
#         datos (dict): Datos que se enviarán en la solicitud POST.

#     Returns:
#         int or None: Si la solicitud es exitosa (status_code 201), devuelve 1. 
#         En caso de error, devuelve None y registra el error en el archivo de registro.

#     Examples:
#         >>> inicializaEndpoint_post("Authorization", "api_key_value", "https://api.example.com/resource", {"key": "value"})
#         # Realiza una solicitud POST al endpoint con la clave de la API proporcionada y los datos especificados.
#     """
#     try:
#         headers = {
#             'Accept': "application/json",
#             'Content-Type': "application/json",
#             Api_key : value_api_key
#         }

#         response = requests.post(url, json=datos, headers=headers)

#         if response.status_code == 200: # 200 indica que se creó el recurso exitosamente
#             return 200
#         else:
#             error_message = f"Código Error: {response.status_code}. Detalle: {response.text}"
#             guardar_error(error_message)
#             # return error_message
#     except requests.exceptions.RequestException as e:
#         error_message = f"Error de conexión: {str(e)}"
#         guardar_error(error_message)
    
#     except ValueError as e:
#         error_message = f"Error de decodificación JSON: {str(e)}"
#         guardar_error(error_message)

#     except Exception as e:
#         error_message = f"Error desconocido: {str(e)}"
#         guardar_error(error_message)

#     return None  # Indica que no se pudo insertar datos


# def inicializaEndpoint_put(Api_key: str, value_api_key: str, url: str, datos: dict):
#     """
#     Realiza una solicitud PUT a un endpoint especificado con la clave de la API proporcionada y los datos proporcionados.

#     Args:
#         Api_key (str): Nombre del encabezado de la API key.
#         value_api_key (str): Valor de la API key.
#         url (str): URL del endpoint al que se realizará la solicitud PUT.
#         datos (dict): Datos que se enviarán en la solicitud PUT.

#     Returns:
#         int or None: Si la solicitud es exitosa (status_code 200), devuelve 1. 
#         En caso de error, devuelve None y registra el error en el archivo de registro.

#     Examples:
#         >>> inicializaEndpoint_put("Authorization", "api_key_value", "https://api.example.com/resource", {"key": "value"})
#         # Realiza una solicitud PUT al endpoint con la clave de la API proporcionada y los datos especificados.
#     """
#     try:
#         headers = {
#             'Accept': "application/json",
#             'Content-Type': "application/json",
#             Api_key : value_api_key
#         }

#         response = requests.put(url, json=datos, headers=headers)

#         if response.status_code == 200: # 200 indica que la actualización se realizó correctamente
#             return 200
#         else:
#             error_message = f"Código Error: {response.status_code}. Detalle: {response.text}. Cedula Usuario: {datos['identification_number']} ID Intrena: {url}"
#             guardar_error(error_message)
#             return response.status_code
#     except requests.exceptions.RequestException as e:
#         error_message = f"Error de conexión: {str(e)}"
#         guardar_error(error_message)
    
#     except ValueError as e:
#         error_message = f"Error de decodificación JSON: {str(e)}"
#         guardar_error(error_message)

#     except Exception as e:
#         error_message = f"Error desconocido: {str(e)}"
#         guardar_error(error_message)

#     return None  # Indica que no se pudo actualizar datos


# def inicializaEndpoint_disable_bulk_put(ids_usuarios: str):
#     """
#     Realiza una solicitud PUT para desactivar múltiples usuarios a través de un endpoint específico.

#     Args:
#         ids_usuarios (str): IDs de los usuarios que se desactivarán, separados por comas.

#     Returns:
#         int or None: Si la solicitud es exitosa (status_code 200), devuelve 1. 
#         En caso de error, devuelve None y registra el error en el archivo de registro.

#     Examples:
#         >>> inicializaEndpoint_disable_bulk_put("[1,2,3,4]")
#         # Desactiva los usuarios con IDs 1, 2, 3 y 4.
#     """
#     try:
#         headers = {
#             'Accept': "application/json",
#             'Content-Type': "application/json",
#             dot_env.API_KEY : dot_env.VALUE_API_KEY
#         }

#         response = requests.put(f'{dot_env.BASE_URL}users/disable/{ids_usuarios}', headers=headers)

#         if response.status_code == 200: # 200 indica que la actualización se realizó correctamente
#             return 1
#         else:
#             error_message = f"Código Error: {response.status_code}. Detalle: {response.text}"
#             guardar_error(error_message)
#             # return error_message
#     except requests.exceptions.RequestException as e:
#         error_message = f"Error de conexión: {str(e)}"
#         guardar_error(error_message)
    
#     except ValueError as e:
#         error_message = f"Error de decodificación JSON: {str(e)}"
#         guardar_error(error_message)

#     except Exception as e:
#         error_message = f"Error desconocido: {str(e)}"
#         guardar_error(error_message)

#     return None  # Indica que no se pudo actualizar datos


# def inicializaEndpoint_enable_bulk_put(ids_usuarios: str):
#     """
#     Realiza una solicitud PUT para activar múltiples usuarios a través de un endpoint específico.

#     Args:
#         ids_usuarios (str): IDs de los usuarios que se activaran, separados por comas.

#     Returns:
#         int or None: Si la solicitud es exitosa (status_code 200), devuelve 1. 
#         En caso de error, devuelve None y registra el error en el archivo de registro.

#     Examples:
#         >>> inicializaEndpoint_enable_bulk_put("[1,2,3,4]")
#         # Activa los usuarios con IDs 1, 2, 3 y 4.
#     """
#     try:
#         headers = {
#             'Accept': "application/json",
#             'Content-Type': "application/json",
#             dot_env.API_KEY : dot_env.VALUE_API_KEY
#         }

#         response = requests.put(f'{dot_env.BASE_URL}users/enable/{ids_usuarios}', headers=headers)

#         if response.status_code == 200: # 200 indica que la actualización se realizó correctamente
#             return 1
#         else:
#             error_message = f"Código Error: {response.status_code}. Detalle: {response.text}"
#             guardar_error(error_message)
#             # return error_message
#     except requests.exceptions.RequestException as e:
#         error_message = f"Error de conexión: {str(e)}"
#         guardar_error(error_message)
    
#     except ValueError as e:
#         error_message = f"Error de decodificación JSON: {str(e)}"
#         guardar_error(error_message)

#     except Exception as e:
#         error_message = f"Error desconocido: {str(e)}"
#         guardar_error(error_message)

#     return None  # Indica que no se pudo actualizar datos


# def obtener_total_registros_consultados(endpoint: str, cantidad_registros_por_pagina: int):
#     """
#     Obtiene el total de registros consultados para un endpoint especificado.

#     Args:
#         endpoint (str): El endpoint al que se realizará la consulta.
#         cantidad_registros_por_pagina (int): La cantidad de registros por página para la consulta.

#     Returns:
#         tuple: Una tupla que contiene el total de registros consultados, el endpoint completo utilizado en la consulta 
#         y la cantidad de registros por página.

#     Examples:
#         >>> obtener_total_registros_consultados("users", 10)
#         # Realiza una consulta al endpoint 'users' con 10 registros por página y devuelve el total de registros consultados,
#         # el endpoint completo utilizado en la consulta y la cantidad de registros por página.
#     """
#     endpoint_full = f'{dot_env.BASE_URL}{endpoint}'
#     endPoint_con_parametros = f'{endpoint_full}?limit=1&skip=0'
#     json_datos = inicializaEndpoint_get(dot_env.API_KEY, dot_env.VALUE_API_KEY, endPoint_con_parametros)
    
#     return json_datos['pagination']['total'], endpoint_full, cantidad_registros_por_pagina


# def consultar_informacion_intrena(endpoint: str, cantidad_registros_por_pagina: int):
#     """
#     Consulta información de Intrena utilizando un endpoint y una cantidad de registros por página especificados.

#     Args:
#         endpoint (str): El endpoint al que se realizará la consulta.
#         cantidad_registros_por_pagina (int): La cantidad de registros por página para la consulta.

#     Returns:
#         pandas.DataFrame: Un DataFrame que contiene la información consultada.

#     Examples:
#         >>> consultar_informacion_interna("users", 10)
#         # Realiza una consulta al endpoint 'users' con 10 registros por página y devuelve la información en un DataFrame.
#     """
#     total_registros_consultados, endpoint, registro_por_pagina = obtener_total_registros_consultados(endpoint, cantidad_registros_por_pagina)
#     lista_informacion = obtener_todos_los_datos(inicializaEndpoint_get, dot_env.API_KEY, dot_env.VALUE_API_KEY, endpoint, registro_por_pagina, total_registros_consultados)
    
#     return pd.DataFrame(lista_informacion)


# def valida_y_crea_areas_y_cargos_nuevos(informacion_edm: pd.DataFrame, informacion_intrena: pd.DataFrame, endpoint: str):
#     """
#     Valida y crea áreas y cargos nuevos en Intrena por medio del API.

#     Args:
#         informacion_edm (pd.DataFrame): DataFrame con la información proveniente de EDM.
#         informacion_intrena (pd.DataFrame): DataFrame con la información de Intrena.
#         endpoint (str): El endpoint al que se realizará la operación.

#     Returns:
#         None

#     Examples:
#         >>> valida_y_crea_areas_y_cargos_nuevos(informacion_edm_df, informacion_interna_df, "areas")
#         # Valida y crea áreas nuevas en el sistema de Intrena basado en la comparación entre la información de EDM
#         # y la información de Intrena actual.
#     """    
#     # Convertir las columnas a tipo de datos 'str'
#     informacion_edm = informacion_edm.astype({'code':'string'})
#     informacion_intrena = informacion_intrena.astype({'code':'string'})
    
#     # Encontrar los registros faltantes en intrena en comparación con la información de edm
#     informacion_faltantes_intrena = informacion_edm.merge(informacion_intrena , on='code', how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
   
#     if not informacion_faltantes_intrena.empty:
#         mensaje_log_ejecucion_crea_con_datos = f'Se encontrarón {informacion_faltantes_intrena.shape[0]} nuevas {endpoint} para crear en Intrena.'
#         guardar_log_ejecucion(mensaje_log_ejecucion_crea_con_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_crea_con_datos}{bcolors.RESET}')
        
#         endpoint_full = f'{dot_env.BASE_URL}{endpoint}'
#         for index, row in informacion_faltantes_intrena.iterrows():
#             registro_nuevo = {"code": row["code"], "name": row["name_x"]}
            
#             inicializaEndpoint_post(dot_env.API_KEY, dot_env.VALUE_API_KEY, endpoint_full, registro_nuevo)
#     else:
#         mensaje_log_ejecucion_crea_sin_datos = f'No se contraron {endpoint} nuevas para crear en Intrena'
#         guardar_log_ejecucion(mensaje_log_ejecucion_crea_sin_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_crea_sin_datos}{bcolors.RESET}')


# def valida_y_crea_usuarios_en_intrena(usuarios_activos_edm: pd.DataFrame, usuarios_intrena: pd.DataFrame, areas_intrena: pd.DataFrame, positions_intrena: pd.DataFrame, endpoint: str):
    
#     cantidad_registros_con_errores = 0
    
#     usuarios_activos_edm = usuarios_activos_edm.astype({'identification_number':'string'})
#     usuarios_intrena = usuarios_intrena.astype({'identification_number':'string'})
    
#     # Realizar una combinación externa en base a la columna de identificación
#     merged_df = pd.merge(usuarios_activos_edm, usuarios_intrena, on='identification_number', how='outer', indicator=True, suffixes=('_edm', '_intrena'))
    
#     # Filtrar las filas donde la columna _merge es 'left_only' ya que esto son los registro que realmente NO existen en intrena y se deben crear
#     filtrar_registros_faltantes = merged_df[merged_df['_merge'] == 'left_only']
    
#     # Lista de columnas que deseas conservar
#     columnas_a_conservar = ['username_edm', 'email_edm', 'first_name_edm', 'last_name_edm', 'mobile_edm',
#                             'avatar_edm', 'area_code_edm', 'position_code_edm', 'disabled_edm',
#                             'role_edm',	'identification_number', 'location_edm', 'extended_field1_edm',
#                             'extended_field2_edm', 'extended_field3_edm', 'manager_identification',
#                             'manager_username', ]

#     # Seleccionar solo esas columnas
#     registros_faltantes = filtrar_registros_faltantes[columnas_a_conservar]
    
#     # Realizar una combinación con las áreas de Intrena para poder sacar el ID de la área.
#     registros_faltantes_con_area = pd.merge(registros_faltantes, areas_intrena, left_on='area_code_edm', right_on='code' ,how='inner', indicator=True, suffixes=('_edm', '_intrena'))
    
#     # Realizar una combinación con las positions(cargos) de Intrena para poder sacar el ID de la positions (cargo).
#     registros_faltantes_con_position = pd.merge(registros_faltantes_con_area, positions_intrena, left_on='position_code_edm', right_on='code' ,how='inner', indicator='merge_positions', suffixes=('_edm', '_intrena'))

    
#     # NOTE: HABILITAR para exportar la información de intrena a archivos de EXCEL
#     if dot_env.EXPORTAR_A_EXCEL == 'True':
#         registros_faltantes_con_position.to_excel('ArchivosExcel/crear_en_intrena_eliminar.xlsx', index=False)
    
#     if not registros_faltantes_con_position.empty:
#         for index, row in registros_faltantes_con_position.iterrows():
#             endpoint_full_users = f'{dot_env.BASE_URL}{endpoint}'
#             crea_usuario_intrena = {
#                 "username": row['username_edm'],
#                 "email": row['email_edm'],
#                 "first_name": row['first_name_edm'],
#                 "last_name": row['last_name_edm'],
#                 "mobile": row['mobile_edm'],
#                 "avatar": '',
#                 "area_id": row['id_edm'],
#                 "position_id": row['id_intrena'],
#                 "role": "user",
#                 "identification_number": row['identification_number'],
#                 "location": row['location_edm'],
#                 # "manager_id": '',
#                 "extended_field1": row['extended_field1_edm'],
#                 "extended_field2": row['extended_field2_edm'],
#                 "extended_field3": row['extended_field3_edm'],
#                 "extended_field4": '',
#                 "extended_field5": '',
#                 "extended_field6": '',
#                 "extended_field7": '',
#                 "organizational_units": []
#             }
            
#             codigo_retornado = inicializaEndpoint_post(dot_env.API_KEY, dot_env.VALUE_API_KEY, endpoint_full_users, crea_usuario_intrena)

#             if codigo_retornado != 200:
#                 cantidad_registros_con_errores += 1
            
#         mensaje_log_ejecucion_actualiza = f'Se crearon en Intrena {(registros_faltantes.shape[0]) - cantidad_registros_con_errores} usuarios.'
        
#         if cantidad_registros_con_errores > 0:
#             mensaje_log_ejecucion_con_errores = f'NO se crearon en intrena {cantidad_registros_con_errores} usuarios.'
#             guardar_log_ejecucion(mensaje_log_ejecucion_con_errores)
#             print(f'{bcolors.WARNING}{mensaje_log_ejecucion_con_errores}{bcolors.RESET}')
        
        
#         guardar_log_ejecucion(mensaje_log_ejecucion_actualiza)
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_actualiza}{bcolors.RESET}')
#     else:
#         mensaje_log_ejecucion_sin_datos = f'No se encontraron usuarios nuevos para realizar la creación del usuario en Intrena.'
#         guardar_log_ejecucion(mensaje_log_ejecucion_sin_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_sin_datos}{bcolors.RESET}')


# def valida_y_actualiza_usuarios_en_intrena(usuarios_edm: pd.DataFrame, usuarios_intrena: pd.DataFrame, areas_intrena: pd.DataFrame, positions_intrena: pd.DataFrame, endpoint: str):
#     """
#     Valida y actualiza usuarios en el sistema Intrena basándose en datos provenientes de la base de datos de EDM.

#     Args:
#         usuarios_edm (pd.DataFrame): DataFrame con información de usuarios proveniente de la base de datos de EDM.
#         usuarios_intrena (pd.DataFrame): DataFrame con información de usuarios del API de Intrena.
#         areas_intrena (pd.DataFrame): DataFrame con información de áreas del API de Intrena.
#         positions_intrena (pd.DataFrame): DataFrame con información de positions (cargos) del API de Intrena.
#         endpoint (str): El endpoint correspondiente a los usuarios del API de Intrena.

#     Returns:
#         None

#     Examples:
#         >>> valida_y_actualiza_usuarios_en_intrena(usuarios_edm_df, usuarios_intrena_df, areas_intrena_df, positions_intrena_df, 'users')
#         # Valida y actualiza los usuarios en el sistema Intrena utilizando la información de los DataFrames proporcionados.
#     """
#     cantidad_registros_con_errores = 0
    
#     # Convertir las columnas a tipo de datos 'str'
#     usuarios_edm = usuarios_edm.astype({'identification_number':'string'})
#     usuarios_intrena = usuarios_intrena.astype({'identification_number':'string'})

#     usuarios_edm_intrena = pd.merge(usuarios_edm, usuarios_intrena, on='identification_number', how='inner', suffixes=('_edm', '_intrena'))
    
#     usuarios_y_areas = pd.merge(usuarios_edm_intrena, areas_intrena, left_on='area_code_edm', right_on='code', suffixes=('_mergeUsers', '_areasIntrena'), how='inner')
    
#     usuarios_y_areas = usuarios_y_areas.astype({'position_code_edm':'string'})
#     positions_intrena = positions_intrena.astype({'code':'string'})
    
#     usuariosAreas_y_positions = pd.merge(usuarios_y_areas, positions_intrena, left_on='position_code_edm', right_on='code', suffixes=('_mergeUsersAreas', '_positionsIntrena'), how='inner')
    
#     # Para poder identificar y asignar o actualizar a los jefes de cada empleado, se crea una lista independiente y solamente con los usuarios activos.
#     identificar_id_manager = usuarios_intrena[['id', 'identification_number', 'first_name', 'last_name']]
    
#     usuariosAreas_y_positions = usuariosAreas_y_positions.astype({'manager_identification':'string'})
#     identificar_id_manager = identificar_id_manager.astype({'identification_number':'string'})
    
#     usuarioAreasPosition_y_manger_id = pd.merge(usuariosAreas_y_positions, identificar_id_manager,
#                                                 left_on='manager_identification',
#                                                 right_on='identification_number',
#                                                 suffixes=('_mergeUsersAreasPositions', '_mangerId'),
#                                                 how='inner'
#                                                 )
    
#     # Convertir a minúsculas los valores de las columnas especificadas, manejando valores nulos
#     columns_to_lowercase = [
#         'email_edm', 'email_intrena', 'mobile_edm', 'mobile_intrena',
#         'location_edm', 'location_intrena', 'extended_field1_edm', 'extended_field1_intrena'
#     ]

#     for column in columns_to_lowercase:
#         usuarioAreasPosition_y_manger_id[column] = usuarioAreasPosition_y_manger_id[column].fillna('').str.lower()

    
#     # Identificar los registros que necesitan ser actualizados
#     usuarioAreasPosition_y_manger_id['Requiere_Actualizacion'] = (
#         (usuarioAreasPosition_y_manger_id['email_edm'] != usuarioAreasPosition_y_manger_id['email_intrena']) |
#         (usuarioAreasPosition_y_manger_id['mobile_edm'] != usuarioAreasPosition_y_manger_id['mobile_intrena']) |
#         (usuarioAreasPosition_y_manger_id['area_id'] != usuarioAreasPosition_y_manger_id['id_areasIntrena']) |
#         (usuarioAreasPosition_y_manger_id['position_id'] != usuarioAreasPosition_y_manger_id['id_mergeUsersAreasPositions']) |
#         (usuarioAreasPosition_y_manger_id['location_edm'] != usuarioAreasPosition_y_manger_id['location_intrena']) |
#         (usuarioAreasPosition_y_manger_id['manager_id'] != usuarioAreasPosition_y_manger_id['id_mangerId']) |
#         (usuarioAreasPosition_y_manger_id['extended_field1_edm'] != usuarioAreasPosition_y_manger_id['extended_field1_intrena']) 
#     )
    
#     # Filtrar los registros que necesitan ser actualizados
#     registros_a_actualizar = usuarioAreasPosition_y_manger_id[usuarioAreasPosition_y_manger_id['Requiere_Actualizacion']]

#     # NOTE: HABILITAR para exportar la información a archivos de EXCEL
#     #region Exportar datos de cada Merge de DataFrames para poder comparar y validar columnas
#     if dot_env.EXPORTAR_A_EXCEL == 'True':
#         usuarios_intrena.to_excel('ArchivosExcel/usuarios_activos_intrena_eliminar.xlsx', index=False)
#         usuarios_edm_intrena.to_excel('ArchivosExcel/usuarios_edm_intrena_eliminar.xlsx', index=False)
#         usuarios_y_areas.to_excel('ArchivosExcel/usuariosMerge_areas_eliminar.xlsx', index=False)
#         usuariosAreas_y_positions.to_excel('ArchivosExcel/usuariosAreas_positions_eliminar.xlsx', index=False)
#         usuarioAreasPosition_y_manger_id.to_excel('ArchivosExcel/usuarioAreasPosition_y_manger_id_eliminar.xlsx', index=False)
    
#     # nuevo_orden_columnas = ['email_edm','email_intrena','mobile_edm','mobile_intrena','area_id','id_areasIntrena','position_id','id_mergeUsersAreasPositions','location_edm','location_intrena','manager_id','id_mangerId','extended_field1_edm','extended_field1_intrena','username_edm','first_name_edm','last_name_edm','avatar_edm','area_code_edm','position_code_edm','disabled_edm','role_edm','identification_number_mergeUsersAreasPositions','extended_field2_edm','extended_field3_edm','manager_identification','manager_username','username_intrena','created_at_mergeUsers','updated_at_mergeUsers','superadmin','uuid','id_mergeUsers','first_name_intrena','last_name_intrena','avatar_intrena','user_id','tenant_id_mergeUsers','disabled_intrena','role_intrena','formation_path_id_mergeUsersAreas','extended_field2_intrena','extended_field3_intrena','extended_field4','extended_field5','extended_field6','extended_field7','hide_from_ranking','tsv','manager_first_name','manager_last_name','tenant_uuid','area_code_intrena','area_name','position_code_intrena','position_name','full_name','code_mergeUsersAreas','name_mergeUsersAreas','tenant_id_areasIntrena','created_at_areasIntrena','updated_at_areasIntrena','code_positionsIntrena','name_positionsIntrena','formation_path_id_positionsIntrena','created_at','costs','formation_path','identification_number_mangerId','first_name','last_name','Requiere_Actualizacion']
#     # registros_a_actualizar = registros_a_actualizar[nuevo_orden_columnas]
#     if dot_env.EXPORTAR_A_EXCEL == 'True':
#         registros_a_actualizar.to_excel('ArchivosExcel/usuarios_para_actualizar_en_Intrena_eliminar.xlsx', index=False)
#     #endregion
    
#     if not registros_a_actualizar.empty:
#         for index, row in registros_a_actualizar.iterrows():
#             endpoint_full_users = f'{dot_env.BASE_URL}{endpoint}/{row['id_mergeUsers']}'
#             actualiza_usuario_intrena = {
#                 "email": row['email_edm'],
#                 "first_name": row['first_name_edm'],
#                 "last_name": row['last_name_edm'],
#                 "mobile": row['mobile_edm'],
#                 "avatar": row['avatar_intrena'] if row['avatar_intrena'] is not None and row['avatar_intrena'] != '' else None,
#                 "area_id": row['id_areasIntrena'],
#                 "position_id": row['id_mergeUsersAreasPositions'],
#                 "role": row['role_intrena'],
#                 "identification_number": row['identification_number_mergeUsersAreasPositions'],
#                 "location": row['location_edm'],
#                 "manager_id": row['id_mangerId'],
#                 "extended_field1": row['extended_field1_edm'],
#                 "extended_field2": row['extended_field2_intrena'],
#                 "extended_field3": row['extended_field3_intrena'],
#                 "extended_field4": row['extended_field4'],
#                 "extended_field5": row['extended_field5'],
#                 "extended_field6": row['extended_field6'],
#                 "extended_field7": row['extended_field7'],
#                 "organizational_units": []
#             }

#             codigo_retornado = inicializaEndpoint_put(dot_env.API_KEY, dot_env.VALUE_API_KEY, endpoint_full_users, actualiza_usuario_intrena)

#             if codigo_retornado != 200:
#                 cantidad_registros_con_errores += 1
            
#         mensaje_log_ejecucion_actualiza = f'Se actualizarón en Intrena {(registros_a_actualizar.shape[0]) - cantidad_registros_con_errores} usuarios.'
#         guardar_log_ejecucion(mensaje_log_ejecucion_actualiza)
        
#         if cantidad_registros_con_errores > 0:
#             mensaje_log_ejecucion_con_errores = f'NO se actualizaron en intrena {cantidad_registros_con_errores} usuarios.'
#             guardar_log_ejecucion(mensaje_log_ejecucion_con_errores)
#             print(f'{bcolors.WARNING}{mensaje_log_ejecucion_con_errores}{bcolors.RESET}')
        
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_actualiza}{bcolors.RESET}')
#     else:
#         mensaje_log_ejecucion_sin_datos = f'No se encontraron usuarios para realizar la actualización de datos en la función "valida_y_actualiza_usuarios_en_intrena"'
#         guardar_log_ejecucion(mensaje_log_ejecucion_sin_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_ejecucion_sin_datos}{bcolors.RESET}')


# def inactivar_usuarios_intrena(usuarios_activos_intrena: pd.DataFrame, usuarios_retirados_edm: pd.DataFrame):
#     """
#     Inactiva usuarios en Intrena basándose en los usuarios retirados de EDM.

#     Args:
#         usuarios_activos_intrena (pd.DataFrame): DataFrame con la información de usuarios activos en Intrena.
#         usuarios_retirados_edm (pd.DataFrame): DataFrame con la información de usuarios retirados de EDM.

#     Returns:
#         None

#     Examples:
#         >>> inactivar_usuarios_intrena(usuarios_activos_intrena_df, usuarios_retirados_edm_df)
#         # Inactiva los usuarios en Intrena que han sido retirados de EDM.
#     """
     
#     # Convertir las columnas a tipo de datos 'str'
#     usuarios_activos_intrena = usuarios_activos_intrena.astype({'identification_number':'string'})
#     usuarios_retirados_edm = usuarios_retirados_edm.astype({'numero_identificaion':'string'})
    
#     inner_usuarios_activosIntrena_RetiradoEdm = pd.merge(usuarios_activos_intrena, usuarios_retirados_edm,
#                                                         left_on='identification_number',
#                                                         right_on='numero_identificaion',
#                                                         suffixes=('_intrena','_edm'))
    
#     if not inner_usuarios_activosIntrena_RetiradoEdm.empty:
        
#         # NOTE: HABILITAR para exportar a Excel los usuarios que se deben Inactivar de Intrena ya que están retirados de EDM
#         if dot_env.EXPORTAR_A_EXCEL == 'True':
#             inner_usuarios_activosIntrena_RetiradoEdm.to_excel('ArchivosExcel/usuarios_a_inactivar_en_intrena.xlsx', index=False)
        
#         ids_inactivar_intrena: str = '[' + ','.join(inner_usuarios_activosIntrena_RetiradoEdm['id'].astype(str).unique()) + ']'

#         mensaje_log_cantidad_registro = f'Cantidad de usuarios a inactivar en Intrena: {len(eval(ids_inactivar_intrena))} usuarios'
#         guardar_log_ejecucion(mensaje_log_cantidad_registro)
#         print(f'{bcolors.WARNING}{mensaje_log_cantidad_registro}{bcolors.RESET}')
        
#         inicializaEndpoint_disable_bulk_put(ids_inactivar_intrena)
#     else:
#         mensaje_log_sin_datos = f'No se encontrarón usuarios para Inactivar en Intrena'
#         guardar_log_ejecucion(mensaje_log_sin_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_sin_datos}{bcolors.RESET}')


# def activar_usuarios_intrena(usuarios_inactivos_intrena: pd.DataFrame, usuarios_activos_edm: pd.DataFrame):
#     """
#     Activa usuarios en Intrena basándose en los usuarios Activos de EDM.

#     Args:
#         usuarios_inactivos_intrena (pd.DataFrame): DataFrame con la información de usuarios inactivos en Intrena.
#         usuarios_activos_edm (pd.DataFrame): DataFrame con la información de usuarios activos de EDM.

#     Returns:
#         None

#     Examples:
#         >>> activar_usuarios_intrena(usuarios_inactivos_intrena, usuarios_activos_edm)
#         # Activa los usuarios en Intrena que se encuentran Activos de EDM.
#     """

#     # Convertir las columnas a tipo de datos 'str'
#     usuarios_inactivos_intrena = usuarios_inactivos_intrena.astype({'identification_number':'string'})
#     usuarios_activos_edm = usuarios_activos_edm.astype({'identification_number':'string'})
    
#     inner_usuarios_inactivosIntrena_ActivosEdm = pd.merge(usuarios_inactivos_intrena, usuarios_activos_edm,
#                                                         left_on='identification_number',
#                                                         right_on='identification_number',
#                                                         suffixes=('_intrena','_edm'))
    
#     if not inner_usuarios_inactivosIntrena_ActivosEdm.empty:
        
#         # NOTE: HABILITAR para exportar a Excel los usuarios que se deben Activar de Intrena ya que están Activos en EDM
#         if dot_env.EXPORTAR_A_EXCEL == 'True':
#             inner_usuarios_inactivosIntrena_ActivosEdm.to_excel('ArchivosExcel/usuarios_a_activar_en_intrena.xlsx', index=False)
        
#         ids_activar_intrena: str = '[' + ','.join(inner_usuarios_inactivosIntrena_ActivosEdm['id'].astype(str).unique()) + ']'

#         mensaje_log_cantidad_registro = f'Cantidad de usuarios a Activar en Intrena: {len(eval(ids_activar_intrena))} usuarios'
#         guardar_log_ejecucion(mensaje_log_cantidad_registro)
#         print(f'{bcolors.WARNING}{mensaje_log_cantidad_registro}{bcolors.RESET}')
        
#         inicializaEndpoint_enable_bulk_put(ids_activar_intrena)
#     else:
#         mensaje_log_sin_datos = f'No se encontrarón usuarios para Activar en Intrena'
#         guardar_log_ejecucion(mensaje_log_sin_datos)
#         print(f'{bcolors.WARNING}{mensaje_log_sin_datos}{bcolors.RESET}')


# def imprimir_diccionario_ordenado(diccionario: dict):
#     # Convertir el objeto JSON a una cadena formateada
#     json_str = json.dumps(diccionario, indent=4)
#     print(json_str)

    
# def obtener_todos_los_datos(funcion_a_ejecutar: Callable, api_key: str, value_api_key: str, endpoint: str ,registros_por_pagina: int, total_registros_obtenidos: int) -> list:
#     """
#     Obtiene todos los datos de un endpoint de forma paginada.

#     Args:
#         funcion_a_ejecutar (Callable): La función que se ejecutará para obtener los datos del endpoint.
#         api_key (str): La clave de la API.
#         value_api_key (str): El valor de la clave de la API.
#         endpoint (str): El endpoint del que se obtendrán los datos.
#         registros_por_pagina (int): El número de registros por página.
#         total_registros_obtenidos (int): El número total de registros que se desea obtener.

#     Returns:
#         list: Una lista que contiene todos los registros obtenidos del endpoint.

#     Examples:
#         >>> obtener_todos_los_datos(inicializaEndpoint_get, 'API_KEY', 'VALUE_API_KEY', 'https://api.example.com/resource', 10, 100)
#         # Obtiene todos los datos del endpoint 'https://api.example.com/resource', con 10 registros por página y un total de 100 registros.
#     """
#     skip = 0
#     todos_los_registros = []
    
#     # Aqui va el while
#     while True:
#         endpoint_con_parametros = f'{endpoint}?limit={registros_por_pagina}&skip={skip}'
#         resultado_api = funcion_a_ejecutar(api_key, value_api_key, endpoint_con_parametros)
        
#         todos_los_registros.extend(resultado_api['results'])
        
        
#         if len(todos_los_registros) >= total_registros_obtenidos:
#             break # Salir del bucle si hemos obtenido todos los registros
        
#         skip += registros_por_pagina # Mover a la siguiente página de resultados
        
#         # if total_registros_obtenidos > skip:
#         #     print(f'{bcolors.WARNING}Cantidad de registros obtenidos en {endpoint.split("/")[-1]}: {len(todos_los_registros)} / {total_registros_obtenidos}')        
    
#     mensaje_log_ejecucion_objtener_datos = f'Cantidad de registros obtenidos en Intrena de {endpoint.split("/")[-1]}: {len(todos_los_registros)} / {total_registros_obtenidos}'
#     guardar_log_ejecucion(mensaje_log_ejecucion_objtener_datos)
#     print(f'{bcolors.WARNING} {mensaje_log_ejecucion_objtener_datos} {bcolors.RESET}')
    
#     return todos_los_registros

# # Función para enviar el correo electrónico utilizando el servidor SMTP de IIS
# def enviar_correo_electronico(asunto_correo: str, mensaje_correo: str, destinatarios_notificaciones: pd.DataFrame):
#     remitente = dot_env.CORREO_REMITENTE
#     destinatarios = destinatarios_notificaciones['Destinatarios'].iloc[0]
#     destinatarios_copia = destinatarios_notificaciones['DestinatariosCopia'].iloc[0]
#     destinatarios_copia_oculta = destinatarios_notificaciones['DestinatariosCopiaOculta'].iloc[0]
    
#     msg = MIMEMultipart()
#     msg['From'] = remitente
#     msg['To'] = destinatarios
#     if destinatarios_copia:
#         msg["Cc"] = destinatarios_copia
#     if destinatarios_copia_oculta:
#             msg['Bcc'] = destinatarios_copia_oculta
#     msg['Subject'] = asunto_correo
#     msg.attach(MIMEText(mensaje_correo, 'plain'))

#     # Construir la lista de todos los destinatarios
#     todos_los_destinatarios = msg["To"].split(";")
    
#     if destinatarios_copia:
#         todos_los_destinatarios += msg["Cc"].split(";")
    
#     if destinatarios_copia_oculta:
#         todos_los_destinatarios += msg["Bcc"].split(";")
    
#     try:
#         # Conectar al servidor SMTP de IIS en la dirección IP y puerto especificados
#         with smtplib.SMTP('10.10.20.4', 587) as server:
#             # server.sendmail(remitente, destinatarios, msg.as_string())
#             server.sendmail(remitente, todos_los_destinatarios, msg.as_string())
#         mensaje_log_ejecucion_correo = 'Correo enviado correctamente'
#         guardar_log_ejecucion(mensaje_log_ejecucion_correo)
#         print(f'{bcolors.OK}{mensaje_log_ejecucion_correo}{bcolors.RESET}')
#     except Exception as e:
#         mensaje_log_error_correo = f'Se produjo un error al enviar el correo: {e}'
#         guardar_error(mensaje_log_error_correo)
#         print(f'{bcolors.FAIL}{mensaje_log_error_correo}{bcolors.RESET}')
