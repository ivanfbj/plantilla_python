# Imports de Python
import os
import re
import sys
import datetime
import time
import json
from datetime import datetime

# Imports de terceros
import pandas as pd
from tqdm import tqdm
from sqlalchemy import Engine, text
from sqlalchemy.orm import sessionmaker
import pyodbc


# Imports propios
from utils.bcolors import bcolors
import utils.utilidades as utilidades

CARPETA_ERRORES = 'ErroresConsultas'

def consultar_registros_en_BDD(conexion_sql_server: pyodbc.Connection, parametro: str) -> pd.DataFrame:
    """
    Obtiene todos los datos del procedimiento almacenado [dbo].[stpr_NombreDelProcedimientosAlmacenado] dependiendo del párametro que se le asigne.

    Args:
        conexion_sql_server (pyodbc.Connection): conexión a la base de datos
        parametro (str): valor que se le asignará al procedimiento almacenado, puede ser: cargos, areas, usuariosActivos o usuariosRetirados

    Returns:
        DataFrame: Contiene la información devuelta por el procedimiento almacenado desde la base de datos.
    """
    try:
        # Llamar al procedimiento almacenado y obtener el resultado
        cursor = conexion_sql_server.cursor()
        cursor.execute("EXEC [dbo].[stpr_NombreDelProcedimientoAlmacenado] ?", (parametro,))
        
        try:
            # Obtener los resultados y cargarlos en un DataFrame
            resultados = [tuple(row) for row in cursor.fetchall()]  # Desempaquetar las tuplas internas
            columnas = [column[0] for column in cursor.description]

            df = pd.DataFrame(resultados, columns=columnas)

            # Cerrar el cursor
            cursor.close()
            
            mensaje_log_ejecucion_bdd = f'Cantidad de registros obtenidos en {parametro} de la BDD de EDM: {df.shape[0]}'
            utilidades.guardar_log_ejecucion(mensaje_log_ejecucion_bdd)
            print(f'{bcolors.WARNING}{mensaje_log_ejecucion_bdd}{bcolors.RESET}')
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'El procedimiento almacenado no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            utilidades.guardar_error(mensaje_error_pyodbc, CARPETA_ERRORES)
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    
    except pyodbc.Error as e:
        # Manejar error de pyobc
        mensaje_error_pyodbc = f'Error al ejecutar el procedimiento almacenado: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        utilidades.guardar_error(mensaje_error_pyodbc, CARPETA_ERRORES)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        utilidades.guardar_error(mensaje_error_otro, CARPETA_ERRORES)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    
def consultar_correos_notificaciones_en_BDD(conexion_sql_server: pyodbc.Connection) -> pd.DataFrame:
    try:
        # Llamar al procedimiento almacenado y obtener el resultado
        cursor = conexion_sql_server.cursor()
        
        # Consulta SQL
        consulta = "SELECT Destinatarios, DestinatariosCopia, DestinatariosCopiaOculta, NombreOrigenNotificacion FROM CorreosNotificaciones WHERE NombreOrigenNotificacion='API_Intrena'"
        cursor.execute(consulta)
        
        try:
            # Obtener los resultados y cargarlos en un DataFrame
            resultados = [tuple(row) for row in cursor.fetchall()]  # Desempaquetar las tuplas internas
            columnas = [column[0] for column in cursor.description]

            df = pd.DataFrame(resultados, columns=columnas)

            # Cerrar el cursor
            cursor.close()
            
            mensaje_log_ejecucion_bdd = f'Cantidad de registros obtenidos en correos_notificaciones de la BDD de EDM: {df.shape[0]}'
            utilidades.guardar_log_ejecucion(mensaje_log_ejecucion_bdd)
            print(f'{bcolors.WARNING}{mensaje_log_ejecucion_bdd}{bcolors.RESET}')
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'La consulta no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            utilidades.guardar_error(mensaje_error_pyodbc, CARPETA_ERRORES)
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    
    except pyodbc.Error as e:
        # Manejar error de pyobc
        mensaje_error_pyodbc = f'Error al ejecutar la consulta de correos_notificaciones: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        utilidades.guardar_error(mensaje_error_pyodbc, CARPETA_ERRORES)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        utilidades.guardar_error(mensaje_error_otro, CARPETA_ERRORES)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de erro

def ejecutar_sp_consulta(engine: Engine, nombre_sp: str):
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Construir la parte de la llamada al procedimiento almacenado con los parámetros y valores
        llamada_sp = f"EXEC {nombre_sp}"

        # # Ejecutar el procedimiento almacenado y cargar los resultados en un DataFrame
        resultados = pd.read_sql_query(llamada_sp, engine)
        
        # logger_info.info(f'\t{len(resultados)} registros recuperados del procedimiento almacenado {nombre_sp}')
        
        return resultados
    except Exception as error:
        mensaje_error = f'Error al ejecutar el procedimiento almacenado "{nombre_sp}": {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        return None
    finally:
        session.close()
        print('Conexión finalizada a la base de datos')