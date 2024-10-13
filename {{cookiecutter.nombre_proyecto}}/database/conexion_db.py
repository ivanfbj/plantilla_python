# Imports de terceros
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import OperationalError, InterfaceError
import pyodbc

# Imports propios
from utils.bcolors import bcolors
import utils.utilidades as utilidades

CARPETA_ERRORES = 'ErroresApp'

def conectar_bd(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str = None):
    """
    Conecta a una base de datos SQL Server.

    Args:
        usuario (str): Nombre de usuario para la conexión.
        contrasena (str): Contraseña para la conexión.
        servidor (str): Dirección del servidor de la base de datos.
        base_datos (str): Nombre de la base de datos.
        instancia (str, opcional): Nombre de la instancia de la base de datos (si aplica).

    Returns:
        pyodbc.Connection or None: Objeto de conexión a la base de datos. Devuelve None si la conexión falla.

    """
    try:
        conexion_sql_server = _crear_conexion(usuario, contrasena, servidor, base_datos, instancia)
        if conexion_sql_server:
            _verificar_conexion(conexion_sql_server)
            return conexion_sql_server
        else:
            return None
    except OperationalError as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        utilidades.guardar_error(mensaje_error, CARPETA_ERRORES)
        return None

def _crear_conexion(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str):
    """
    Crea una conexión a la base de datos SQL Server.

    Args:
        usuario (str): Nombre de usuario para la conexión.
        contrasena (str): Contraseña para la conexión.
        servidor (str): Dirección del servidor de la base de datos.
        base_datos (str): Nombre de la base de datos.
        instancia (str): Nombre de la instancia de la base de datos (si aplica).

    Returns:
        pyodbc.Connection or None: Objeto de conexión a la base de datos. Devuelve None si la conexión falla.

    """
    if instancia:
        nombre_driver = '{SQL Server}'
        conn_str = f"DRIVER={nombre_driver};SERVER={servidor}\\{instancia};DATABASE={base_datos};UID={usuario};PWD={contrasena};Connect Timeout=30"
    else:
        conn_str = f"DRIVER={nombre_driver};SERVER={servidor};DATABASE={base_datos};UID={usuario};PWD={contrasena};Connect Timeout=30"

    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        utilidades.guardar_error(mensaje_error, CARPETA_ERRORES)
        return None

def _verificar_conexion(connection):
    """
    Verifica la conexión a la base de datos.

    Args:
        connection: Objeto de conexión a la base de datos.

    Returns:
        None

    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        mensaje_log_conexion_bdd = f'Conexión exitosa a la base de datos.'
        print(f"{bcolors.OK}{mensaje_log_conexion_bdd}{bcolors.RESET}")
        utilidades.guardar_log_ejecucion(mensaje_log_conexion_bdd)
    except pyodbc.Error as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        utilidades.guardar_error(mensaje_error, CARPETA_ERRORES)
