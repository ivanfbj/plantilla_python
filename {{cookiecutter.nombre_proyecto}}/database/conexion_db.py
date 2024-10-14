# Imports de terceros
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import OperationalError, InterfaceError, SQLAlchemyError
import pyodbc
from decouple import Config, UndefinedValueError, RepositoryEnv

# Imports propios
from utils.bcolors import bcolors
import utils.utilidades as utilidades
from utils.logger import logger_info, logger_debug, logger_error

CARPETA_ERRORES = 'ErroresApp'

def conectar_bd_pyodbc(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str = None):
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
        conexion_sql_server = _crear_conexion_pyodbc(usuario, contrasena, servidor, base_datos, instancia)
        if conexion_sql_server:
            _verificar_conexion_pyodbc(conexion_sql_server)
            return conexion_sql_server
        else:
            return None
    except OperationalError as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        utilidades.guardar_error(mensaje_error, CARPETA_ERRORES)
        return None

def _crear_conexion_pyodbc(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str):
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

def _verificar_conexion_pyodbc(connection):
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

def conectar_bd_sqlalchemy():
    """
    Crea un motor de conexión a la base de datos utilizando SQLAlchemy y maneja errores si ocurren.
    
    :return: Motor de conexión a la base de datos si la conexión es exitosa, None en caso de error.
    """
    try:
        config = Config(RepositoryEnv(utilidades.ruta_recurso('.env')))
        # Intentar cargar las variables de entorno
        usuario = config('USUARIO_DB')
        contrasena = config('CONTRASENA_DB')
        servidor = config('SERVIDOR_DB')
        instancia = config('INSTANCIA_DB')
        nombre_db = config('NOMBRE_DB')

        # Crear la cadena de conexión para el motor de la base de datos
        cadena_conexion = f"mssql+pyodbc://{usuario}:{contrasena}@{servidor}\\{instancia}/{nombre_db}?driver=ODBC+Driver+17+for+SQL+Server"

        # Intentar crear el motor de conexión
        engine = create_engine(cadena_conexion, echo=False)
        return engine

    except UndefinedValueError as e:
        # Capturar errores relacionados con variables de entorno no definidas
        logger_error.error(f"Error: Una o más variables de entorno no están definidas: {e}")
        return None

    except SQLAlchemyError as e:
        # Capturar errores relacionados con la conexión a la base de datos o SQLAlchemy
        logger_error.error(f"Error al crear el motor de la base de datos: {e}")
        return None

    except Exception as e:
        # Capturar cualquier otro error inesperado
        logger_error.error(f"Ocurrió un error inesperado al crear el motor de la base de datos: {e}")
        return None