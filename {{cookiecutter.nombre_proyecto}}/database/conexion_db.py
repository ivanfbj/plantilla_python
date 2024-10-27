# Imports de terceros
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import OperationalError, InterfaceError, SQLAlchemyError
import pyodbc
from decouple import Config, UndefinedValueError, RepositoryEnv

# Imports propios
from utils.bcolors import bcolors
import utils.utilidades as utilidades
from utils.logger import logger_info, logger_debug, logger_error

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
        logger_error.error(mensaje_error)
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
        logger_error.error(mensaje_error)
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
        # print(f"{bcolors.OK}{mensaje_log_conexion_bdd}{bcolors.RESET}")
        logger_info.info(mensaje_log_conexion_bdd)
    except pyodbc.Error as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)

def conectar_bd_sqlalchemy(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str = None):
    try:
        engine = _crear_conexion_sqlalchemy(usuario, contrasena, servidor, base_datos, instancia)
        if engine:
            _verificar_conexion_sqlalchemy(engine)
            return engine
        else:
            return None
    except OperationalError as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
        return None

def _crear_conexion_sqlalchemy(usuario: str, contrasena: str, servidor: str, base_datos: str, instancia: str):
    if instancia:
        return create_engine(f"mssql+pyodbc://{usuario}:{contrasena}@{servidor}\\{instancia}/{base_datos}?driver=ODBC+Driver+17+for+SQL+Server")
    else:
        return create_engine(f"mssql+pyodbc://{usuario}:{contrasena}@{servidor}/{base_datos}?driver=ODBC+Driver+17+for+SQL+Server")


def _verificar_conexion_sqlalchemy(engine: Engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
            # print(f"{bcolors.OK}Conexión exitosa a la base de datos.{bcolors.RESET}")
            logger_info.info('Conexión exitosa a la base de datos')            
            
    except (OperationalError, InterfaceError) as error:
        mensaje_error = f'Error al conectar a la base de datos: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
        raise