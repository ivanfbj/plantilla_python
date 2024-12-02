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
from sqlalchemy import Engine, text, create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc


# Imports propios
from utils.bcolors import bcolors
import utils.utilidades as utilidades
from utils.logger import logger_info, logger_debug, logger_error

# ******ESTAS FUNCIONES SE UTILIZARÁN CUANDO LA CONEXIÓN A BASE DE DATOS SE REALICE POR MEDIO DE PYODBC*********

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
            logger_info.info(mensaje_log_ejecucion_bdd)
            print(f'{bcolors.WARNING}{mensaje_log_ejecucion_bdd}{bcolors.RESET}')
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'El procedimiento almacenado no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            logger_error.error(mensaje_error_pyodbc)
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    
    except pyodbc.Error as e:
        # Manejar error de pyobc
        mensaje_error_pyodbc = f'Error al ejecutar el procedimiento almacenado: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        logger_error.error(mensaje_error_pyodbc)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        logger_error.error(mensaje_error_otro)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

def ejecutar_sp_consulta_sin_parametros_pyodbc(conexion_sql_server: pyodbc.Connection, nombre_procedimiento_almacenado: str) -> pd.DataFrame:
    
    try:
        # Llamar al procedimiento almacenado y obtener el resultado
        cursor = conexion_sql_server.cursor()
        cursor.execute(f"EXEC {nombre_procedimiento_almacenado}")
        
        # Verificar si hay resultados antes de llamar a fetchall()
        if cursor.description is None:
            mensaje_error_pyodbc = 'El procedimiento almacenado no retornó ningún conjunto de resultados.'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            logger_error.error(mensaje_error_pyodbc)
        
        try:
            # Obtener los resultados y cargarlos en un DataFrame
            resultados = [tuple(row) for row in cursor.fetchall()]  # Desempaquetar las tuplas internas
            columnas = [column[0] for column in cursor.description]

            df = pd.DataFrame(resultados, columns=columnas)

            # Cerrar el cursor
            cursor.close()
            
            mensaje_log_ejecucion_bdd = f'Cantidad de registros obtenidos de la BDD de EDM: {df.shape[0]}'
            logger_info.info(mensaje_log_ejecucion_bdd)
            print(f'{bcolors.WARNING}{mensaje_log_ejecucion_bdd}{bcolors.RESET}')
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'El procedimiento almacenado no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            logger_error.error(mensaje_error_pyodbc)
            return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    
    except pyodbc.Error as e:
        # Manejar error de pyodbc
        mensaje_error_pyodbc = f'Error al ejecutar el procedimiento almacenado: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        logger_error.error(mensaje_error_pyodbc)
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        logger_error.error(mensaje_error_otro)
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
            logger_info.info(mensaje_log_ejecucion_bdd)
            print(f'{bcolors.WARNING}{mensaje_log_ejecucion_bdd}{bcolors.RESET}')
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'La consulta no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            logger_error.error(mensaje_error_pyodbc)
            raise
    
    except pyodbc.Error as e:
        # Manejar error de pyobc
        mensaje_error_pyodbc = f'Error al ejecutar la consulta de correos_notificaciones: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        logger_error.error(mensaje_error_pyodbc)
        raise
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        logger_error.error(mensaje_error_otro)
        raise

def ejecutar_consulta_pyodbc(conexion_sql_server: pyodbc.Connection, consulta: str) -> pd.DataFrame:
    try:
        # Llamar al procedimiento almacenado y obtener el resultado
        cursor = conexion_sql_server.cursor()

        # Consulta SQL
        cursor.execute(consulta)
        
        try:
            # Obtener los resultados y cargarlos en un DataFrame
            resultados = [tuple(row) for row in cursor.fetchall()]  # Desempaquetar las tuplas internas
            columnas = [column[0] for column in cursor.description]

            df = pd.DataFrame(resultados, columns=columnas)

            # Cerrar el cursor
            cursor.close()
            
            return df
        except pyodbc.ProgrammingError as e:
            # Si no hay resultados, retornar DataFrame vacío
            mensaje_error_pyodbc = f'La consulta no retornó ningún resultado: {e}'
            print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
            logger_error.error(mensaje_error_pyodbc)
            raise
    
    except pyodbc.Error as e:
        # Manejar error de pyobc
        mensaje_error_pyodbc = f'Error al ejecutar la consulta de correos_notificaciones: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_pyodbc}{bcolors.RESET}')
        logger_error.error(mensaje_error_pyodbc)
        raise
    except Exception as e:
        # Manejar otros tipos de errores
        mensaje_error_otro = f'Ocurrió un error: {e}'
        print(f'{bcolors.FAIL}{mensaje_error_otro}{bcolors.RESET}')
        logger_error.error(mensaje_error_otro)
        raise

# ******ESTAS FUNCIONES SE UTILIZARÁN CUANDO LA CONEXIÓN A BASE DE DATOS SE REALICE POR MEDIO DE SQL ALCHEMY*********
def ejecutar_sp_consulta_sin_parametros(engine: Engine, nombre_sp: str):
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
        logger_info.info('Conexión finalizada a la base de datos')

def ejecutar_consulta(engine: Engine, consulta: str):
    """
    Ejecuta una consulta SQL en una base de datos utilizando el motor proporcionado.

    Args:
        engine (sqlalchemy.engine.Engine): Motor de SQLAlchemy para la conexión a la base de datos.
        consulta (str): Consulta SQL a ejecutar.

    Returns:
        pandas.DataFrame or None: DataFrame de pandas que contiene los resultados de la consulta si la ejecución es exitosa, None si hay un error.

    """
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        # resultados = session.execute(text(consulta)).fetchall()
        resultados = pd.read_sql_query(text(consulta), engine)
        return resultados
    except Exception as error:
        mensaje_error = f'Error al ejecutar la consulta: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
        return None
    finally:
        session.close()
        logger_info.info(f'{bcolors.WARNING}Conexión finalizada a la base de datos{bcolors.RESET}')


def ejecutar_sp_consulta_con_parametros(engine: Engine, nombre_sp: str, parametros: dict):
    """
    Ejecuta un procedimiento almacenado en una base de datos utilizando el motor proporcionado y los parámetros especificados.

    Args:
        engine (sqlalchemy.engine.Engine): Motor de SQLAlchemy para la conexión a la base de datos.
        nombre_sp (str): Nombre del procedimiento almacenado a ejecutar.
        parametros (dict): Un diccionario que contiene los nombres de los parámetros y sus valores correspondientes.

    Returns:
        pandas.DataFrame or None: DataFrame de pandas que contiene los resultados de la ejecución del procedimiento almacenado si es exitosa, None si hay un error.

    """
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        # Construir la parte de la llamada al procedimiento almacenado con los parámetros y valores
        llamada_sp = "EXEC " + nombre_sp + " "
        # parametros_str = ", ".join([f"@{param}='{valor}'" for param, valor in parametros.items()])
        parametros_str = ", ".join([f"@{param}={valor if valor is not None else 'NULL'}" for param, valor in parametros.items()])
        # Para que permita valores Nulos
        
        
        llamada_sp += parametros_str

        # Ejecutar el procedimiento almacenado y cargar los resultados en un DataFrame
        resultados = pd.read_sql_query(llamada_sp, engine)
        
        print(f'\t{len(resultados)} registros recuperados del procedimiento almacenado {nombre_sp}')
        
        return resultados
    except Exception as error:
        mensaje_error = f'Error al ejecutar el procedimiento almacenado "{nombre_sp}": {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
        return None
    finally:
        session.close()
        logger_info.info('Conexión finalizada a la base de datos')


def ejecutar_sp_eliminar_duplicados(engine: Engine):
    """
    Ejecuta un procedimiento almacenado para eliminar registros duplicados en una tabla específica.

    Args:
        engine (sqlalchemy.engine.Engine): Motor de SQLAlchemy para la conexión a la base de datos.

    """
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        # Llamada al procedimiento almacenado
        llamada_sp = "EXEC stpr_EliminarDuplicadosLogErroresFiltrado"

        # Ejecutar el procedimiento almacenado
        with engine.begin() as conn:
            result = conn.execute(text(llamada_sp))
            # Obtener la cantidad de filas afectadas
            rows_affected = result.rowcount
            print(f'{bcolors.WARNING}\t{rows_affected} registros duplicados han sido eliminados.{bcolors.RESET}')
            result.close()

    except Exception as error:
        mensaje_error = f'Error al ejecutar el procedimiento almacenado {llamada_sp}: Error: {error}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
    finally:
        session.close()
        logger_info.info('Conexión finalizada a la base de datos')


def prueba_insertar_datos_con_parametros_con_valores_tabla(USUARIO_DB: str, CONTRASENA_DB: str, SERVIDOR_DB: str, INSTANCIA_DB: str, NOMBRE_DB: str):
    """
    Prueba puntual para poder pasarle una variable tipo tabla a un procedimiento almacenado de SQL Server.
    
    Esta fue una prueba puntual para realizar lo siguiente:
    1. Crear la conexión a la base de datos
    2. Si existe el procedimiento almacenado y tipo de tabla definido por el usuario entonces borrarlos borrarlo.
    3. Crear el tipo de tabla definido por el usuario (SSMS -> Servidor -> Base de datos -> Programmability -> Types -> User-Defined Table Types)
    4. Crear el procedimiento almacenado
    5. Se define un diccionario llamado data, que contiene una lista de tuplas con los datos a pasar al procedimiento almacenado.
    6. Se construye la consulta SQL utilizando el nombre del procedimiento almacenado y el parámetro de la tabla de valor.
    7. Se ejecuta la consulta SQL utilizando la conexión conn, pasando los datos a través del parámetro data.
    8. Luego, se obtienen todos los resultados devueltos por la ejecución de la consulta y se imprimen.
    
    
    Luego de los pasos anteriores, se repite el ejercicio pero con el procedimiento almacenado y los datos que
    realmente irian a la base de datos para luego replicar a escala mayor en el código.
    
    La información de como ejecutar un procedimiento almacenado con Parametros con valores de tabla desde python,
    se tomó del siguiente enlace:
    
    Python call sql-server stored procedure with table valued parameter
    https://copyprogramming.com/howto/execute-stored-procedure-with-table-valued-parameters-in-sql#how-to-use-table-valued-parameter-in-stored-procedure
    """
    # Crear la cadena de conexión
    cadena_conexion = f"mssql+pyodbc://{USUARIO_DB}:{CONTRASENA_DB}@{SERVIDOR_DB}\\{INSTANCIA_DB}/{NOMBRE_DB}?driver=ODBC+Driver+17+for+SQL+Server"

    # Definir la conexión al motor de la base de datos
    engine = create_engine(cadena_conexion, echo=True)
    
    proc_name = "so51930062"
    type_name = proc_name + "Type"
    # set up test environment
    with engine.begin() as conn:
        conn.exec_driver_sql(f"""\
            DROP PROCEDURE IF EXISTS {proc_name} 
        """)
        conn.exec_driver_sql(f"""\
            DROP TYPE IF EXISTS {type_name} 
        """)
        conn.exec_driver_sql(f"""\
            CREATE TYPE {type_name} AS TABLE (
            id int,
            txt nvarchar(50)
            ) 
        """)
        conn.exec_driver_sql(f"""\
            CREATE PROCEDURE {proc_name} 
            @tvp {type_name} READONLY
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT id, txt AS new_txt FROM @tvp;
            END
        """)
    #run test
    with engine.begin() as conn:
        
        # Se define un diccionario llamado data, que contiene una lista de tuplas con los datos a pasar al procedimiento almacenado.
        data = {"tvp": [(1, "foo"), (2, "bar"), (3, "navi")]}
        
        # Se construye la consulta SQL utilizando el nombre del procedimiento almacenado y el parámetro de la tabla de valor.
        concatena_nombre_sp = f'{CALL {proc_name} (:tvp)}'
        sql = f"{concatena_nombre_sp}"
        print('PRUEBA EJECUTAR')
        
        # Se ejecuta la consulta SQL utilizando la conexión conn, pasando los datos a través del parámetro data.
        # Luego, se obtienen todos los resultados devueltos por la ejecución de la consulta y se imprimen.
        print(conn.execute(text(sql), data).fetchall())
        # [(1, 'new_foo'), (2, 'new_bar')]
        
        
        # Mi prueba
        print('PRUEBA')
        # Se define el nombre del procedimiento almacenado de la base de datos que se va a llamar, en este caso, "stpr_InsertLogErroresFiltrado".
        proc_name_dos = "stpr_InsertLogErroresFiltrado"
        
        # Se crea un diccionario llamado datos_prueba que contiene datos de ejemplo para realizar la inserción en la base de datos
        datos_prueba = {
            'OrderId': ['ORD001', 'ORD002', 'ORD003'],
            'idLog': [1, 2, 3],
            'IdLogPrincipal': [101, 102, 103],
            'IdNombreTarea': ['Tarea1', 'Tarea2', 'Tarea3'],
            'FechaInicioTareaLog': ['2024-02-25 10:00:00', '2024-02-25 11:00:00', '2024-02-25 12:00:00'],
            'FechaFinTareaLog': ['2024-02-25 10:30:00', '2024-02-25 11:30:00', '2024-02-25 12:30:00'],
            'FechaTareaLogPrincipal': ['2024-02-25 09:00:00', '2024-02-25 10:00:00', '2024-02-25 11:00:00'],
            'MensajeError': ['Mensaje 1', 'Mensaje 2', 'Mensaje 3']
        }
        
        # Se crea un DataFrame de Pandas llamado df utilizando el diccionario datos_prueba.
        df = pd.DataFrame(datos_prueba)
        
        # Se convierte el DataFrame df a una lista de tuplas llamada data_dos, donde cada tupla representa una fila del DataFrame.
        data_dos = [tuple(row) for row in df.to_numpy()]
        
        # Se crea un diccionario llamado data_tvp que contiene la lista de tuplas bajo la clave 'tvp', necesario para pasar los datos al procedimiento almacenado como un parámetro de tipo tabla de valor.
        data_tvp = {"tvp": data_dos}
        
        # Se construye la consulta SQL utilizando el nombre del procedimiento almacenado y el parámetro de la tabla de valor.
        concatena_nombre_sp = f'{CALL {proc_name_dos} (:tvp)}'
        sql = f"{concatena_nombre_sp}"
        
        # Se ejecuta la consulta SQL utilizando la conexión conn, pasando los datos a través del parámetro data_tvp.
        conn.execute(text(sql), data_tvp)
        print('FIN PRUEBA')
        
        
    
    # Cerrar la conexión del motor
    engine.dispose()


def ejecutar_sp_insercion(nombre_sp:str, data_frame_errores: pd.DataFrame, cadena_conexion):
    """
        Ejecuta un procedimiento almacenado para insertar información de errores en una base de datos.

        Args:
            nombre_sp (str): Nombre del procedimiento almacenado que se va a ejecutar.
            data_frame_errores (pd.DataFrame): DataFrame que contiene los datos de errores a insertar en la base de datos.

    """
    try:
        # Definir la conexión al motor de la base de datos
        engine = create_engine(cadena_conexion, echo=False)
        
        with engine.begin() as conn:
            print(f'{bcolors.OK}Inicio del proceso para insertar datos en la tabla LogErroresFiltrado{bcolors.RESET}')
        
            # Convertir el DataFrame de errores a una lista de tuplas para los parámetros del procedimiento almacenado
            tupla_lista_errores = [tuple(row) for row in data_frame_errores.to_numpy()]
            dataos_parametros_con_valores_de_tabla = {"tvp": tupla_lista_errores}
            concatena_nombre_sp = f'{CALL {nombre_sp} (:tvp)}'
            sql = f"{concatena_nombre_sp}"
            
            # registros_invalidos = validar_dataframe(data_frame_errores, 'FechaTareaLogPrincipal')

            # if registros_invalidos:
            #     print(f"{bcolors.FAIL}Se encontraron registros con fechas inválidas:{bcolors.RESET}")
            #     for registro in registros_invalidos:
            #         print(registro)
            # else:
            #     print(f"{bcolors.OK}No se encontraron registros con fechas inválidas.{bcolors.RESET}")
            
            
            # Ejecutar el procedimiento almacenado con los parámetros y valores de tabla
            conn.execute(text(sql), dataos_parametros_con_valores_de_tabla)
            print(f'\t{len(data_frame_errores)} registros insertados en la tabla LogErroresFiltrado.')
            print(f'{bcolors.OK}FIN del proceso para insertar datos en la tabla LogErroresFiltrado{bcolors.RESET}')
        
        # Cerrar la conexión del motor
        engine.dispose()
    except Exception as error:
        # Mostrar un mensaje de error si ocurre algún problema durante la ejecución del procedimiento almacenado
        mensaje_error = f'Se ha producido un error al ejecutar el procedimiento almacenado {nombre_sp}: {str(error)}'
        print(f"{bcolors.FAIL}{mensaje_error}{bcolors.RESET}")
        logger_error.error(mensaje_error)
