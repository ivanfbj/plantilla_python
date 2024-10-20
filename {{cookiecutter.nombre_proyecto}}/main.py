# Importaciones de la biblioteca estándar de Python
import time
import os
import sys

# Importaciones propias
from utils.logger import logger_info, logger_debug, logger_error
from database.conexion_db import conectar_bd_pyodbc, conectar_bd_sqlalchemy
from utils.variables_entorno import VariablesEntorno

# Importaciones de terceros
from decouple import Config, UndefinedValueError, RepositoryEnv
import sqlalchemy
import pyodbc

def main():
    try:
        # Registrar inicio de la ejecución
        logger_info.info("********** INICIO de la ejecución **********")
        logger_debug.debug("********** INICIO de la ejecución **********")
        logger_error.error("********** INICIO de la ejecución **********")
        
        print('Hola, este es el proyecto llamado "{{cookiecutter.nombre_proyecto}}" se ha inicializado correctamente')
        
        # Instanciamos la clase para cargar las variables de entorno
        variables_entorno = VariablesEntorno()
        
        # Conectar a la base de datos
        # engine_database = conectar_bd_sqlalchemy()
        engine_database = conectar_bd_pyodbc(variables_entorno.USUARIO_DB, variables_entorno.CONTRASENA_DB, variables_entorno.SERVIDOR_DB, variables_entorno.NOMBRE_DB, variables_entorno.INSTANCIA_DB)
        
        if engine_database:
            try:
                print('Conexión a la base de datos')
                # Opcional: exportar el resultado a CSV
                # df.to_csv('Exportar/nombre_archivo.csv', index=False, sep='|', encoding='ansi')
            
            except Exception as e:
                logger_error.error(f"Ocurrió un error durante el procesamiento de productos: {e}")
                raise
            
            finally:
                # Asegurar que la conexión a la base de datos se cierre correctamente
                if isinstance(engine_database, sqlalchemy.engine.base.Engine):
                    engine_database.dispose()
                    logger_info.info("Conexión SQLAlchemy cerrada.")
                elif isinstance(engine_database, pyodbc.Connection):
                    engine_database.close()
                    logger_info.info("Conexión pyodbc cerrada.")
                else:
                    logger_info.warning("No se pudo determinar el tipo de conexión.")
    
    except Exception as e:
        logger_error.error(f"Ocurrió un error crítico en la ejecución del script: {e}")
    
    finally:
        # Registrar fin de la ejecución
        logger_info.info("********** FIN de la ejecución **********")
        logger_debug.debug("********** FIN de la ejecución **********")
        logger_error.error("********** FIN de la ejecución **********")

if __name__ == "__main__":
    main()
