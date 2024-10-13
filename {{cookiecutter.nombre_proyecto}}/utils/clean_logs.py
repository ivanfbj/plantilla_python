# Importaciones de la biblioteca estándar de Python
import os
import time
from pathlib import Path

# Importaciones propias
from logger import logger_info, logger_debug, logger_error

def clean_old_logs(directory: str, max_age_minutes: int):
    """
    Elimina archivos en la carpeta de logs si la fecha de modificación supera un tiempo en minutos.
    
    :param directory: Ruta de la carpeta donde se encuentran los archivos de log
    :param max_age_minutes: Tiempo máximo en minutos antes de eliminar un archivo (por defecto 5 minutos para pruebas)
    """
    try:
        # Convertir el tiempo máximo de minutos a segundos
        max_age_seconds = max_age_minutes * 60
        current_time = time.time()

        # Crear el Path del directorio de logs
        log_directory = Path(directory)
        
        # Verificar si el directorio existe
        if not log_directory.exists():
            logger_info.info(f"La carpeta {directory} no existe. No se realizó ninguna limpieza.")
            return
        
        # Iterar sobre todos los archivos del directorio
        for log_file in log_directory.iterdir():
            if log_file.is_file():  # Asegurarse de que es un archivo y no un directorio
                # Obtener el tiempo de la última modificación
                last_modified_time = log_file.stat().st_mtime
                file_age = current_time - last_modified_time
                
                # Si el archivo supera la edad máxima permitida, eliminarlo
                if file_age > max_age_seconds:
                    logger_info.info(f"Eliminando archivo antiguo: {log_file}. Con ultima fecha de modifiacción {last_modified_time}")
                    log_file.unlink()  # Borrar el archivo

    except FileNotFoundError as e:
        logger_error.error(f"Error: {e}. No se encontró la carpeta de logs. El proceso continuará sin detenerse.")
    except Exception as e:
        logger_error.error(f"Ocurrió un error inesperado: {e}. El proceso continuará.")

