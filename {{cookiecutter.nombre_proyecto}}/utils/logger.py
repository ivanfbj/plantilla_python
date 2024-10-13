import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
from decouple import Config, UndefinedValueError, RepositoryEnv

# Definir tamaño máximo de los archivos de log (en bytes) como variable global
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB por defecto
BACKUP_LOG_COUNT = 5  # Número máximo de archivos de respaldo que se mantendrán

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

def setup_rotating_logger(name: str, log_file: str, max_size: int = MAX_LOG_SIZE, backup_count: int = BACKUP_LOG_COUNT, level=logging.INFO):
    """
    Configura un logger con rotación de archivos basada en el tamaño y salida en consola.
    
    :param name: Nombre del logger
    :param log_file: Ruta del archivo de log
    :param max_size: Tamaño máximo del archivo de log en bytes antes de rotar (por defecto 5 MB)
    :param backup_count: Número máximo de archivos de respaldo que se mantendrán
    :param level: Nivel de logging
    """
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Rotating File Handler
    file_handler = RotatingFileHandler(log_file, maxBytes=max_size, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    
    # Stream Handler (Consola)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configuración del logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Agregar handlers al logger
    logger.addHandler(file_handler)
    
    # Habilitar o comentarear esta linea si se desea ver en consola cada mensaje del LOGGER
    logger.addHandler(console_handler)
    
    return logger

config = Config(RepositoryEnv(ruta_recurso('.env')))

# Crear el directorio 'logs_diarios' si no existe
log_directory = Path(config('NOMBRE_CARPETA_LOGS'))
log_directory.mkdir(exist_ok=True)

# Obtener la fecha actual en formato YYYY-MM-DD
current_date = datetime.now().strftime('%Y-%m-%d')

# Definir las rutas completas para los archivos de log, incluyendo la fecha
debug_log_path = log_directory / f'debug_{current_date}.log'
info_log_path = log_directory / f'info_{current_date}.log'
error_log_path = log_directory / f'error_{current_date}.log'

# Configurar los loggers con rotación de archivos, utilizando el tamaño máximo global (MAX_LOG_SIZE)
logger_debug = setup_rotating_logger('logger_debug', debug_log_path, max_size=MAX_LOG_SIZE, backup_count=BACKUP_LOG_COUNT, level=logging.DEBUG)
logger_info = setup_rotating_logger('logger_info', info_log_path, max_size=MAX_LOG_SIZE, backup_count=BACKUP_LOG_COUNT, level=logging.INFO)
logger_error = setup_rotating_logger('logger_error', error_log_path, max_size=MAX_LOG_SIZE, backup_count=BACKUP_LOG_COUNT, level=logging.ERROR)
