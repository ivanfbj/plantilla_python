# Imports de Python
import os
from decouple import Config, RepositoryEnv, UndefinedValueError

# Imports propios
from utils.utilidades import ruta_recurso
from utils.logger import logger_info, logger_debug, logger_error


class VariablesEntorno:
    _instance = None  # Variable para almacenar la instancia Singleton

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VariablesEntorno, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return  # Evitar la inicialización si ya fue inicializado
        self._initialized = True

        # Inicializamos las variables a None
        self.USUARIO_DB = None
        self.CONTRASENA_DB = None
        self.SERVIDOR_DB = None
        self.INSTANCIA_DB = None
        self.NOMBRE_DB = None
        self.NOMBRE_CARPETA_LOGS = None

        # Cargar automáticamente las variables de entorno al inicializar la clase
        self.cargar_variables_entorno_local()

    def cargar_variables_entorno_local(self):
        try:
            config = Config(RepositoryEnv(ruta_recurso('.env')))
            
            # Cargar las variables de entorno
            self.USUARIO_DB = config('USUARIO_DB')
            self.CONTRASENA_DB = config('CONTRASENA_DB')
            self.SERVIDOR_DB = config('SERVIDOR_DB')
            self.INSTANCIA_DB = config('INSTANCIA_DB')
            self.NOMBRE_DB = config('NOMBRE_DB')
            self.NOMBRE_CARPETA_LOGS = config('NOMBRE_CARPETA_LOGS')

            logger_info.info("Variables de entorno cargadas exitosamente")
        except UndefinedValueError as e:
            logger_error.error(f"Error al obtener una de las variables de entorno: {e}")
            raise

# Ejemplo de cómo acceder a las variables de entorno en otros archivos
variables_entorno = VariablesEntorno()

# Ahora en cualquier parte de tu código, puedes usar la instancia Singleton
# Por ejemplo: variables_entorno.USUARIO_DB
