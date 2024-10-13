# Imports de Python
import os
import tempfile
from io import StringIO

# Imports de terceros
from dotenv import load_dotenv
from decouple import config, UndefinedValueError
from cryptography.fernet import Fernet
from decouple import Config, RepositoryEnv

# Imports propios
import utils.utilidades as utilidades

class variables_entorno:
    def __init__(self):
        self.USUARIO_DB = None
        self.CONTRASENA_DB = None
        self.SERVIDOR_DB = None
        self.INSTANCIA_DB = None  # Especifica la instancia si es necesario
        self.NOMBRE_DB = None
        self.API_KEY = None
        self.VALUE_API_KEY = None
        self.BASE_URL = None
        self.CORREO_REMITENTE = None
        self.CONTRASENA_REMITENTE = None
        self.EXPORTAR_A_EXCEL = None
        # self.mensaje_correo = "El correo debe contener un INICIO y FIN PROCESO, sino lo contiene debe comunicarse con T.I:\n\n"
        self.mensaje_correo = ""
    
    def cargar_variables_entorno_encriptadas(self, clave):
        f = Fernet(clave)
        with open(utilidades.ruta_recurso('.env.enc'), 'rb') as file:
            datos_encriptados = file.read()
        datos_desencriptados = f.decrypt(datos_encriptados)
        datos_desencriptados_str = datos_desencriptados.decode()

        # Leer la cadena desencriptada y procesar las variables de entorno
        config = Config(StringIO(datos_desencriptados_str))
        try:
            self.USUARIO_DB = config('USER_DB')
            self.CONTRASENA_DB = config('PASS_DB')
            self.SERVIDOR_DB = config('SERVER_DB')
            self.INSTANCIA_DB = config('INSTANCE_DB')
            self.NOMBRE_DB = config('NAME_DB')
            self.API_KEY = config('API_KEY')
            self.VALUE_API_KEY = config('VALUE_API_KEY')
            self.BASE_URL = config('BASE_URL')
            self.CORREO_REMITENTE = config('CORREO_REMITENTE')
            self.CONTRASENA_REMITENTE = config('CONTRASENA_REMITENTE')
            self.EXPORTAR_A_EXCEL = config('EXPORTAR_A_EXCEL')
        except UndefinedValueError as e:
            # Manejar errores si alguna variable de entorno falta en el archivo desencriptado
            print("Error al cargar variables de entorno:", e)

    def cargar_variables_entorno_local(self):
        
        load_dotenv(dotenv_path=utilidades.ruta_recurso('.env'))
        
        self.USUARIO_DB = config('USER_DB')
        self.CONTRASENA_DB = config('PASS_DB')
        self.SERVIDOR_DB = config('SERVER_DB')
        self.INSTANCIA_DB = config('INSTANCE_DB')
        self.NOMBRE_DB = config('NAME_DB')
        self.API_KEY = config('API_KEY')
        self.VALUE_API_KEY = config('VALUE_API_KEY')
        self.BASE_URL = config('BASE_URL')
        self.CORREO_REMITENTE = config('CORREO_REMITENTE')
        self.CONTRASENA_REMITENTE = config('CONTRASENA_REMITENTE')
        self.EXPORTAR_A_EXCEL = config('EXPORTAR_A_EXCEL')
    
    def concatenar_mensaje_correo(self, mensaje):
        """
        Concatena un mensaje al atributo mensaje_correo.
        """
        self.mensaje_correo += mensaje

# Crear una instancia de la clase para acceder a las variables de entorno
obj_variables_entorno = variables_entorno()