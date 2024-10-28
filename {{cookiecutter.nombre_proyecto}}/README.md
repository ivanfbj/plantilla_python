# {{cookiecutter.titulo}}

Desarrollado por: {{cookiecutter.autor}}

{{cookiecutter.descripcion}}

## Requerimientos

1. Instalar Python 3.12.1 o superior para poder ejecutar el código
2. Instalar GIT para poder clonar el repositorio y descargar todo el código con su historial de versiones.
3. Instalar SQL Server Management Studio (SSMS) para visualizar las consultas o procedimientos almacendos descritos más adelante.
4. Tener los datos necesarios para la conexión a la base de datos, usuario, contraseña, ip del servidor, nombre de la base de datos e instancia del servidor.

## Instalación

Se recomienda usar GIT Bash para los comandos de instalación e inicializar el proyecto.

1. Descargar o clonar el repositorio con el código fuente.
2. Dentro de la carpeta el proyecto "{{cookiecutter.nombre_proyecto}}" es necesario crear un entorno virtual para instalar todas las librerías para ejecutar el código sin ningúna novedad.
    - Para crear el entorno virtual se realiza dentro de la carpeta del proyecto y ejecutando el siguiente comando `python -m venv venv`
3. Una vez creado el entorno virtual de trabajo se debe inicializar con el comando `source venv/Scripts/activate`
4. Luego se instalan todas las dependencias necesarias para ejecutar el programa, la lista y versiones de las dependendicas se encuentra en el archivo `requirements.txt` y el comando para realizar la instalación es `pip install -r requirements.txt`
5. Por temas de seguridad las credenciales de la base de datos se usan localmente y no se sincronizan con el repositorio remoto para lo cual es necesario crear un archivo con el siguiente nombre `.env`, este archivo no tiene ninguna extensión de archivo.
6. El archivo `.env` debe contener la misma estructura que se haya definido en el archivo .env.template para que se puedan usar las variables según se hayan configurado y definido en el proyecto.
7. Con esto ya se puede ejecutar el código por medio del comando `python main.py`
8. El comando que se debe ejecutar para generar el archivo "{{cookiecutter.nombre_proyecto}}" y poderlo compartir con otros usuario y ejecutar sin necesidad de instalar python y sus librerias es:
    ```Bash
    pyinstaller --add-data ".env:." --onefile --icon=logo_intrena.ico --clean --name        {{cookiecutter.nombre_proyecto}} main.py
    ```

## Explicación general de la lógica del código

### Archivos:
- **utils/bcolors.py**: Clase que permite implementar colores para resaltar mensajes en la consola.
- **database/consultas.py**: Contiene funciones que ejecutan alguna consulta o procedimiento almacenado sobre la base de datos.
- **database/conexion_db.py**: Contiene la función para conectarse a la base de datos ya sea por la librería SqlAlchemy o pyodbc.
- **utils/utilidades.py**: Contiene funciones con diferentes funcionalidades, como por ejemplo crear carpeta, ruta del recurso para cuando hay que accerder a archivo dentro del proyecto, convertir lista en data frame, exportar lista a csv, se pueden implmenetar funcionalidades genericas.
- **main.py**: Es el archivo principal que ejecuta las funcionalidad del proyecto, se encarga de cargar las variables de entorno que contiene las credenciales a la base de datos, API y hacer uso de las diferentes clases y funciones para llevar a cabo el flujo del proceso.
- **utils/variables_entorno.py**: Es una clase que almacena la información de las variables de entorno para poderla utilizar desde cualquier otra clase que requiera los datos de conexión a la base de datos o al API.

### Flujo

En el momento de ejecutar el código se presentan los siguientes pasos generales:
1. Inicializa los archivos de logger: info, debug y error.
2. Crear la carpeta Exportar en donde quedarán los archivos exportados con la información de la base de datos.
3. Instancia un objeto con las variables de entorno para utilizarlas en cualquier parte del código.
4. Descripción paso 4

## Explicación del Comando para compilar y generar archivo .exe

```Bash
    pyinstaller --add-data ".env:." --onefile --icon=terminal.ico --clean --name {{cookiecutter.nombre_proyecto}} --hidden-import=pyodbc main.py
```

El archivo `.env.template` contiene la estructura de ejemplo que debe contener el archivo .env para las variables de entorno.

**--onefile**: Este parámetro indica que se requiere generar un solo archivo ejecutable en lugar de varios archivos. Esto significa que todos los archivos necesarios para ejecutar el programa se incluirán en un único archivo ejecutable.

**--icon=terminal.ico**: Este parámetro especifica el icono que se utilizará para el archivo ejecutable. En este caso, el icono se tomará del archivo terminal.ico.

**--clean**: Este parámetro indica que se requiere limpiar los archivos temporales generados por pyinstaller durante el proceso de construcción del ejecutable.

**--name {{cookiecutter.nombre_proyecto}}**: Este parámetro especifica el nombre del archivo ejecutable que se generará. En este caso, el nombre del ejecutable será {{cookiecutter.nombre_proyecto}}.

**main.py**: Este es el archivo principal de tu aplicación que pyinstaller convertirá en un ejecutable.

___

## Mejoras pendientes

1. Documentación del README.md
2. Mejora número 2
3. ~~Mejora completada e implementada.~~