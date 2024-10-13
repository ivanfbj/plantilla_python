#!/bin/bash

# Verificar si la carpeta venv no existe
if [ ! -d "venv" ]; then
    echo "La carpeta venv no existe. Creando entorno virtual..."
    python -m venv venv
    if [ $? -ne 0 ]; then
        echo "Error: No se pudo crear el entorno virtual."
        exit 1
    fi
fi

# Activar el entorno virtual
echo "Activando el entorno virtual..."
source venv/Scripts/activate

# Verificar si la carpeta .git no existe
if [ ! -d ".git" ]; then
    echo "La carpeta .git no existe. Inicializando repositorio Git..."
    git init
    if [ $? -ne 0 ]; then
        echo "Error: No se pudo inicializar Git. Verifica si Git está instalado."
        exit 1
    fi
fi

# Verificar si no hay librerías instaladas
if [ -z "$(pip freeze)" ]; then
    echo "No hay librerías instaladas en el entorno virtual. Instalando dependencias desde requirements.txt..."
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "Error: No se pudieron instalar las dependencias."
        exit 1
    fi
else
    echo "Librerías ya instaladas en el entorno virtual."
fi

# Ejecutar el archivo Python principal
echo "Ejecutando main.py..."
python main.py
