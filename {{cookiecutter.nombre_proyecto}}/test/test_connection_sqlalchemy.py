from sqlalchemy import create_engine, exc, text
from sqlalchemy.pool import NullPool

def conectar_y_verificar(usuario, contraseña, ip_base_de_datos, instancia_base_de_datos, nombre_base_de_datos):
    # Crear la URL de conexión usando SQLAlchemy
    connection_url = (
        f"mssql+pyodbc://{usuario}:{contraseña}@{ip_base_de_datos}\\{instancia_base_de_datos}/{nombre_base_de_datos}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )

    # Crear el motor de conexión de SQLAlchemy con NullPool para desactivar el pooling
    engine = create_engine(connection_url, poolclass=NullPool)

    try:
        # Intentar conectar al servidor y ejecutar una consulta simple
        with engine.connect() as connection:
            print(f"Conexión exitosa con el usuario {usuario}!")
            
            # Ejecutar una consulta simple usando `text` para SQL sin formato
            result = connection.execute(text("SELECT 1 1"))
            row = result.fetchone()
            if row:
                print("Consulta exitosa:", row)

    except exc.DBAPIError as e:
        # Captura y muestra los detalles del error de SQLAlchemy
        print(f"Error de conexión con el usuario {usuario}:")
        print(e)
    except exc.SQLAlchemyError as e:
        # Captura cualquier otro error específico de SQLAlchemy
        print("Otro error específico de SQLAlchemy ocurrió:")
        print(e)
    except Exception as e:
        # Captura cualquier otro error general
        print("Otro error ocurrió:")
        print(e)

# Probar primero con el usuario 'sa'
# conectar_y_verificar("sa", "TOspaN0y4P+e", "10.10.21.5", "EDMSQL", "data_reporting")
conectar_y_verificar("usr_base_de_datos", "password_DB", "10.15.20.5", "sqlexpress", "testDB")

# Luego puedes probar con el usuario 'usr_app_contabilidad'
# Descomenta la línea siguiente para probar con usr_app_contabilidad
# conectar_y_verificar("usr_app_contabilidad", "ContraseñaSegura123!")
