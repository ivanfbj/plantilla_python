import pyodbc

def conectar_y_verificar(usuario, contraseña, ip_base_de_datos, instancia_db, nombre_base_datos):
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={ip_base_de_datos}\\{instancia_db};"  # Usa la IP o nombre del servidor si es necesario
            f"DATABASE={nombre_base_datos};"
            f"UID={usuario};"
            f"PWD={contraseña};"
        )
        # Intentar conectar a SQL Server
        conn = pyodbc.connect(conn_str, timeout=5)
        print(f"Conexión exitosa con el usuario {usuario}!")
        
        # Ejecutar una consulta simple para verificar el acceso
        cursor = conn.cursor()
        cursor.execute("SELECT 1 1;")
        row = cursor.fetchone()
        if row:
            print("Consulta exitosa:", row)
        
        conn.close()

    except pyodbc.OperationalError as e:
        # Captura y muestra los detalles completos del error
        print(f"Error de conexión con el usuario {usuario}:")
        print(e)
    except Exception as e:
        # Captura cualquier otro error que pueda ocurrir
        print("Otro error ocurrió:")
        print(e)

# Probar primero con el usuario 'sa'
conectar_y_verificar("usr_base_de_datos", "password_DB", "10.15.20.5", "sqlexpress", "testDB")

# Luego puedes probar con el usuario 'usr_app_contabilidad'
# Descomenta la línea siguiente para probar con usr_app_contabilidad
# conectar_y_verificar("usr_app_contabilidad", "ContraseñaSegura123!")
