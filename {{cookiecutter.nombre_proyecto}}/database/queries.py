GET_CONSULTA_1_DB = '''
SELECT TOP 10
*
FROM SCHEMA.NombreTablaAConsultar WITH(NOLOCK)
WHERE Fecha BETWEEN '2024-01-01 00:00:00' AND '2024-01-31 23:59:59'
'''