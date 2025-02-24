import mysql.connector
import csv
import pandas as pd
import os


def mysql_connection_sismos():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="Sismos"
    )


def create_empty_df():
    return pd.DataFrame(columns=['Magnitud', 'Latitud', 'Longitud', 'Profundidad', 'Estatus',
                                 'Fecha Hora', 'Fecha Hora UTC', 'Estado', 'Referencia Distancia',
                                 'Referencia Direccion', 'Referencia Localidad'])


def create_one_row_df(tupla):
    return pd.DataFrame([tupla], columns=['Magnitud', 'Latitud', 'Longitud', 'Profundidad', 'Estatus',
                                          'Fecha Hora', 'Fecha Hora UTC', 'Estado', 'Referencia Distancia',
                                          'Referencia Direccion', 'Referencia Localidad'])


def insert_csv_to_db(route, conn):
    cursor = conn.cursor()
    BATCH_SIZE = 5000  # Ajusta según la capacidad de tu servidor

    conn.autocommit = False

    # Insertar datos
    query = """INSERT INTO `sismos` (`Magnitud`, `Latitud`, `Longitud`, `Profundidad`, `Estatus`, `Fecha Hora`, `Fecha Hora UTC`, `Estado`, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad`) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    with open(route, "r", encoding="utf-8") as archivo:
        lector = csv.reader(archivo)
        datos_lote = []
        for i, row in enumerate(lector, start=1):
            datos_lote.append(tuple(None if valor.strip() == "" else valor for valor in row[1:]))

            if len(datos_lote) > BATCH_SIZE:
                cursor.executemany(query, datos_lote)
                conn.commit()
                datos_lote.clear()
                print(f"Insertadas {i} filas...")

        if datos_lote:
            cursor.executemany(query, datos_lote)
            conn.commit()
        print("Inserción completa ✅")

    conn.autocommit = True
    cursor.close()


def dataframe_to_csv(df, nombre_archivo):
    df.to_csv(os.path.join('QueryResults', nombre_archivo), index=False)
    print("Guardado realizado correctamente ✅")


def query1(conn):
    cursor = conn.cursor()
    df = create_empty_df()
    cursor.execute(
        "SELECT `Magnitud`, `Latitud`, `Longitud`, `Profundidad`, `Estatus`, `Fecha Hora`, `Fecha Hora UTC`, `Estado`, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad` FROM sismos where magnitud >= 7.0 ORDER BY magnitud DESC;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = create_one_row_df(row)
        else:
            df = pd.concat([df, create_one_row_df(row)], ignore_index=True)
    dataframe_to_csv(df, 'query1.csv')
    cursor.close()


def query2(conn):
    cursor = conn.cursor()
    df = create_empty_df()
    cursor.execute(
        "SELECT Magnitud, Latitud, Longitud, Profundidad, Estatus, `Fecha Hora`, `Fecha Hora UTC`, Estado, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad` FROM sismos WHERE Estado = 'OAX' ORDER BY `Fecha Hora UTC` DESC LIMIT 10;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = create_one_row_df(row)
        else:
            df = pd.concat([df, create_one_row_df(row)], ignore_index=True)
    dataframe_to_csv(df, 'query2.csv')
    cursor.close()


def query3(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Año', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT YEAR(`Fecha Hora UTC`) AS Año, COUNT(*) AS total FROM sismos WHERE YEAR(`Fecha Hora UTC`) > 2014 GROUP BY YEAR(`Fecha Hora UTC`);")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Año', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Año', 'Cantidad de Sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query3.csv')
    print("Guardado realizado correctamente ✅")
    cursor.close()


def query4(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT `Estado`, COUNT(*) AS `Cantidad de sismos` FROM sismos GROUP BY `Estado` HAVING COUNT(*) >= 1000;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Cantidad de Sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query4.csv')
    print("Guardado realizado correctamente ✅")
    cursor.close()


connection = mysql_connection_sismos()
# insert_csv_to_db('QueryResults/Sismos_Limpios.csv', connection)
# query1(connection)
# query2(connection)
# query3(connection)
# query4(connection)

# Cerrar conexión
connection.close()
