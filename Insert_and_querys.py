import mysql.connector
import csv
import pandas as pd
import os
from datetime import timedelta


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
    cursor.close()


def query5(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Magnitud Promedio'])
    cursor.execute(
        "SELECT `estado`, AVG(`Magnitud`) AS `Magnitud Promedio` FROM sismos WHERE YEAR(`Fecha Hora UTC`) >= 2024 GROUP BY estado;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Magnitud Promedio'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Magnitud Promedio'])], ignore_index=True)
    dataframe_to_csv(df, 'query5.csv')
    cursor.close()


def query6(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Número de Mes', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT MONTH(`Fecha Hora UTC`) AS `Mes`, COUNT(*) AS `Cantidad de sismos` FROM sismos WHERE YEAR(`Fecha Hora UTC`) = 2024 GROUP BY `Mes`")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Número de Mes', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Número de Mes', 'Cantidad de Sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query6.csv')
    cursor.close()


def query7(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Año', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT YEAR(`Fecha Hora UTC`) AS `Año`, COUNT(*) AS `Cantidad de sismos` FROM sismos GROUP BY `Año` ORDER BY `Cantidad de sismos` DESC LIMIT 5;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Año', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Año', 'Cantidad de Sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query7.csv')
    cursor.close()


def query8(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Periodo', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT CASE WHEN MONTH(`Fecha Hora UTC`) BETWEEN 1 AND 6 THEN 'Primera mitad' WHEN MONTH(`Fecha Hora UTC`) BETWEEN 7 AND 12 THEN 'Segunda mitad' END AS `Periodo`, COUNT(*) AS `Cantidad_de_sismos` FROM sismos WHERE YEAR(`Fecha Hora UTC`) = 2023 GROUP BY `Periodo` ORDER BY `Periodo`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Periodo', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Periodo', 'Cantidad de Sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query8.csv')
    cursor.close()


def query9(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Década', 'Magnitud Promedio'])
    cursor.execute(
        "SELECT FLOOR(YEAR(`Fecha Hora UTC`) / 10) * 10 AS `Década`, AVG(`Magnitud`) AS `Magnitud_Promedio` FROM sismos WHERE YEAR(`Fecha Hora UTC`) >= 1900 GROUP BY `Década` ORDER BY `Década`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Década', 'Magnitud Promedio'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Década', 'Magnitud Promedio'])], ignore_index=True)
    dataframe_to_csv(df, 'query9.csv')
    cursor.close()


def query10(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT `Fecha Hora UTC` FROM sismos where `magnitud` >= 6.0 ORDER BY `Fecha Hora UTC` DESC;")
    resultados = cursor.fetchall()
    fechas = [row[0] for row in resultados]
    sumatory = timedelta(0)
    for i in range(len(fechas) - 1):
        sumatory += (fechas[i] - fechas[i + 1])
    intervalo_promedio = sumatory / (len(fechas) - 1)
    with open(os.path.join('QueryResults', 'query10.txt'), "w") as file:
        file.write(f"Intervalo promedio entre sismos de magnitud mayor a 6.0: {intervalo_promedio}\n")
    cursor.close()


connection = mysql_connection_sismos()
# insert_csv_to_db('QueryResults/Sismos_Limpios.csv', connection)
# query1(connection)
# query2(connection)
# query3(connection)
# query4(connection)
# query5(connection)
# query6(connection)
# query7(connection)
# query8(connection)
# query9(connection)
# query10(connection)

# Cerrar conexión
connection.close()
