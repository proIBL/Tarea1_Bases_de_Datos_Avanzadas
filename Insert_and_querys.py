import mysql.connector
import csv
import pandas as pd
import os
from datetime import timedelta
import numpy as np

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


def query11(conn):
    # El "centro" geografico de la cdmx está aprox en 19.3° N, 99.1° O
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Magnitud', 'Latitude', 'Longitude'])
    cursor.execute(
        "SELECT `Magnitud`, `Latitud`, `Longitud` FROM `sismos` WHERE ST_Distance_Sphere(POINT(`Longitud`, `Latitud`), POINT(-99.1, 19.3)) <= 100000 AND Magnitud IS NOT NULL;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Magnitud', 'Latitude', 'Longitude'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Magnitud', 'Latitude', 'Longitude'])], ignore_index=True)
    dataframe_to_csv(df, 'query11.csv')
    cursor.close()


def query12(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Cantidad de sismos'])
    cursor.execute(
        "SELECT `Estado`, COUNT(*) AS `Cantidad de sismos` FROM `sismos` WHERE `Profundidad` >= 100 AND YEAR(`Fecha Hora UTC`) >= 2020 GROUP BY `Estado`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query12.csv')
    cursor.close()


def query13(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Cantidad de sismos'])
    cursor.execute(
        "SELECT `Estado`, COUNT(*) AS `Cantidad de sismos` FROM `sismos` WHERE `Magnitud` >= 5.0 GROUP BY `Estado`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query13.csv')
    cursor.close()


def query14(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Grupo de Profundidad por Km', 'Cantidad de sismos'])
    cursor.execute(
        "SELECT FLOOR(`Profundidad` / 10) * 10 AS `Grupo Profundidad`, COUNT(*) AS `Cantidad de Sismos` FROM Sismos GROUP BY `Grupo Profundidad` ORDER BY `Grupo Profundidad`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Grupo de Profundidad por Km', 'Cantidad de sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Grupo de Profundidad por Km', 'Cantidad de sismos'])], ignore_index=True)
    dataframe_to_csv(df, 'query14.csv')
    cursor.close()


def query15(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Cantidad de sismos'])
    cursor.execute(
        "SELECT `Estado`, COUNT(*) AS `Cantidad de Sismos` FROM `Sismos` WHERE Estado IN ('JAL', 'BCS', 'SIN', 'GRO', 'CHIS', 'MICH', 'OAX', 'BC', 'VER', 'SON', 'COL', 'NAY', 'TAB', 'CAMP', 'QR', 'TAMS', 'YUC') AND Profundidad >= 50 GROUP BY `Estado`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Cantidad de sismos'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query15.csv')
    cursor.close()


def query16(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Año', 'Desviación Estandar'])
    cursor.execute(
        "SELECT YEAR(`Fecha Hora UTC`) AS Año, STDDEV_POP(Magnitud) AS `Desviacion Estandar` FROM Sismos WHERE YEAR(`Fecha Hora UTC`) >= 2015 GROUP BY YEAR(`Fecha Hora UTC`) ORDER BY Año;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Año', 'Desviación Estandar'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Año', 'Desviación Estandar'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query16.csv')
    cursor.close()


def query17(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT `Magnitud` FROM `Sismos` WHERE `Magnitud` IS NOT NULL;")
    resultados = cursor.fetchall()
    Lista_Magnitudes = []
    for row in resultados:
        Lista_Magnitudes.append(float(row[0]))
    datos = np.array(Lista_Magnitudes)
    Q1 = np.percentile(datos, 25)
    Q3 = np.percentile(datos, 75)
    IQR = Q3 - Q1
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR
    valores_atipicos = datos[(datos < limite_inferior) | (datos > limite_superior)].tolist()
    conteo = pd.Series(valores_atipicos).value_counts().reset_index()
    conteo.columns = ['Magnitud', 'Frecuencia del valor atípico']
    conteo = conteo.sort_values(by='Magnitud')
    dataframe_to_csv(conteo, 'query17.csv')
    cursor.close()


def query18(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Grupo de Magnitud', 'Cantidad de sismos'])
    cursor.execute(
        "SELECT FLOOR(`Magnitud` / 0.5) * 0.5 AS `Grupo Magnitud`, COUNT(*) AS `Cantidad de Sismos` FROM Sismos WHERE `Magnitud` IS NOT NULL GROUP BY `Grupo Magnitud` ORDER BY `Grupo Magnitud`;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Grupo de Magnitud', 'Cantidad de sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Grupo de Magnitud', 'Cantidad de sismos'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query18.csv')
    cursor.close()


def query19(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Mediana'])
    cursor.execute(
        "SELECT estado, AVG(profundidad) AS mediana_profundidad FROM ( SELECT estado, profundidad, ROW_NUMBER() OVER (PARTITION BY estado ORDER BY profundidad) AS rn, COUNT(*) OVER (PARTITION BY estado) AS total FROM Sismos ) t WHERE rn IN (FLOOR((total + 1) / 2), CEIL((total + 1) / 2)) GROUP BY estado;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Mediana'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Mediana'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query19.csv')
    cursor.close()


def query20(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'P25', 'P50', 'P75'])
    cursor.execute(
        "WITH cte AS ( SELECT Estado, magnitud, NTILE(100) OVER (PARTITION BY Estado ORDER BY magnitud) AS percentil FROM Sismos ) SELECT Estado, COALESCE(MAX(CASE WHEN percentil = 25 THEN magnitud END), '') AS p25, COALESCE(MAX(CASE WHEN percentil = 50 THEN magnitud END), '') AS p50, COALESCE(MAX(CASE WHEN percentil = 75 THEN magnitud END), '') AS p75 FROM cte GROUP BY Estado;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'P25', 'P50', 'P75'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'P25', 'P50', 'P75'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query20.csv')
    cursor.close()


def query21(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Mes', 'Sismos de 6 o más'])
    cursor.execute(
        "SELECT Estado, MONTH(`Fecha Hora UTC`) AS Mes, COUNT(*) AS total_registros FROM Sismos WHERE `Magnitud` > 6.0 GROUP BY Estado, Mes ORDER BY Estado, Mes;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Mes', 'Sismos de 6 o más'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Mes', 'Sismos de 6 o más'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query21.csv')
    cursor.close()


def query22(conn):
    cursor = conn.cursor()
    df = pd.DataFrame(columns=['Estado', 'Cantidad de Sismos'])
    cursor.execute(
        "SELECT Estado, COUNT(*) AS total_registros FROM Sismos WHERE `Magnitud` > 5.0 AND `Profundidad` < 30 GROUP BY Estado;")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = pd.DataFrame([row], columns=['Estado', 'Cantidad de Sismos'])
        else:
            df = pd.concat([df, pd.DataFrame([row], columns=['Estado', 'Cantidad de Sismos'])],
                           ignore_index=True)
    dataframe_to_csv(df, 'query22.csv')
    cursor.close()


def query23(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT `Magnitud`, `Profundidad`, `Estado` FROM `Sismos` WHERE `Magnitud` IS NOT NULL AND `Profundidad` IS NOT NULL;")
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=['Magnitud', 'Profundidad', 'Estado'])
    df[['Magnitud', 'Profundidad']] = df[['Magnitud', 'Profundidad']].astype(float)
    correlaciones = df.groupby('Estado')[['Magnitud', 'Profundidad']].corr().iloc[0::2, -1].reset_index()
    correlaciones = correlaciones[['Estado', 'Profundidad']].rename(columns={'Profundidad': 'Correlacion'})

    dataframe_to_csv(correlaciones, 'query23.csv')
    cursor.close()


def query24(conn):
    """
    Segun Google un sismo así es de 6.5 o mayor y con una profundidad menor a 70km,
    además de ocurrir en el mar, por lo que tomamos estados costeros
    """
    cursor = conn.cursor()
    df = create_empty_df()
    cursor.execute(
        "SELECT `Magnitud`, `Latitud`, `Longitud`, `Profundidad`, `Estatus`, `Fecha Hora`, `Fecha Hora UTC`, `Estado`, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad` FROM `Sismos` WHERE `Magnitud` >= 6.5 AND `Profundidad` <= 70 AND Estado IN ('JAL', 'BCS', 'SIN', 'GRO', 'CHIS', 'MICH', 'OAX', 'BC', 'VER', 'SON', 'COL', 'NAY', 'TAB', 'CAMP', 'QR', 'TAMS', 'YUC');")
    resultados = cursor.fetchall()
    for row in resultados:
        if df.empty:
            df = create_one_row_df(row)
        else:
            df = pd.concat([df, create_one_row_df(row)], ignore_index=True)
    dataframe_to_csv(df, 'query24.csv')
    cursor.close()


def query25(conn):
    pass


def query26(conn):
    pass


def query27(conn):
    pass


def query28(conn):
    pass


def query29(conn):
    pass


def query30(conn):
    pass


def query31(conn):
    pass


def query32(conn):
    pass


def query33(conn):
    pass


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
# query11(connection)
# query12(connection)
# query13(connection)
# query14(connection)
# query15(connection)
# query16(connection)
# query17(connection)
# query18(connection)
# query19(connection)
# query20(connection)
# query21(connection)
# query22(connection)
# query23(connection)
query24(connection)
# query25(connection)
# query26(connection)
# query27(connection)
# query28(connection)
# query29(connection)
# query30(connection)
# query31(connection)
# query32(connection)
# query33(connection)


# Cerrar conexión
connection.close()
