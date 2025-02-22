import mysql.connector
import csv
import pandas as pd


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


def insert_csv_to_db(conn):
    cursor = conn.cursor()
    BATCH_SIZE = 5000  # Ajusta según la capacidad de tu servidor

    conn.autocommit = False

    # Insertar datos
    query = """INSERT INTO `sismos` (`Magnitud`, `Latitud`, `Longitud`, `Profundidad`, `Estatus`, `Fecha Hora`, `Fecha Hora UTC`, `Estado`, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad`) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    with open(r"C:\Users\OMEN\Downloads\Sismos_Limpios.csv", "r", encoding="utf-8") as archivo:
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


def query1(conn):
    cursor = conn.cursor()
    df = create_empty_df()
    cursor.execute(
        "SELECT `Magnitud`, `Latitud`, `Longitud`, `Profundidad`, `Estatus`, `Fecha Hora`, `Fecha Hora UTC`, `Estado`, `Referencia Distancia`, `Referencia Direccion`, `Referencia Localidad` FROM sismos where magnitud >= 7.0")
    resultados = cursor.fetchall()
    for row in resultados:
        print(row)
        pd.concat([df, create_one_row_df(row)], ignore_index=True)
    print(df.head())
    cursor.close()


conection = mysql_connection_sismos()
# insert_csv_to_db(conection)
query1(conection)

# Cerrar conexión
conection.close()
