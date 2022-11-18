import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

pd.set_option("display.max_columns", None)

from variables import fecha
from cfg import postgresql_config


# nombres de tablas

nombres_tablas = [
    "Region GBA",
    "Region Pampeana",
    "Region Noroeste",
    "Region Noreste",
    "Region Cuyo",
    "Region Patagonia",
]


df_lista_regiones = pd.DataFrame(nombres_tablas, columns=["region"])

# dataframes de regiones

region_gba = pd.read_csv(rf"../Dataframe/region_gba_{fecha}.csv")
region_pampeana = pd.read_csv(rf"../Dataframe/region_pampeana_{fecha}.csv")
region_noroeste = pd.read_csv(rf"../Dataframe/region_noroeste_{fecha}.csv")
region_noreste = pd.read_csv(rf"../Dataframe/region_noreste_{fecha}.csv")
region_cuyo = pd.read_csv(rf"../Dataframe/region_cuyo_{fecha}.csv")
region_patagonica = pd.read_csv(rf"../Dataframe/region_patagonica_{fecha}.csv")

dict_df_regiones = {
    "Region GBA": region_gba,
    "Region Pampeana": region_pampeana,
    "Region Noroeste": region_noroeste,
    "Region Noreste": region_noreste,
    "Region Cuyo": region_cuyo,
    "Region Patagonia": region_patagonica,
}


# conectar a base de datos

db = create_engine(postgresql_config)
conn = db.connect()


# crear tablas / querys


def crear_tabla_regiones():
    """
    Crea una tabla llamada regiones si no existe, y luego inserta los datos del marco de datos
    df_lista_regiones en la tabla
    """
    try:
        db.execute("SELECT * FROM regiones")
        print("Tabla regiones ya existe")
    except sqlalchemy.exc.ProgrammingError:
        db.execute(
            "CREATE TABLE regiones (id serial, region VARCHAR(50) primary key not null)"
        )

        for index, row in df_lista_regiones.iterrows():
            print(row["region"])
            try:
                db.execute(
                    f"SELECT region FROM regiones WHERE region = {row['region']}"
                )
                print(f"Region {row['region']} ya existe")

            except sqlalchemy.exc.ProgrammingError:
                db.execute(
                    """INSERT INTO regiones (region) VALUES (%s)""", row["region"]
                )
                print(f"{row['region']} insertada")

        print(
            "Tabla regiones creada con: ",
            db.execute("SELECT * FROM regiones").rowcount,
            " filas",
        )
        try:
            rows = db.execute("SELECT * FROM regiones").fetchall()
            for row in rows:
                print(row)

        except sqlalchemy.exc.DatabaseError as error:
            print(error)
    conn.close()


def crear_tabla_region(dict):
    """
    Crea una tabla por region si no existe, y luego inserta los datos del marco de datos
    dataframe de region
    """

    for key, region in dict.items():
        print(key)
        try:

            db.execute(f'SELECT * FROM "{key}"')
            print(
                f"Tabla {key} ya existe con: ",
                db.execute(f'SELECT * FROM "{key}"').rowcount,
                " filas",
            )

        except sqlalchemy.exc.ProgrammingError:
            region.to_sql(key, db, if_exists="replace", index=False)
            print(
                f"Tabla {key} creada con: ",
                db.execute(f'SELECT * FROM "{key}"').rowcount,
                " filas",
            )
            db.execute(f'ALTER TABLE "{key}" ADD PRIMARY KEY ("fecha")')
            db.execute(
                f'ALTER TABLE "{key}" ADD FOREIGN KEY ("region") REFERENCES regiones ("region")'
            )
    print("Tablas creadas\n")
    conn.close()


def update_region_gba():
    """
    Toma un marco de datos, itera sobre él e inserta cada fila en una tabla de PostgreSQL.
    """

    contador = 0
    for index, row in region_gba.iterrows():

        data = db.execute(
            f'SELECT fecha FROM "Region GBA" where fecha = %s', row["fecha"]
        )
        data2 = data.fetchone()

        if data2 == None:
            print(f"Actualizando tabla Region GBA")

            db.execute(
                """INSERT INTO "Region GBA" (
                "fecha",
                "Nivel general",
                "Alimentos y bebidas no alcohólicas",
                "Alimentos",
                "Pan y cereales",
                "Carnes y derivados",
                "Leche, productos lácteos y huevos",
                "Aceites, grasas y manteca",
                "Frutas",
                "Verduras, tubérculos y legumbres",
                "Azúcar, dulces, chocolate, golosinas, etc.",
                "Bebidas no alcohólicas",
                "Café, té, yerba y cacao",
                "Bebidas alcohólicas",
                "Tabaco",
                "Prendas de vestir y calzado",
                "Prendas de vestir y materiales",
                "Calzado",
                "Vivienda, agua, electricidad, gas y otros combustibles",
                "Alquiler de la vivienda y gastos conexos",
                "Alquiler de la vivienda",
                "Mantenimiento y reparación de la vivienda",
                "Electricidad, gas y otros combustibles",
                "Equipamiento y mantenimiento del hogar",
                "Bienes y servicios para la conservación del hogar",
                "Salud",
                "Productos medicinales, artefactos y equipos para la salud",
                "Gastos de prepagas",
                "Transporte",
                "Adquisición de vehículos",
                "Funcionamiento de equipos de transporte personal",
                "Combustibles y lubricantes para vehículos de uso del hogar",
                "Transporte público",
                "Comunicación",
                "Servicios  de telefonía e internet",
                "Recreación y cultura",
                "Equipos audiovisuales, fotográficos y de procesamiento de la información",
                "Servicios recreativos y culturales",
                "Periódicos, diarios, revistas, libros y artículos de papelería",
                "Educación",
                "Restaurantes y hoteles",
                "Restaurantes y comidas fuera del hogar",
                "Bienes y servicios varios",
                "Cuidado personal",
                "region"
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                row["fecha"],
                row["Nivel general"],
                row["Alimentos y bebidas no alcohólicas"],
                row["Alimentos"],
                row["Pan y cereales"],
                row["Carnes y derivados"],
                row["Leche, productos lácteos y huevos"],
                row["Aceites, grasas y manteca"],
                row["Frutas"],
                row["Verduras, tubérculos y legumbres"],
                row["Azúcar, dulces, chocolate, golosinas, etc."],
                row["Bebidas no alcohólicas"],
                row["Café, té, yerba y cacao"],
                row["Bebidas alcohólicas"],
                row["Tabaco"],
                row["Prendas de vestir y calzado"],
                row["Prendas de vestir y materiales"],
                row["Calzado"],
                row["Vivienda, agua, electricidad, gas y otros combustibles"],
                row["Alquiler de la vivienda y gastos conexos"],
                row["Alquiler de la vivienda"],
                row["Mantenimiento y reparación de la vivienda"],
                row["Electricidad, gas y otros combustibles"],
                row["Equipamiento y mantenimiento del hogar"],
                row["Bienes y servicios para la conservación del hogar"],
                row["Salud"],
                row["Productos medicinales, artefactos y equipos para la salud"],
                row["Gastos de prepagas"],
                row["Transporte"],
                row["Adquisición de vehículos"],
                row["Funcionamiento de equipos de transporte personal"],
                row["Combustibles y lubricantes para vehículos de uso del hogar"],
                row["Transporte público"],
                row["Comunicación"],
                row["Servicios  de telefonía e internet"],
                row["Recreación y cultura"],
                row[
                    "Equipos audiovisuales, fotográficos y de procesamiento de la información"
                ],
                row["Servicios recreativos y culturales"],
                row["Periódicos, diarios, revistas, libros y artículos de papelería"],
                row["Educación"],
                row["Restaurantes y hoteles"],
                row["Restaurantes y comidas fuera del hogar"],
                row["Bienes y servicios varios"],
                row["Cuidado personal"],
                row["region"],
            )

            contador += 1
            print(f"Se insertaron {contador} filas")

    print(f"Tabla Region GBA actualizada, se insertaron {contador} filas")
    conn.close()


def update_regiones(dict):

    """
    Toma un marco de datos, itera sobre él e inserta cada fila en una tabla de PostgreSQL.
    """
    for key, region in dict.items():

        contador = 0
        if key != "Region GBA":

            for index, row in region.iterrows():

                data = db.execute(
                    f'SELECT fecha FROM "{key}" where fecha = %s',
                    row["fecha"],
                )
                data2 = data.fetchone()

                if data2 == None:
                    print(f"Actualizando tabla {key}")

                    db.execute(
                        f"""INSERT INTO "{key}" (
                        "fecha",
                        "Nivel general",
                        "Alimentos y bebidas no alcohólicas",
                        "Alimentos",
                        "Pan y cereales",
                        "Carnes y derivados",
                        "Leche, productos lácteos y huevos",
                        "Aceites, grasas y manteca",
                        "Frutas",
                        "Verduras, tubérculos y legumbres",
                        "Azúcar, dulces, chocolate, golosinas, etc.",
                        "Bebidas no alcohólicas",
                        "Café, té, yerba y cacao",
                        "Bebidas alcohólicas",
                        "Tabaco",
                        "Prendas de vestir y calzado",
                        "Prendas de vestir y materiales",
                        "Calzado",
                        "Vivienda, agua, electricidad, gas y otros combustibles",
                        "Alquiler de la vivienda y gastos conexos",
                        "Alquiler de la vivienda",
                        "Electricidad, gas y otros combustibles",
                        "Equipamiento y mantenimiento del hogar",
                        "Bienes y servicios para la conservación del hogar",
                        "Salud",
                        "Productos medicinales, artefactos y equipos para la salud",
                        "Gastos de prepagas",
                        "Transporte",
                        "Adquisición de vehículos",
                        "Funcionamiento de equipos de transporte personal",
                        "Combustibles y lubricantes para vehículos de uso del hogar",
                        "Transporte público",
                        "Comunicación",
                        "Servicios  de telefonía e internet",
                        "Recreación y cultura",
                        "Servicios recreativos y culturales",
                        "Periódicos, diarios, revistas, libros y artículos de papelería",
                        "Educación",
                        "Restaurantes y hoteles",
                        "Restaurantes y comidas fuera del hogar",
                        "Bienes y servicios varios",
                        "Cuidado personal",
                        "region"
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        row["fecha"],
                        row["Nivel general"],
                        row["Alimentos y bebidas no alcohólicas"],
                        row["Alimentos"],
                        row["Pan y cereales"],
                        row["Carnes y derivados"],
                        row["Leche, productos lácteos y huevos"],
                        row["Aceites, grasas y manteca"],
                        row["Frutas"],
                        row["Verduras, tubérculos y legumbres"],
                        row["Azúcar, dulces, chocolate, golosinas, etc."],
                        row["Bebidas no alcohólicas"],
                        row["Café, té, yerba y cacao"],
                        row["Bebidas alcohólicas"],
                        row["Tabaco"],
                        row["Prendas de vestir y calzado"],
                        row["Prendas de vestir y materiales"],
                        row["Calzado"],
                        row["Vivienda, agua, electricidad, gas y otros combustibles"],
                        row["Alquiler de la vivienda y gastos conexos"],
                        row["Alquiler de la vivienda"],
                        row["Electricidad, gas y otros combustibles"],
                        row["Equipamiento y mantenimiento del hogar"],
                        row["Bienes y servicios para la conservación del hogar"],
                        row["Salud"],
                        row[
                            "Productos medicinales, artefactos y equipos para la salud"
                        ],
                        row["Gastos de prepagas"],
                        row["Transporte"],
                        row["Adquisición de vehículos"],
                        row["Funcionamiento de equipos de transporte personal"],
                        row[
                            "Combustibles y lubricantes para vehículos de uso del hogar"
                        ],
                        row["Transporte público"],
                        row["Comunicación"],
                        row["Servicios  de telefonía e internet"],
                        row["Recreación y cultura"],
                        row["Servicios recreativos y culturales"],
                        row[
                            "Periódicos, diarios, revistas, libros y artículos de papelería"
                        ],
                        row["Educación"],
                        row["Restaurantes y hoteles"],
                        row["Restaurantes y comidas fuera del hogar"],
                        row["Bienes y servicios varios"],
                        row["Cuidado personal"],
                        row["region"],
                    )

                    contador += 1
                    print(f"Se insertaron {contador} filas")
                # else:
                #     print(f"fila {index+1} de {len(region)} ya existe")
            print(f"Tabla {key} actualizada, se insertaron {contador} filas")
    conn.close()


if __name__ == "__main__":
    crear_tabla_regiones()
    crear_tabla_region(dict_df_regiones)
    update_region_gba()
    update_regiones(dict_df_regiones)
