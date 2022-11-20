import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

pd.set_option("display.max_columns", None)


from variables import fecha, indice_aperturas
from cfg import postgresql_config


# conectar a base de datos

db = create_engine(postgresql_config)
conn = db.connect()

df_indice_aperturas = pd.read_csv(indice_aperturas)
# crear / update tabla


def crear_tabla_indices(df_indice_aperturas):
    """
    Crea una tabla en una base de datos si no existe, y si existe, imprime el número de filas en la
    tabla

    :param indice_aperturas: un CSV con los datos que se insertarán en la tabla
    """

    try:

        db.execute(f"SELECT * FROM indice_aperturas")
        print(
            f"Tabla indice_aperturas ya existe con: ",
            db.execute(f"SELECT * FROM indice_aperturas").rowcount,
            " filas",
        )

    except sqlalchemy.exc.ProgrammingError:
        df_indice_aperturas.to_sql("indice_aperturas", db, if_exists="replace")
        print(
            "Tabla indice_apertura creada con: ",
            db.execute("SELECT * FROM indice_aperturas").rowcount,
            " filas",
        )

        db.execute(
            f"ALTER TABLE indice_aperturas ALTER COLUMN fecha TYPE DATE USING to_date(fecha, 'YYYY-MM')"
        )
    print("Tabla creadas\n")
    conn.close()


def update_indice_aperturas(df_indice_aperturas):
    """
    Toma el CSV de indices actualizado, itera sobre cada fila e inserta la fila en una tabla en una base de datos
    PostgreSQL
    """

    contador = 0
    for index, row in df_indice_aperturas.iterrows():

        data = db.execute(
            f"SELECT fecha FROM indice_aperturas where fecha = %s", row["fecha"]
        )
        data2 = data.fetchone()

        if data2 == None:
            print(f"Actualizando tabla Region GBA")

            db.execute(
                """INSERT INTO "Region GBA" (
                "fecha",
                "nivel_general",
                "alimentos_y_bebidas_no_alcohólicas",
                "alimentos",
                "pan_y_cereales",
                "carnes_y_derivados",
                "leche_productos_lácteos_y_huevos",
                "aceites_grasas_y_manteca",
                "frutas",
                "verduras_tubérculos_y_legumbres",
                "azúcar_dulces_chocolate_golosinas_etc.",
                "bebidas_no_alcohólicas",
                "café_té_yerba_y_cacao",
                "bebidas_alcohólicas",
                "tabaco",
                "prendas_de_vestir_y_calzado",
                "prendas_de_vestir_y_materiales",
                "calzado",
                "vivienda_agua_electricidad_gas_y_otros_combustibles",
                "alquiler_de_la_vivienda_y_gastos_conexos",
                "alquiler_de_la_vivienda",
                "mantenimiento_y_reparación_de_la_vivienda",
                "electricidad_gas_y_otros_combustibles",
                "equipamiento_y_mantenimiento_del_hogar",
                "bienes_y_servicios_para_la_conservación_del_hogar",
                "salud",
                "productos_medicinales_artefactos_y_equipos_para_la_salud",
                "gastos_de_prepagas",
                "transporte",
                "adquisición_de_vehículos",
                "funcionamiento_de_equipos_de_transporte_personal",
                "combustibles_y_lubricantes_para_vehículos_de_uso_del_hogar",
                "transporte_público",
                "comunicación",
                "servicios_de_telefonía_e_internet",
                "recreación_y_cultura",
                "equipos_audiovisuales_fotográficos_y_de_procesamiento_de_la_información",
                "servicios_recreativos_y_culturales",
                "periódicos_diarios_revistas_libros_y_artículos_de_papelería",
                "educación",
                "restaurantes_y_hoteles",
                "restaurantes_y_comidas_fuera_del_hogar",
                "Bienes_y_servicios_varios",
                "cuidado_personal",
                "region"
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                row["fecha"],
                row["nivel_general"],
                row["alimentos_y_bebidas_no_alcohólicas"],
                row["alimentos"],
                row["pan_y_cereales"],
                row["carnes_y_derivados"],
                row["leche_productos_lácteos_y_huevos"],
                row["aceites_grasas_y_manteca"],
                row["frutas"],
                row["verduras_tubérculos_y_legumbres"],
                row["azúcar_dulces_chocolate_golosinas_etc."],
                row["bebidas_no_alcohólicas"],
                row["café_té_yerba_y_cacao"],
                row["bebidas_alcohólicas"],
                row["tabaco"],
                row["prendas_de_vestir_y_calzado"],
                row["prendas_de_vestir_y_materiales"],
                row["calzado"],
                row["vivienda_agua_electricidad_gas_y_otros_combustibles"],
                row["alquiler_de_la_vivienda_y_gastos_conexos"],
                row["alquiler_de_la_vivienda"],
                row["mantenimiento_y_reparación_de_la_vivienda"],
                row["electricidad_gas_y_otros_combustibles"],
                row["equipamiento_y_mantenimiento_del_hogar"],
                row["bienes_y_servicios_para_la_conservación_del_hogar"],
                row["salud"],
                row["productos_medicinales_artefactos_y_equipos_para_la_salud"],
                row["gastos_de_prepagas"],
                row["transporte"],
                row["adquisición_de_vehículos"],
                row["funcionamiento_de_equipos_de_transporte_personal"],
                row["combustibles_y_lubricantes_para_vehículos_de_uso_del_hogar"],
                row["transporte_público"],
                row["comunicación"],
                row["servicios_de_telefonía_e_internet"],
                row["recreación_y_cultura"],
                row[
                    "equipos_audiovisuales_fotográficos_y_de_procesamiento_de_la_información"
                ],
                row["servicios_recreativos_y_culturales"],
                row["periódicos_diarios_revistas_libros_y_artículos_de_papelería"],
                row["educación"],
                row["restaurantes_y_hoteles"],
                row["restaurantes_y_comidas_fuera_del_hogar"],
                row["bienes_y_servicios_varios"],
                row["cuidado_personal"],
                row["region"],
            )

            contador += 1
            print(f"Se insertaron {contador} filas")

    print(f"Tabla Indices Aperturas actualizada, se insertaron {contador} filas")
    conn.close()


if __name__ == "__main__":

    crear_tabla_indices(df_indice_aperturas)
    update_indice_aperturas(df_indice_aperturas)
    print(f"Proceso finalizado")
