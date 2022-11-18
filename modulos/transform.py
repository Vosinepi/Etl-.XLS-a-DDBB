import wget
import pandas as pd
import os

from variables import url, fecha, lista_regiones, ipc_aperturas


# Lee el archivo descargado y lo guarda en un dataframe

regiones = {}  # diccionario con regiones y sus indices


def extract(file):
    """
    Lee el archivo descargado y lo guarda en un dataframe

    Crea diccionario con regiones y sus indices
    :param file: El archivo que desea leer
    """
    global df
    df = pd.read_excel(
        file, sheet_name="Índices aperturas", skiprows=4, parse_dates=True
    )

    # Elimina las filas con valores nulos

    df = df.dropna()
    df.columns = ["Columna_" + str(i) for i in range(1, len(df.columns) + 1)]
    df = df.reset_index(drop=True)

    # crear dict con las regiones

    for row in df.Columna_1:
        if "Región" in row:
            regiones[row] = df[df.Columna_1 == row].index[0]


# creamos los DataFrames por regiones


def transform_df_regiones(regiones, df):
    """
    Crea un DataFrame por región.

    :param region: La región que desea crear
    """
    region_gba = df[regiones["Región GBA"] : regiones["Región Pampeana"]]
    region_gba = region_gba.transpose()
    region_gba.columns = region_gba.iloc[0]
    region_gba = region_gba.drop(region_gba.index[0])
    region_gba = region_gba.reset_index(drop=True)
    region_gba.rename(columns={"Región GBA": "fecha"}, inplace=True)
    region_gba["fecha"] = pd.to_datetime(region_gba["fecha"], format="%Y-%m-%d").apply(
        lambda x: x.strftime("%Y-%m")
    )
    region_gba["region"] = "Region GBA"

    region_gba.to_csv(rf"../Dataframe/region_gba_{fecha}.csv", index=False)

    region_pampeana = df[regiones["Región Pampeana"] : regiones["Región Noroeste"]]
    region_pampeana = region_pampeana.transpose()
    region_pampeana.columns = region_pampeana.iloc[0]
    region_pampeana = region_pampeana.drop(region_pampeana.index[0])
    region_pampeana = region_pampeana.reset_index(drop=True)
    region_pampeana.rename(columns={"Región Pampeana": "fecha"}, inplace=True)
    region_pampeana["fecha"] = pd.to_datetime(
        region_pampeana["fecha"], format="%Y-%m-%d"
    ).apply(lambda x: x.strftime("%Y-%m"))
    region_pampeana["region"] = "Region Pampeana"

    region_pampeana.to_csv(rf"../Dataframe/region_pampeana_{fecha}.csv", index=False)

    region_noroeste = df[regiones["Región Noroeste"] : regiones["Región Noreste"]]
    region_noroeste = region_noroeste.transpose()
    region_noroeste.columns = region_noroeste.iloc[0]
    region_noroeste = region_noroeste.drop(region_noroeste.index[0])
    region_noroeste = region_noroeste.reset_index(drop=True)
    region_noroeste.rename(columns={"Región Noroeste": "fecha"}, inplace=True)
    region_noroeste["fecha"] = pd.to_datetime(
        region_noroeste["fecha"], format="%Y-%m-%d"
    ).apply(lambda x: x.strftime("%Y-%m"))
    region_noroeste["region"] = "Region Noroeste"

    region_noroeste.to_csv(rf"../Dataframe/region_noroeste_{fecha}.csv", index=False)

    region_noreste = df[regiones["Región Noreste"] : regiones["Región Cuyo"]]
    region_noreste = region_noreste.transpose()
    region_noreste.columns = region_noreste.iloc[0]
    region_noreste = region_noreste.drop(region_noreste.index[0])
    region_noreste = region_noreste.reset_index(drop=True)
    region_noreste.rename(columns={"Región Noreste": "fecha"}, inplace=True)
    region_noreste["fecha"] = pd.to_datetime(
        region_noreste["fecha"], format="%Y-%m-%d"
    ).apply(lambda x: x.strftime("%Y-%m"))
    region_noreste["region"] = "Region Noreste"

    region_noreste.to_csv(rf"../Dataframe/region_noreste_{fecha}.csv", index=False)

    region_cuyo = df[regiones["Región Cuyo"] : regiones["Región Patagonia"]]
    region_cuyo = region_cuyo.transpose()
    region_cuyo.columns = region_cuyo.iloc[0]
    region_cuyo = region_cuyo.drop(region_cuyo.index[0])
    region_cuyo = region_cuyo.reset_index(drop=True)
    region_cuyo.rename(columns={"Región Cuyo": "fecha"}, inplace=True)
    region_cuyo["fecha"] = pd.to_datetime(
        region_cuyo["fecha"], format="%Y-%m-%d"
    ).apply(lambda x: x.strftime("%Y-%m"))
    region_cuyo["region"] = "Region Cuyo"

    region_cuyo.to_csv(rf"../Dataframe/region_cuyo_{fecha}.csv", index=False)

    region_patagonica = df[regiones["Región Patagonia"] :]
    region_patagonica = region_patagonica.transpose()
    region_patagonica.columns = region_patagonica.iloc[0]
    region_patagonica = region_patagonica.drop(region_patagonica.index[0])
    region_patagonica = region_patagonica.reset_index(drop=True)
    region_patagonica.rename(columns={"Región Patagonia": "fecha"}, inplace=True)
    region_patagonica["fecha"] = pd.to_datetime(
        region_patagonica["fecha"], format="%Y-%m-%d"
    ).apply(lambda x: x.strftime("%Y-%m"))
    region_patagonica["region"] = "Region Patagonia"

    region_patagonica.to_csv(
        rf"../Dataframe/region_patagonica_{fecha}.csv", index=False
    )


if __name__ == "__main__":
    extract(ipc_aperturas)
    transform_df_regiones(regiones, df)
    print("Archivo transformado")
    print("CSVs de regiones creados\n", *lista_regiones, sep="\n")
    print("procesos terminados")
