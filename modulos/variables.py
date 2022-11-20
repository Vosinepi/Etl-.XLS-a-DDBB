import datetime as dt
import pandas as pd

nombre_base_datos = "indice_aperturas"

url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls"

fecha = dt.datetime.now().strftime("%Y-%m-%d")

# path al archivo
ipc_aperturas = rf"../Data_cruda/sh_ipc_aperturas_{fecha}.xls"

# paths a los archivos que se subirán al bucket y a la base de datos
region_gba = rf"../Dataframe/region_gba_{fecha}.csv"
region_pampeana = rf"../Dataframe/region_pampeana_{fecha}.csv"
region_noroeste = rf"../Dataframe/region_noroeste_{fecha}.csv"
region_noreste = rf"../Dataframe/region_noreste_{fecha}.csv"
region_cuyo = rf"../Dataframe/region_cuyo_{fecha}.csv"
region_patagonica = rf"../Dataframe/region_patagonica_{fecha}.csv"

# path al csc indice_aperturas

indice_aperturas = rf"../Dataframe/indice_aperturas_{fecha}.csv"

datasets = [
    region_gba,
    region_pampeana,
    region_noroeste,
    region_noreste,
    region_cuyo,
    region_patagonica,
    ipc_aperturas,
    indice_aperturas,
]

lista_regiones = [
    region_gba,
    region_pampeana,
    region_noroeste,
    region_noreste,
    region_cuyo,
    region_patagonica,
]
