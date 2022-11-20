import wget
import pandas as pd
import os

from variables import url, fecha, lista_regiones, ipc_aperturas

# descarga el archivo de la url


def descargar_archivo(url):
    """
    Si el directorio está vacío, descargue el archivo. Si el directorio no está vacío, compruebe si el
    archivo existe. Si lo hace, no hagas nada. Si no es así, descargue el archivo.

    :param url: La URL del archivo que desea descargar
    """
    try:
        if os.path.isfile(rf"../Data_cruda/sh_ipc_aperturas_{fecha}.xls"):
            print("El archivo ya existe")
        else:
            response = wget.download(
                url, rf"../Data_cruda/sh_ipc_aperturas_{fecha}.xls"
            )
            print(f"El archivo se descargó correctamente en {response}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    descargar_archivo(url)

    print("proceso terminados")
