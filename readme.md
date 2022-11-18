## Iber Ismael Piovani

# Pequeño ETL para guardar datos desde un archivo XLS online en una base de datos PostgreSQL

## Objetivo

Tomar datos de un XLS, normalizarlos, transformarlos y cargarlos en una base de datos Postgresql para su posterior utilizacion.
Tener la capacidad de actualizar los datos en la base de datos al surgir nuevas versiones del XLS.

## Requerimientos

- Python 3.8

- [Pandas](https://pandas.pydata.org/docs/)
- [Matplotlib](https://matplotlib.org/)
- [wget](https://pypi.org/project/wget/)
- [xlrd](https://pypi.org/project/xlrd/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [psycopg2](https://pypi.org/project/psycopg2/)
- [google.cloud.storage](https://pypi.org/project/google-cloud-storage/)
- [google-cloud-storage](https://pypi.org/project/google-cloud-storage/)

## Uso

- Clonar el repositorio

```
git clone
```

- Crear un entorno virtual

```
python -m venv venv
```

- correr Docker de SQL Server

```
docker run -d --name indice_aperturas_etl -v my_db:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=indice_aperturas postgres
```

test ddbb

```
docker exec -it indice_aperturas_etl psql -h localhost -U postgres -W indice_aperturas
```

- Instalar las dependencias

```
pip install -r requirements.txt
```

- cargar las credenciales de la base de datos en el archivo `cfg.py`
- correr descarga.py para descargar el archivo XLS

```
python descarga.py
```

- correr transform.py para transformar los datos y crear los csvs

```
python transform.py
```

- correr querys.py para cargar los datos en la base de datos

```
python querys.py
```

- correr store_bucket_GCP.py para cargar los datos en el bucket de GCP

```
python store_bucket_GCP.py
```

## Resultados

- El archivo descargado es guardado en la carpeta `Data` en estado crudo, solo se le agrego la fecha de descarga.
- Normalizacion de datos obtenidos, se crean CSVs por region para facilitar acceso a los datos. Se guardan en la carpeta `Dataframe`
- Guardado de datos en una base de datos Postgresql
- Subida de CSVs y .xls crudo a un bucket de GCP

## Contacto

- [Linkedin](https://www.linkedin.com/in/iber-ismael-piovani-8b35bbba/)
- [Twitter](https://twitter.com/laimas)
- [Github](https://github.com/Vosinepi)
