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

- cargar las credenciales de la base de datos en el archivo `credenciales_bbdd.py`
- Ejecutar etl.py

## Resultados

- Normalizacion de datos obtenidos de los CSV
- Guardado de datos en una base de datos Postgresql

## A futuro

- Poder obtener los datos de una API de existir o mediante WEB scraping

## Contacto

- [Linkedin](https://www.linkedin.com/in/iber-ismael-piovani-8b35bbba/)
- [Twitter](https://twitter.com/laimas)
- [Github](https://github.com/Vosinepi)
