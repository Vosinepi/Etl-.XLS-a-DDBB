import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from tabulate import tabulate


pd.set_option("display.max_columns", None)


from variables import fecha
from cfg import postgresql_config

db = create_engine(postgresql_config)
conn = db.connect()


def traer_interanual(region):
    """
    Trae la tabla interanual de la base de datos
    """

    query = f"""
    SELECT * FROM indice_aperturas 
    WHERE date_part('month', fecha) = date_part('month', CURRENT_DATE) -1
    and region = '{region}'
    ORDER BY fecha DESC LIMIT 2;
    """
    df = pd.read_sql_query(query, con=db)

    return df


def print_interanual(region, top):
    df_interanual = traer_interanual(region)
    df_interanual.head()
    for column in df_interanual.columns[2::]:
        if column != "region":
            df_interanual[column] = df_interanual[column].astype(float)

    for column in df_interanual.columns[2::]:
        if column != "region":
            df_interanual[column] = (
                (df_interanual[column][0] - df_interanual[column][1])
                / df_interanual[column][1]
            ) * 100

    fecha = (
        f'{df_interanual["fecha"][0].strftime("%B")} {df_interanual["fecha"][0].year}'
    )
    df_interanual = df_interanual.dropna(axis=1)
    df_interanual = df_interanual.drop(columns=["fecha", "index", "region"])
    df_interanual_final = df_interanual
    df_interanual_final.iloc[0, :]
    df = pd.DataFrame(df_interanual_final.iloc[0, :])
    df = df.reset_index()
    df = df.rename(columns={"index": "Indice", 0: "Variacion Porcentual"})

    print(f"Top {top} Indice Interanual de {fecha} para la {region} ")

    df2 = df.nlargest(top, ["Variacion Porcentual"])
    print(tabulate(df2, headers="keys", tablefmt="psql", showindex=False))


def main():
    try:
        print_interanual(
            region=str(input("Ingrese la region: ")), top=int(input("Ingrese el top: "))
        )
    except:
        print("Error en la region")


if __name__ == "__main__":

    main()
