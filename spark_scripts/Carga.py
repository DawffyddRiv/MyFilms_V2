import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession




class CargaDatos:
    
    def __init__(self,cadenaConexion: str, db_config: dict):
        self.cadenaConexion=cadenaConexion
        
        #self.motor=create_engine(cadenaConexion)

    def test_conexion(self):
        try:
            with self.motor.connect() as conexion:
                resultado=conexion.execute(text("Select 1"))
                print("Conexion exitosa a Postgres")
        except SQLAlchemyError as e:
                print("Error al conectar a Postgres")
                print(e)
    def cargar_a_sql(self,df,nombre_tabla,modo='replace'):
        if df.empty:
            print("El DataFrame está vacío. No se cargó nada.")
            return
        if modo not in ['replace', 'append']:
            print(f"Modo '{modo}' no válido. Usa 'replace' o 'append'.")
            return
    
        try:
            df.to_sql(nombre_tabla,con=self.motor,if_exists=modo,index=False)
            print(f"Datos cargados en la tabla '{nombre_tabla}' usando modo '{modo}'")
        except SQLAlchemyError as e:
            print("Error al cargar datos a SQL Server:")
            print(e)

if __name__ == "__main__":    
    
    print("De nuevo please")
    spark = (
        SparkSession.builder
        .appName("TransformacionPeliculas")
        .getOrCreate()
    )



"""
    # Validación del dataframe
    

    def _validar_dataframe(self, df: pd.DataFrame):

        if df is None:
            raise ValueError("El DataFrame es None.")

        if df.empty:
            raise ValueError("El DataFrame está vacío.")

        logging.info(f"Registros a cargar: {len(df)}")

    
    # CARGA A CSV
    

    def guardar_csv(
        self,
        df: pd.DataFrame,
        ruta: str
    ):

        try:

            self._validar_dataframe(df)

            Path(ruta).parent.mkdir(
                parents=True,
                exist_ok=True
            )

            df.to_csv(
                ruta,
                index=False
            )

            logging.info(f"Archivo CSV guardado en {ruta}")

        except Exception as e:

            logging.error(f"Error al guardar CSV: {e}")

            raise

    
    # CARGA A PARQUET
    
    def guardar_parquet(
        self,
        df: pd.DataFrame,
        ruta: str
    ):

        try:

            self._validar_dataframe(df)

            Path(ruta).parent.mkdir(
                parents=True,
                exist_ok=True
            )

            df.to_parquet(
                ruta,
                index=False
            )

            logging.info(f"Archivo Parquet guardado en {ruta}")

        except Exception as e:

            logging.error(f"Error al guardar Parquet: {e}")

            raise

    
    # CARGA A POSTGRESQL
    

    def guardar_postgres(
        self,
        df: pd.DataFrame,
        conexion: str,
        tabla: str,
        if_exists="append"
    ):

        try:

            self._validar_dataframe(df)

            engine = create_engine(conexion)

            df.to_sql(
                tabla,
                engine,
                if_exists=if_exists,
                index=False
            )

            logging.info(
                f"Datos cargados correctamente en la tabla '{tabla}'."
            )

        except SQLAlchemyError as e:

            logging.error(f"Error de base de datos: {e}")

            raise

        except Exception as e:

            logging.error(f"Error inesperado: {e}")

            raise

    
    # Resumen de la carga
    

    def mostrar_resumen(self, df: pd.DataFrame):

        logging.info("==============")

        logging.info("Carga finalizada")

        logging.info(f"Total registros: {len(df)}")

        logging.info(f"Total columnas : {len(df.columns)}")

        logging.info("==============")
"""