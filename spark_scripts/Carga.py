import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import Row


class CargaDatos:
    
    def __init__(self,spark: SparkSession,cadenaConexion, usuario,password):
        self.spark = spark
        self.cadenaConexion=cadenaConexion
        self.usuario=usuario
        self.password=password
        #self.motor=create_engine(cadenaConexion)

    def test_conexion(self):
        try:
            (
                self.spark.read
                .format("jdbc")
                .option("url", self.cadenaConexion)
                .option("dbtable", "(SELECT 1) AS prueba")
                .option("user", self.usuario)
                .option("password", self.password)
                .option("driver", "org.postgresql.Driver")
                .load()
                .show()
            )

            logging.info("Conexión exitosa a postgres.")

        except Exception as e:

            logging.error(f"Error al conectar con postgres: {e}")

            raise


    def cargar_a_sql(self,df:DataFrame,nombre_tabla:str,modo:str= "overwrite"): #modo='overwrite'
        if df.rdd.isEmpty():
            print("El DataFrame está vacío. No se cargó nada.")
            logging.warning("El DataFrame está vacío. No se realizará la carga.")
            return
        if modo not in ["overwrite", "append"]:
            print(f"El modo debe ser 'overwrite' o 'append")
            raise ValueError("El modo debe ser 'overwrite' o 'append'.")
            
    
        try:

            (
                df.write
                .format("jdbc")
                .option("url", self.cadenaConexion)
                .option("dbtable", nombre_tabla)
                .option("user", self.usuario)
                .option("password", self.password)
                .option("driver", "org.postgresql.Driver")
                .mode(modo)
                .save()
            )

            logging.info(f"Datos cargados correctamente en '{nombre_tabla}'.")

        except Exception as e:

            logging.error(f"Error al cargar datos en PostgreSQL: {e}")

            raise

if __name__ == "__main__":    
    logging.basicConfig(level=logging.INFO)
    print("fase de carga")
    spark = (
        SparkSession.builder
        .appName("CargaPeliculas")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    load_dotenv(dotenv_path=Path("/opt/spark/.env"))
    print("HOST:", os.getenv("POSTGRES_HOST"))
  


    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    print("HOST:", POSTGRES_HOST)
    print("PORT:", POSTGRES_PORT)
    print("DB:", POSTGRES_DB)
    print("USER:", POSTGRES_USER)  
    logging.info("Configuración de PostgreSQL cargada.")  


    CADENA_CONEXION = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}") 

    cargador = CargaDatos(spark, CADENA_CONEXION, POSTGRES_USER, POSTGRES_PASSWORD)
    cargador.test_conexion()

# Mock DataFrame
    datos_mock = [
    Row(id=1, titulo="Matrix", anio=1999),
    Row(id=2, titulo="Inception", anio=2010),
    Row(id=3, titulo="Interstellar", anio=2014),
    ]   
    df_mock = spark.createDataFrame(datos_mock)

    cargador.cargar_a_sql(df_mock, "peliculas_mock", modo="overwrite")



    spark.stop()
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