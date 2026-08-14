from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql.functions import col,to_date

from datetime import datetime
import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

#from config.settings import OMDB_API_KEY

load_dotenv("/opt/airflow/.env")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

from spark_scripts.Extraccion import ExtractorDatos
from spark_scripts.Transformacion import Transformador
from spark_scripts.Carga import CargaDatos


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
}

# TAREA 1 - EXTRACCIÓN

def tarea_extraccion():
    logging.info("Iniciando extracción...")
    API_KEY=OMDB_API_KEY
    pelis=ExtractorDatos(API_KEY)#Esta es mi instancia principal
    palabrasClave = ["star", "love", "man", "dark"]#<- Esto es lo unico que se puede modificar por usuario
    nombrespeliculas=pelis.busqueda_peliculas(palabrasClave) 
    datosPeliculas=pelis.extrae_data_pelicula(nombrespeliculas)
    print(datosPeliculas)
    #pelis.guarda_parquet(datosPeliculas,"data/raw/peliculas.parquet")
    pelis.guarda_parquet(datosPeliculas,"/opt/airflow/data/raw/peliculas.parquet")

    logging.info("Extracción finalizada.")


# TAREA 2 - TRANSFORMACIÓN


def tarea_transformacion():

    logging.info("Iniciando transformación...")

    print("Fase de transformación")
    spark = (
        SparkSession.builder
        .appName("cargaPeliculas")        
        .getOrCreate()
    )
    
    #transformador = Transformador(spark,"/home/iqdav10/data-engineering/projects/proyecto_pelis/data/raw/peliculas.parquet")
    transformador = Transformador(spark,"/opt/airflow/data/raw/peliculas.parquet")
    
    #De acuerdo al template
    transformador.cargar_datos()
    #Validando el layout
    transformador.validar_layout()
    #Calidad de datos
    transformador.mostrar_esquema()
    transformador.analizar_anidada("Ratings", "rating")
    transformador.agrupa_un_campo("Ratings","id_ratings")
    
    transformador.aplanar_ratings()
    transformador.mostrar_esquema()#Muestra de nuevo el esquema ya plano
    transformador.analizar_nulos_na()
    #Mitigación
    transformador.estandarizar_a_nulos()
    transformador.analizar_nulos_na()
    transformador.estandarizar_campos()
    transformador.mostrar_esquema()
    transformador.analizar_nulos_na()
    #Validación de contexto
    transformador.validar_year()
    transformador.validar_id()
    transformador.validar_duplicados()
    #definicion del layout de salida
    transformador.dataframe_destino("/opt/airflow/data/processed/peliculas")

    spark.stop()
    logging.info("Transformación finalizada.")

# TAREA 3 - CARGA


def tarea_carga():
    logging.info("iniciando carga...")
    print("inicia fase de carga")
    spark = (
        SparkSession.builder
        .appName("CargaPeliculas")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") #.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")#.config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .getOrCreate()
        )
    
    load_dotenv("/opt/airflow/.env")   
    
    
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
  
    
    CADENA_CONEXION = (
        f"jdbc:postgresql://"
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}") 
    
    cargador = CargaDatos(spark, CADENA_CONEXION, POSTGRES_USER, POSTGRES_PASSWORD)
    cargador.test_conexion()
    
    df = spark.read.parquet("/opt/airflow/data/processed/peliculas")

    df2 = df.withColumn("imdbRating", col("imdbRating").cast("float")) \
        .withColumn("Metacritic", col("Metacritic").cast("float")) \
        .withColumn("Released", to_date(col("Released"), "yyyy-MM-dd"))

    cargador.cargar_a_sql(df2, "peliculas", modo="append")

    #cargador.cargar_a_sql(df2,"peliculas", modo="append")
    
    
    spark.stop()


# DEFINICIÓN DEL DAG


with DAG(

    dag_id="dag_etl_peliculasV2",

    description="ETL de películas utilizando OMDb, PySpark y PostgreSQL",

    start_date=datetime(2026, 8, 13),

    schedule_interval="@once",

    catchup=False,

    default_args=default_args,

    tags=["template","etl","airflow","spark"]

) as dag:

    extraccion = PythonOperator(
        task_id="extraccion",
        python_callable=tarea_extraccion
    )

    transformacion = PythonOperator(
        task_id="transformacion",
        python_callable=tarea_transformacion
    )

    carga = PythonOperator(
        task_id="carga",
        python_callable=tarea_carga
    )

    extraccion >> transformacion >> carga