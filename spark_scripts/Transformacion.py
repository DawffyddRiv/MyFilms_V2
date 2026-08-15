#from Extraccion import ExtractorDatos
import logging
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, count, when,explode,size,expr,regexp_replace,split,to_date
from pyspark.sql import DataFrame

class Transformador:
    """
    Clase encargada EXCLUSIVAMENTE de la transformación y validación de datos de clima.
    recibe una ruta de un archivo parquet y lo transforma.
    """
    def __init__(self,spark,ruta_parquet):
        self.spark = spark
        self.ruta_parq=ruta_parquet

    def cargar_datos(self):
        """schema_a=StructType([
        StructField("Title",StringType(),True),
        StructField("Year",StringType(),True),
        StructField("Rated",StringType(),True),
        StructField("Released",StringType(),True),
        StructField("Runtime",StringType(),True),
        StructField("Genre",StringType(),True),
        StructField("Director",StringType(),True),
        StructField("Writer",StringType(),True),
        StructField("Actors",StringType(),True),
        StructField("Plot",StringType(),True),
        StructField("Language",StringType(),True),
        StructField("Country",StringType(),True),
        StructField("Awards",StringType(),True),
        StructField("Poster",StringType(),True),
        StructField("Ratings",StringType(),True),        
        StructField("Metascore",IntegerType(),True),
        StructField("imdbRating",DecimalType(10, 2),True),
        StructField("imdbVotes",IntegerType(),True),
        StructField("imdbID",StringType(),True),
        StructField("Type",StringType(),True),
        StructField("DVD",StringType(),True),
        StructField("BoxOffice",DecimalType(10, 2),True),
        StructField("Production",StringType(),True),
        StructField("Website",StringType(),True),
        StructField("Response",BooleanType(),True),
        StructField("totalSeasons",StringType(),True)
        ])              #Esto de aqui solo cuando sea json o csv"""

        #self.df=(
                #    spark.read
                #   .format("parquet")
                #    .schema(schema_a)
                #    .load(self.ruta_parq))
                #self.df.show(5)
        self.df = self.spark.read.parquet(self.ruta_parq)


    def validar_layout(self):
        columnas_esperadas = {
            "Title",
            "Year",
            "Rated",
            "Released",
            "Runtime",
            "Genre",
            "Director",
            "Writer",
            "Actors",
            "Plot",
            "Language",
            "Country",
            "Awards",
            "Poster",
            "Ratings",
            "Metascore",
            "imdbRating",
            "imdbVotes",
            "imdbID",
            "Type",
            "DVD",
            "BoxOffice",
            "Production",
            "Website",
            "Response",
            "totalSeasons"}

        columnas_actuales = set(self.df.columns)

        faltantes = columnas_esperadas - columnas_actuales
        adicionales = columnas_actuales - columnas_esperadas

        if faltantes:
            print(f"Columnas faltantes: {faltantes}")
            logging.error(f"Columnas faltantes: {faltantes}")
            raise ValueError("El layout de entrada no coincide con el esperado.") #Para detener el etl

        if adicionales:
            print(f"Columnas adicionales: {adicionales}")
            logging.warning(f"Columnas adicionales: {adicionales}")

    def mostrar_esquema(self):  
        logging.info("Mostrando schema")              
        self.df.printSchema() # tras la exploración se puede apreciar que en todos los campos se permiten valores nulos


    def analizar_anidada(self,columna,alias_columna): # Nos da los campos anidados en "Ratings"
        logging.info("Mostrando campos anidados en Ratings")     
        self.df.select(explode(col(columna)).alias(alias_columna)) \
        .select(f"{alias_columna}.Source").distinct().show(truncate=False)       

       #ESTAMOS REORDENANDO DESDE AQUI
        self.df.select("Title", "Ratings").show(10, truncate=False) #Esto se puede omitir
    def agrupa_un_campo(self,columna,alias_columna):      
        logging.info("Exploración de los campos")  
        self.df.groupBy(size(columna).alias(alias_columna)) \
        .count().show() #Aqui exploramos tres de los campos, sobre todo el campo Ratings

    def aplanar_ratings(self): #Filtra por x.Source y devúelveme el valor asociado a ese Source
        logging.info("Extrayenndo campos anidados dentro de Ratings")
        self.df = (self.df.withColumn("Rotten",
            expr("filter(Ratings, x -> x.Source = 'Rotten Tomatoes')[0].Value"))
        .withColumn("IMDB",
            expr("filter(Ratings, x -> x.Source = 'Internet Movie Database')[0].Value"))
        .withColumn("Metacritic",expr("filter(Ratings, x -> x.Source = 'Metacritic')[0].Value"))
        .drop("Ratings"))

    def analizar_nulos_na(self):
        #print("Analisis de nulos por columna")#null
        logging.info("Realizando análisis de nulos por columna")
        self.df.select([
            count(when(col(c).isNull(), c)).alias(c)
            for c in self.df.columns
            ]).show(vertical=True)
        logging.info("Análisis de nulos finalizado")
        #valores N/A
        #print("Analisis para valores N/A")
        logging.info("Realizando análisis de N/A por columna")
        self.df.select([
            count(when(col(c)== "N/A",c)).alias(c)
            for c in self.df.columns
        ]).show(vertical=True)
        logging.info("Análisis de N/A finalizado")
                #Aqui vamos
        #Aqui vamos a explorar si aparecen los nulos: 
        #print("Veamos si aparecen registros nulos en DVD y totalSeasons")
        #self.df.filter(self.df["DVD"].isNull()).show()
        #self.df.filter(self.df["totalSeasons"].isNull()).show(10)
    def estandarizar_a_nulos(self):
        #Convertimos los N/A a null
        logging.info("Convirtiendo N/A a nulos y estandarizando valores")
        #Aqui podríamos hacer una lista y despues meterla a subset pero lo dejamos para el futuro :v        
        self.df = self.df.replace("N/A", None, subset=["Awards","DVD","Metascore", "BoxOffice","Production","Website"])

        #Definiendo valores estandar para sustituir por nulos
        valores_estandar = {
        "Awards":"N/A",
        "Metascore":"0",
        "DVD": "N/A",                  
        "BoxOffice": "0",              
        "Production": "N/A",       
        "Website": "N/A",
        "totalSeasons": "0"  ,
        "Rotten": "0",
        "Metacritic": "0"                  
        }
        #Aplicamos los valores estandar al dataframe
        self.df = self.df.fillna(valores_estandar)    
        logging.info("Conversión y estandarización finalizada")


        #print("Va de nuez el análisis")

    def estandarizar_campos(self):

        #Comenzamos a definir el tipo de dato para el análisis posterior
        self.df=self.df.withColumn("Year",col("Year").cast("int"))
        self.df = self.df.withColumn("Released",to_date("Released", "dd MMM yyyy"))
        self.df = self.df.withColumn("Runtime",regexp_replace("Runtime", " min", ""))
        self.df=self.df.withColumn("Runtime",col("Runtime").cast("int"))
        self.df=self.df.withColumn("Metascore",col("Metascore").cast("int"))
        self.df=self.df.withColumn("imdbRating",col("imdbRating").cast("decimal(3,1)"))
        self.df = self.df.withColumn("imdbVotes",regexp_replace("imdbVotes", ",", ""))
        self.df=self.df.withColumn("imdbVotes",col("imdbVotes").cast("int"))
        self.df = self.df.withColumn("BoxOffice",regexp_replace("BoxOffice", "\\$", ""))
        self.df = self.df.withColumn("BoxOffice",regexp_replace("BoxOffice", ",", ""))
        self.df=self.df.withColumn("BoxOffice",col("BoxOffice").cast("int"))
        self.df = self.df.withColumn("Rotten",regexp_replace("Rotten", "%", ""))
        self.df=self.df.withColumn("Rotten",col("Rotten").cast("int"))
        self.df = self.df.withColumn("IMDB",split("IMDB", "/").getItem(0))
        self.df=self.df.withColumn("imdbRating",col("imdbRating").cast("decimal(3,1)"))
        self.df = self.df.withColumn("Metacritic",split("Metacritic", "/").getItem(0))
        self.df=self.df.withColumn("Metacritic",col("Metacritic").cast("decimal(3,1)"))

        #self.df.filter(self.df["Year"].isNull()).show()    
        #Para el caso de year cuando hay un periodo de tiempo, 
        # se opta por crear otra columna donde señalamos año de lanzamiento y así capturar solo el año de lanzamiento. En 
        # este caso omitiremos este proceso y dejaremos como valores nulos
    def validar_year(self):
        registros_invalidos = self.df.filter( (col("Year") < 1870) | (col("Year") > 2026) ).count()
        if registros_invalidos > 0:
            #print(f"Se encontraron {registros_invalidos} registros con Year inválido" )
            logging.warning( f"Se encontraron {registros_invalidos} registros con Year inválido" )
        else:
            logging.info("Validación de Year: OK")
    def validar_id(self):
        registros_invalidos = self.df.filter(col("imdbID").isNull()).count()

        if registros_invalidos > 0: 
            print("Se encontraron {registros_invalidos} películas sin imdbID")
            logging.warning(f"Se encontraron {registros_invalidos} películas sin imdbID")
        else:
            logging.info("Validación imdbID: OK")
    def validar_duplicados(self):
        duplicados = (self.df.groupBy("imdbID").count().filter(col("count") > 1).count())

        duplicados_imdb = self.df.groupBy("imdbID").count().filter(col("count") > 1).select("imdbID")

        self.df.groupBy("imdbID").count().filter(col("count") > 1).show()
        self.df.join(duplicados_imdb,on="imdbID",how="inner").show(truncate=False)


        if duplicados > 0:
            logging.warning(f"Se encontraron {duplicados} imdbID duplicados")
        else:
            logging.info("Validación de duplicados: OK")

    def dataframe_destino(self,ruta_salida,ruta_duplicados="/opt/airflow/data/processed/peliculas_duplicados"):
        columnas_destino=[
        "imdbID",
        "Title",
        "Year",
        "Released",
        "Runtime",
        "Genre",
        "Director",
        "Actors",
        "Language",
        "Country",
        "Type",
        "imdbRating",
        "imdbVotes",
        "Rotten",
        "Metacritic",
        "BoxOffice",
        "totalSeasons" ]

        df_destino = self.df.select(*columnas_destino)
        duplicados = df_destino.groupBy("imdbID").count().filter(col("count") > 1)

        
        if duplicados.count() > 0:
            logging.warning(f"Se encontraron {duplicados.count()} imdbID duplicados, guardando en archivo aparte...")
            # almacenamiento de duplicados
            df_duplicados = df_destino.join(duplicados.select("imdbID"), on="imdbID", how="inner")
            df_duplicados.write.mode("overwrite").parquet(ruta_duplicados)
            # Eliminación de duplicados para tener con valores unicos df_destino
            df_destino = df_destino.dropDuplicates(["imdbID"])
        self.df = df_destino
        self.df.write.mode("overwrite").parquet(ruta_salida)

if __name__ == "__main__":    
    
    print("Fase de transformación")
    spark = (
        SparkSession.builder
        .appName("cargaPeliculas")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    
    transformador = Transformador(spark,"/home/iqdav10/data-engineering/projects/proyecto_pelis/data/raw/peliculas.parquet")
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
    transformador.dataframe_destino("/opt/airflow/data/processed/peliculas2.parquet")

    spark.stop()
#Lo siguiente solo forma parte del template que tenemos como guia


    # 1. SUB-ETAPA: LIMPIEZA 
    """ 
    def _limpiar_y_sanitizar(self, df: pd.DataFrame) -> pd.DataFrame:
        #Renombra columnas, estandariza tipos de datos y elimina duplicados.
       
       

    
    # * NUEVA ETAPA: VALIDACIÓN DE CALIDAD DE DATOS (Data Quality)
    
    def _validar_calidad_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        
        #Identifica anomalías (nulos, negativos inválidos).
        
        # Métrica de nulos
        
        # Métrica de reglas lógicas (la precipitación nunca puede ser menor a 0)
        
        
        # Registro en logs de auditoría (es mejor que print para producción)
        
        # ESTRATEGIA DE MITIGACIÓN (Acción correctiva):
        
        # 2. Corregimos valores lógicamente imposibles (ej. precipitación negativa a 0.0)
            
        # 3. Tratamiento de nulos en métricas: imputamos con la media o dejamos indicador
        
    
    # 2. SUB-ETAPA: LÓGICA DE NEGOCIO
    
    
        
        # Agregamos valor profesional: Columna calculada de auditoría / trazabilidad
        
        
        # Ejemplo de regla de negocio: Flag de clima severo
        
        
        

    
    # 3. SUB-ETAPA: STRUCTURING / AGGREGATION
    
    def _estructurar_destino(self, df: pd.DataFrame) -> pd.DataFrame:
        #Garantiza el orden de columnas exacto que requiere el destino (Load).
    
        # Nos aseguramos de que solo viajen estas columnas al destino
    

    

         """