#from Extraccion import ExtractorDatos
import logging
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import explode
from pyspark.sql.functions import size
from pyspark.sql.functions import expr

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
        
        self.df = self.spark.read.parquet(self.ruta_parq)
        
        #def explorar_datos(self):

        #self.df=(
        #    spark.read
        #   .format("parquet")
        #    .schema(schema_a)
        #    .load(self.ruta_parq))
        #self.df.show(5)
        self.df.printSchema()
        

        self.df.select(explode("Ratings").alias("rating")) \
            .select("rating.Source") \
            .distinct() \
            .show(truncate=False)

        self.df.select("Title", "Ratings").show(10, truncate=False)
        self.df.groupBy(size("Ratings").alias("num_ratings")) \
       .count() \
       .show()
        self.df=( #Filtra por x.Source y devúelveme el valor asociado a ese Source
            self.df.withColumn("Rotten",
            expr("filter(Ratings,x -> x.Source= 'Rotten Tomatoes')[0].Value")) 
            .withColumn("IMDB",
            expr("filter(Ratings,x -> x.Source= 'Internet Movie Database')[0].Value"))            
            .withColumn("Metacritic",
            expr("filter(Ratings,x -> x.Source= 'Metacritic')[0].Value"))
            .drop("Ratings")
        )    

        self.df.show()
        self.df.printSchema()#Ahora si ya tenemos un schema plano
        print("Analisis de nulos por columna")
        self.df.select([
            count(when(col(c).isNull(), c)).alias(c)
             for c in self.df.columns
             ]).show(vertical=True)
        #valores N/A
        print("Analisis para valores nulos")
        self.df.select([
            count(when(col(c)== "N/A",c)).alias(c)
            for c in self.df.columns
        ]).show(vertical=True)
        
        #print(self.df.schema)
    
    

if __name__ == "__main__":    
    
    print("De nuevo please")
    spark = (
        SparkSession.builder
        .appName("TransformacionPeliculas")
        .getOrCreate()
    )
    
    transformador = Transformador(spark,"/home/iqdav10/data-engineering/projects/proyecto_pelis/data/raw/peliculas.parquet")

    transformador.cargar_datos()
    

    # 1. SUB-ETAPA: LIMPIEZA 
    """ 
    def _limpiar_y_sanitizar(self, df: pd.DataFrame) -> pd.DataFrame:
        #Renombra columnas, estandariza tipos de datos y elimina duplicados.
        df_clean = df.copy()
        
        # Renombrado de columnas (mapeo explícito)
        columnas_mapeo = {
            "time": "fecha",
            "temperature_2m": "temperatura_c",
            "precipitation": "precipitacion_mm"
        }
        df_clean = df_clean.rename(columns=columnas_mapeo)
        
        # Casteo de tipos seguro con 'coerce' para evitar quiebres por texto corrupto
        df_clean["fecha"] = pd.to_datetime(df_clean['fecha'], errors='coerce')
        df_clean["temperatura_c"] = pd.to_numeric(df_clean["temperatura_c"], errors='coerce')
        df_clean["precipitacion_mm"] = pd.to_numeric(df_clean["precipitacion_mm"], errors='coerce')
        
        # Eliminación de duplicados en la llave lógica
        df_clean = df_clean.drop_duplicates(subset=["fecha"]).reset_index(drop=True)
        
        return df_clean

    
    # * NUEVA ETAPA: VALIDACIÓN DE CALIDAD DE DATOS (Data Quality)
    
    def _validar_calidad_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        
        #Identifica anomalías (nulos, negativos inválidos).
        #En lugar de solo imprimir, separa o limpia los datos.
        #
        # Métrica de nulos
        nulos_fecha = df["fecha"].isna().sum()
        nulos_temp = df["temperatura_c"].isna().sum()
        nulos_prec = df["precipitacion_mm"].isna().sum()
        
        # Métrica de reglas lógicas (la precipitación nunca puede ser menor a 0)
        prec_negativas = (df["precipitacion_mm"] < 0).sum()
        
        # Registro en logs de auditoría (es mejor que print para producción)
        logging.info(f"[QUALITY] Nulos detectados -> Fecha: {nulos_fecha}, Temp: {nulos_temp}, Precip: {nulos_prec}")
        if prec_negativas > 0:
            logging.warning(f"[QUALITY] Se detectaron {prec_negativas} registros con precipitación negativa.")

        # ESTRATEGIA DE MITIGACIÓN (Acción correctiva):
        # 1. Eliminamos filas sin fecha elemental
        df_valido = df.dropna(subset=["fecha"]).copy()
        
        # 2. Corregimos valores lógicamente imposibles (ej. precipitación negativa a 0.0)
        if prec_negativas > 0:
            df_valido.loc[df_valido["precipitacion_mm"] < 0, "precipitacion_mm"] = 0.0
            logging.info("[QUALITY] Valores de precipitación negativa corregidos a 0.0")
            
        # 3. Tratamiento de nulos en métricas: imputamos con la media o dejamos indicador
        df_valido["temperatura_c"] = df_valido["temperatura_c"].fillna(df_valido["temperatura_c"].mean())
        df_valido["precipitacion_mm"] = df_valido["precipitacion_mm"].fillna(0.0)

        return df_valido

    
    # 2. SUB-ETAPA: LÓGICA DE NEGOCIO
    
    def _aplicar_reglas_negocio(self, df: pd.DataFrame, hora_ini: int, hora_fin: int) -> pd.DataFrame:
        #Aplica filtros específicos y añade columnas calculadas/flags de negocio.
        df_business = df.copy()
        
        # Tu filtro original por rango de horas
        df_business = df_business[
            (df_business["fecha"].dt.hour >= hora_ini) & 
            (df_business["fecha"].dt.hour <= hora_fin)
        ]
        
        # Agregamos valor profesional: Columna calculada de auditoría / trazabilidad
        df_business["procesado_at"] = pd.Timestamp.now()
        
        # Ejemplo de regla de negocio: Flag de clima severo
        df_business["flag_lluvia_pesada"] = np.where(df_business["precipitacion_mm"] > 5.0, True, False)
        
        return df_business

    
    # 3. SUB-ETAPA: STRUCTURING / AGGREGATION
    
    def _estructurar_destino(self, df: pd.DataFrame) -> pd.DataFrame:
        #Garantiza el orden de columnas exacto que requiere el destino (Load).
        columnas_finales = [
            "fecha",
            "temperatura_c",
            "precipitacion_mm",
            "flag_lluvia_pesada",
            "procesado_at"
        ]
        # Nos aseguramos de que solo viajen estas columnas al destino
        return df[columnas_finales].reset_index(drop=True)

    
    # PIPELINE DE TRANSFORMACIÓN PRINCIPAL (Micro-Orquestador)
    
    def ejecutar_transformacion(self, df_raw: pd.DataFrame, hora_ini: int, hora_fin: int) -> pd.DataFrame:
        
        #Punto de entrada único del componente. Orquesta el flujo secuencial interno.
        
        try:
            logging.info("Iniciando proceso de transformación...")
            
            # Validación temprana de esquema (Fail-Fast)
            columnas_requeridas = {"time", "temperature_2m", "precipitation"}
            if not columnas_requeridas.issubset(df_raw.columns):
                faltantes = columnas_requeridas - set(df_raw.columns)
                raise KeyError(f"Columnas requeridas ausentes en el dataset de origen: {faltantes}")

            # Flujo secuencial de transformación
            df_limpio     = self._limpiar_y_sanitizar(df_raw)
            df_validado   = self._validar_calidad_datos(df_limpio)
            df_negocio    = self._aplicar_reglas_negocio(df_validado, hora_ini, hora_fin)
            df_final      = self._estructurar_destino(df_negocio)
            
            logging.info("Transformación y validación de datos completada con éxito.")
            return df_final
            
        except KeyError as e:
            logging.error(f"Fallo crítico por esquema de datos erróneo: {e}")
            raise
        except Exception as e:
            logging.error(f"Error inesperado en la fase de transformación: {e}")
            raise

         """