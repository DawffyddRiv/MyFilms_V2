#from Extraccion import ExtractorDatos
import logging
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

class Transformador:
    """
    Clase encargada EXCLUSIVAMENTE de la transformación y validación de datos de clima.
    recibe una ruta de un archivo parquet y lo transforma.
    """
    def __init__(self,spark,ruta_parquet):
        self.spark = spark
        self.ruta_parq=ruta_parquet

    def cargar_datos(self):
        self.df=spark.read.parquet(self.ruta_parq)
        self.df.show(5)
    


if __name__ == "__main__":    
    
    print("De nuevo please")
    spark = (
        SparkSession.builder
        .appName("TransformacionPeliculas")
        .getOrCreate()
    )
    
    #transformador = Transformador(spark,"data/raw/peliculas.parquet")

    #transformador.cargar_datos()
    #Vamos a practicar spark
    pequeñoschema=StructType([
    StructField("id_pedido", StringType(), nullable=True), # nullable=es nulo?
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("precio", FloatType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("ciudad", StringType(), True)
    ])

    datos=[
    ("O-101", "Laptop", "Electrónica", 1500.00, 2, "CDMX"),
    ("O-102", "Mouse", "Electrónica", 25.50, 10, "Monterrey"),
    ("O-103", "Cafetera", "Hogar", 80.00, 0, "Guadalajara"),
    ("O-104", "Monitor", "Electrónica", 300.00, 4, "CDMX"),
    ("O-105", "Silla Oficina", "Hogar", 120.00, 3, "Monterrey"),
    ("O-106", "Teclado", "Electrónica", 45.00, 0, "CDMX"),
    ("O-107","Pantalón","Ropa",400.0,10,None),
    ("O-108", "Vestido","Ropa",1000.0,5,None)
    ]
    prueba=spark.createDataFrame(datos,schema=pequeñoschema)
    #Comentario

    df=prueba.filter((prueba.categoria=='Electrónica') & (prueba.precio>150))
    #Agregar una columna calculada
    df1=prueba.withColumn("Total_venta", prueba.precio*prueba.cantidad)
    
    df1=df1.na.fill(value="Sin Ciudad",subset=['ciudad'])
    df_agrupado=prueba.groupby('categoria').agg(F.avg('precio').alias('precioProm'),F.sum('precio').alias('SumaTotal'))
    df_agrupado.show()
    df_filtrado=prueba.filter(prueba.precio>300)
    #df_filtrado.show()
    #df1.show()
    #prueba.printSchema()

    data_3 = [
    ("Laptop", "Electrónica", 1500.00, "2026-01-01"),
    ("Laptop", "Electrónica", 1200.00, "2026-01-01"),
    ("Laptop", "Electrónica", 1450.00, "2026-01-02"),
    ("Laptop", "Electrónica", 1600.00, "2026-01-03"),
    ("Mouse", "Electrónica", 25.50, "2026-01-01"),
    ("Mouse", "Electrónica", 27.00, "2026-01-02"),
    ("Silla", "Hogar", 120.00, "2026-01-01"),
    ("Silla", "Hogar", 115.00, "2026-01-02")
]

    schema_3 = StructType([
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("precio", FloatType(), True),
    StructField("fecha", StringType(), True)
    ])

    df_historico = spark.createDataFrame(data_3, schema_3)
    #Aqui vamos a hacer una funcion de ventana
    ventana_producto_fecha=Window.partitionBy('producto').orderBy('fecha')
    aplicando_ventana=df_historico.withColumn('PrecioMaximo',F.max('precio').over(ventana_producto_fecha))
    #aplicando_ventana.show()
    #transformador.limpiar()

    data_categorias = [
    ("Electrónica", "Departamento de Tecnología"),
    ("Hogar", "Departamento de Muebles y Cocina")
    ]
    schema_cat = ["categoria", "nombre_departamento"]
    df_cats = spark.createDataFrame(data_categorias, schema_cat)
    interseccion=df_historico.join(broadcast(df_cats),on='categoria',how='inner')
    interseccion.show(truncate=False)
    
    spark.stop()