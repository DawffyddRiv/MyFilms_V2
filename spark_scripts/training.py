from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast,to_date
from datetime import date


if __name__ == "__main__":    
    spark=(
        SparkSession.builder
        .appName("Training")
        .getOrCreate()
    )

    """EJERCICIO 1:
    Obtén las 10 canciones más populares de los últimos 10 años, por nombre de la canción y
    artista."""
    datos=spark.read.option("header","True").option("quote", '"').option("escape", '"').csv("/home/iqdav10/data-engineering/projects/proyecto_pelis/data/raw/spotify-data.csv")  #Modificacion 1            
    datos=datos.withColumn("popularity",F.col("popularity").cast(IntegerType()))
    datos = datos.withColumn("duration_ms", F.col("duration_ms").cast(DoubleType()))#Modificacion 2
    datos=datos.withColumn("duracion_min", datos.duration_ms/60000)   
    datos=datos.withColumn("year",F.col("year").cast(IntegerType())) #Modificacion 3
    datos = datos.withColumn("artists", F.regexp_replace(F.col("artists"),r"\[|\]|'", "")) #Modificación 4         

    
    #ventana=Window.partitionBy('release_date').orderBy('duration_ms')
    diez=datos.filter(F.col("year")>=2016).sort(F.col("popularity").desc_nulls_last()).limit(10)    
    diez_populares=diez.select("name","artists", "year","popularity")
    #Respuesta
    diez_populares.show()
    #select top(10) duration from musica where fecha between 2016-01-01 And 2026-01-01 order by duration
    """
    EJERCICIO 2:
    Obtén las 10 canciones con mayor duración de los últimos 10 años, considerar la duración de
    la canción en minutos.
    """
    print("Diez con mayor duracion de los ultimos diez años")
    diez_mayor_duracion=datos.filter(datos.year>=2016).sort(F.col("duracion_min").desc_nulls_last()).limit(10)    
    diez_mayor_duracion.show()
    """ EJERCICIO 3:
    Obtén el número de canciones publicadas por artista y año de publicación.
    df_agrupado=prueba.groupby('categoria').agg(F.avg('precio').alias('precioProm'),F.sum('precio').alias('SumaTotal'))
    """
    print("Agrupado por artista y año")
    df_agrupado=datos.groupby("artists","year").agg(F.count("*").alias("Cantidad"))
    #df_agrupado=datos.groupby("artists","year").count()
    df_agrupado.show(10)
    
    """
    EJERCICIO 4:
    Obtén los artistas con mayor popularidad promedio por canción en los últimos 5 años.
    
    """
    print("Artistias con mayor popularidad promedio")
    #ventana=Window.partitionBy('artist').orderBy('popularity')

    #popularidad_promedio=datos.withColumn('PrecioMaximo',F.avg('precio').over(ventana_producto_fecha))
    popularidad_promedio=datos.groupby("artists").agg(F.avg("popularity").alias("pularidad_promedio"))
    popularidad_promedio.show(10)

    """
    EJERCICIO 5:
    Por cada año, obtén el top de los 5 artistas con mayor número de canciones, considera el
    artista separando la columna artists.
    """
    #datos = datos.withColumn("artists", F.regexp_replace(F.col("artists"),r"\[|\]|'", "")) #Modificación 4         
    #Aqui iria un split
    apariciones=datos.groupBy("year","artists").agg(F.count("*").alias("apariciones"))
    print("apariciones")
    apariciones.show(10)

    ventana=Window.partitionBy('year').orderBy(F.col('apariciones').desc())
    aplicando_ventana= apariciones.withColumn("ranking",F.row_number().over(ventana)).filter(F.col("ranking")<=5)
    
    print("Top 5 artistas con mayor numero de canciones")
    aplicando_ventana.show(10)
    #Aqui reconozco que se modificó el dataframe en el campo "year". Debí definir los campos al inicio p
    #para evitar estos inconvenientes
    spark.stop()