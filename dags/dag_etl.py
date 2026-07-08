from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

from spark_scripts.Extraccion import ExtractorDatos
from spark_scripts.Transformacion import Transformador
from spark_scripts.Cargador import CargaDatos


# CONFIGURACIÓN DEL DAG


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
}

# TAREA 1 - EXTRACCIÓN

def tarea_extraccion(**context):
    """
    Extrae información desde la fuente de datos.
    """

    logging.info("Iniciando extracción...")

    extractor = ExtractorDatos(paseAPI=None)

    resultado = extractor.busqueda(
        fecha_inicio="2026-06-01",
        fecha_fin="2026-06-07"
    )

    df = extractor.extrae_datos(resultado)

    context["ti"].xcom_push(
        key="datos_extraidos",
        value=df.to_json()
    )

    logging.info("Extracción finalizada.")


# TAREA 2 - TRANSFORMACIÓN


def tarea_transformacion(**context):

    logging.info("Iniciando transformación...")

    datos = context["ti"].xcom_pull(
        key="datos_extraidos",
        task_ids="extraccion"
    )

    import pandas as pd

    df = pd.read_json(datos)

    transformador = Transformador()

    df_final = transformador.ejecutar_transformacion(
        df,
        hora_ini=6,
        hora_fin=22
    )

    context["ti"].xcom_push(
        key="datos_transformados",
        value=df_final.to_json()
    )

    logging.info("Transformación finalizada.")


# TAREA 3 - CARGA


def tarea_carga(**context):

    logging.info("Iniciando carga...")

    datos = context["ti"].xcom_pull(
        key="datos_transformados",
        task_ids="transformacion"
    )

    import pandas as pd

    df = pd.read_json(datos)

    cargador = CargaDatos()

    cargador.guardar_csv(
        df=df,
        ruta="/opt/airflow/data/processed/clima.csv"
    )

    cargador.mostrar_resumen(df)

    logging.info("Carga finalizada.")


# DEFINICIÓN DEL DAG


with DAG(

    dag_id="dag_etl_template",

    description="Template ETL para proyectos de Ingeniería de Datos",

    start_date=datetime(2026, 1, 1),

    schedule="@once",

    catchup=False,

    default_args=default_args,

    tags=[
        "template",
        "etl",
        "airflow",
        "spark"
    ]

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