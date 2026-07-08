import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class CargaDatos:
    """
    Clase encargada de la carga de datos.

    Permite escribir el resultado del proceso ETL en diferentes destinos.
    """

    def __init__(self):
        pass

    # ==========================================
    # VALIDACIÓN DEL DATAFRAME
    # ==========================================

    def _validar_dataframe(self, df: pd.DataFrame):

        if df is None:
            raise ValueError("El DataFrame es None.")

        if df.empty:
            raise ValueError("El DataFrame está vacío.")

        logging.info(f"Registros a cargar: {len(df)}")

    # ==========================================
    # CARGA A CSV
    # ==========================================

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

    # ==========================================
    # CARGA A PARQUET
    # ==========================================

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

    # ==========================================
    # CARGA A POSTGRESQL
    # ==========================================

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

    # ==========================================
    # RESUMEN DE CARGA
    # ==========================================

    def mostrar_resumen(self, df: pd.DataFrame):

        logging.info("==============")

        logging.info("Carga finalizada")

        logging.info(f"Total registros: {len(df)}")

        logging.info(f"Total columnas : {len(df.columns)}")

        logging.info("==============")