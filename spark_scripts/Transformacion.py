from Extraccion import ExtractorDatos
import logging
import pandas as pd
import numpy as np




class Transformador:
    """
    Clase encargada EXCLUSIVAMENTE de la transformación y validación de datos de clima.
    Aplica el principio de diseño: Recibe un DataFrame, devuelve un DataFrame.
    """
    def __init__(self):
        pass

    
    # 1. SUB-ETAPA: LIMPIEZA 
    
    def _limpiar_y_sanitizar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra columnas, estandariza tipos de datos y elimina duplicados."""
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
        """
        Identifica anomalías (nulos, negativos inválidos).
        En lugar de solo imprimir, separa o limpia los datos.
        """
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
        """Aplica filtros específicos y añade columnas calculadas/flags de negocio."""
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
        """Garantiza el orden de columnas exacto que requiere el destino (Load)."""
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
        """
        Punto de entrada único del componente. Orquesta el flujo secuencial interno.
        """
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