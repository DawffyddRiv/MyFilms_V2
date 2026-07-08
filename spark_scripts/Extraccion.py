import requests 
import logging
import pandas as pd
#import json


from config.settings import OMDB_API_KEY
logger = logging.getLogger(__name__)


class ExtractorDatos:
    BASE_URL = "http://www.omdbapi.com/"
    

    def __init__(self, api_key):
        self.llaveApi = api_key

    def busqueda_peliculas(self,queries,max_pages=5):
        resultados=[]
        for q in queries: 
            for page in range(1,max_pages+1):
                parmetrs = {
                    "apikey": self.llaveApi,
                    "s": q,
                    "type": "movie",
                    "page": page
                }
                logger.info(f"Consultando '{q}' página {page}")
                
                try:
                    
                    respuesta=requests.get(self.BASE_URL,params=parmetrs,timeout=30)
                    respuesta.raise_for_status()
                    data=respuesta.json()
                
                except requests.exceptions.RequestException:
                    logger.exception(f"Error consultando '{q}'")
                    break
                except ValueError:
                    logger.exception(f"Respuesta JSON inválida")    
                    break
                
                if data.get("Response")=="True":
                    resultados.extend([p["Title"] for p in data["Search"]])# ->-Recorre cada elemento "p"  dentro de "Search" del cual extrae sólo "Title"
                else:
                    logger.warning(f"No hay resultados para {q} {data.get('Error')}")
                    break
        logger.info(f"Se encontraron {len(resultados)} títulos.")
        return resultados

    def extraer_api(self, titulo):

        parmetrs = {
                    "apikey": self.llaveApi,
                    "t": titulo 
        }
       
        #url=f"http://www.omdbapi.com/?t={titulo}&apikey={self.llaveApi}" #<- "?t=" recolecta información más detallada. Ya no se requiere, ya se refactorizó abajo
        logger.info(f"Obteniendo información de '{titulo}'")
        try:
            respuesta=requests.get(self.BASE_URL,params=parmetrs,timeout=30)

            respuesta.raise_for_status()

            return respuesta.json()

        except requests.exceptions.RequestException:
            logger.exception(f"Error consultando '{titulo}'")       

        return None


    def extrae_data_pelicula(self,resultados):
        data=[]

        for titulo in resultados:
            peli=self.extraer_api(titulo)
            if peli and peli.get('Response')=='True':
                data.append(peli)
            else:
                logger.warning(f"No se encontró: {titulo}")
        return pd.DataFrame(data)

   



# Prueba local


if __name__ == "__main__":    

    API_KEY=OMDB_API_KEY
    pelis=ExtractorDatos(API_KEY)#Esta es mi instancia principal
    palabrasClave = ["star", "love", "man", "dark"]
    nombrespeliculas=pelis.busqueda_peliculas(palabrasClave) 
    datosPeliculas=pelis.extrae_data_pelicula(nombrespeliculas)
    print(datosPeliculas)