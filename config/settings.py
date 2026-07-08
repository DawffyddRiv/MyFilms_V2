from wihth import load_dotenv
import os

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")

#BASE_URL = "http://www.omdbapi.com/"
#TIMEOUT = 30
#MAX_PAGES = 5