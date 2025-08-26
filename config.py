import os
from dotenv import load_dotenv


# keys
# Load from .env file
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DATA_RAW_PLAYOFFS = os.path.join(BASE_DIR, "data", "raw", "pbp_1996_1997_playoffs.csv")
DATA_RAW_FINALS = os.path.join(BASE_DIR, "data", "raw", "pbp_1997_finals_chi_uta.csv")
   
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")