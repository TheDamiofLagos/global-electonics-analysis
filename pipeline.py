import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Configure PG connection
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

#Create the SQLalchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# folder containing csvs
csv_folder = "data"
# r = "r"

# Loop through the csv files and load them in PG
def load_csv_to_postgres():
    for file in os.listdir(csv_folder):
        if file.endswith(".csv"):
            table_name = file.split(".")[0]
            df = pd.read_csv( f"{csv_folder}/{file}", encoding = "ISO-8859-1")
            # df = pd.read_csv(r"driectory")
            df.to_sql(table_name, engine, index=False, if_exists="replace")
            print(f"Table {table_name} loaded successfully")


if __name__ == "__main__":
    load_csv_to_postgres()
