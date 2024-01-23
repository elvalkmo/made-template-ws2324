import pandas as pd
import urllib.request
import zipfile
import os
from sqlalchemy import create_engine

url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
urllib.request.urlretrieve(url, "ex4.zip")

with zipfile.ZipFile("ex4.zip", 'r') as zip_ref:
    target_csv_file_name = "data.csv"
    zip_ref.extract(target_csv_file_name, path=".")

columns_to_include = list(range(11))
df = pd.read_csv("data.csv", delimiter=";", decimal=",", header=None, converters={i: str for i in columns_to_include}, usecols=columns_to_include, skiprows=1)
df.columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur", "Latitude (WGS84)", "Longitude (WGS84)", "Verschleierung (m)", "Aufenthaltsdauer im Freien (ms)", "Batterietemperatur", "Geraet aktiv"]
df = df[["Geraet", "Hersteller", "Model", "Monat", "Temperatur", "Batterietemperatur", "Geraet aktiv"]]

os.remove("data.csv")
os.remove("ex4.zip")

def CelsiusToFahrenheit(temp):
    return ((temp * 9/5) + 32)

df["Temperatur"] = df["Temperatur"].str.replace(",", ".").astype(float).apply(CelsiusToFahrenheit).round(2)
df["Batterietemperatur"] = df["Batterietemperatur"].str.replace(",", ".").astype(float).apply(CelsiusToFahrenheit).round(2)

# Validate Data
df["Geraet"] = pd.to_numeric(df["Geraet"], errors='coerce')
df["Monat"] = pd.to_numeric(df["Monat"], errors='coerce')
df = df.dropna(subset=["Geraet", "Hersteller", "Model", "Monat"])

engine = create_engine(f'sqlite:///temperatures.sqlite')
df.to_sql('temperatures', engine, index=False, if_exists='replace')