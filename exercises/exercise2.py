"""
attrs==22.2.0
greenlet==2.0.2
iniconfig==2.0.0
numpy==1.24.2
packaging==23.0
pandas==1.5.3
pluggy==1.0.0
python-dateutil==2.8.2
pytz==2022.7.1
six==1.16.0
SQLAlchemy==1.4.46
typing_extensions==4.5.0
"""

import pandas as pd
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 10)

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(url, delimiter=";", thousands=".", decimal=",")

df = df.drop("Status", axis=1)
print(df.head())
print(df.info())

# print(df.groupby("Verkehr").agg({"Verkehr":"count"}))
print(df.shape)
valid_verkehr_list = ["FV", "RV", "nur DPN"]
clean_df = df[df["Verkehr"].isin(valid_verkehr_list)]
# print(clean_df.groupby("Verkehr").agg({"Verkehr":"count"}))
print(clean_df.shape)

print(max(clean_df["Laenge"]))
print(min(clean_df["Laenge"]))
print(max(clean_df["Breite"]))
print(min(clean_df["Breite"]))

clean_df = clean_df[(clean_df["Laenge"] >= -90) & (clean_df["Laenge"] <= 90)]
clean_df = clean_df[(clean_df["Breite"] >= -90) & (clean_df["Breite"] <= 90)]

print(max(clean_df["Laenge"]))
print(min(clean_df["Laenge"]))
print(max(clean_df["Breite"]))
print(min(clean_df["Breite"]))

clean_df = clean_df[clean_df["IFOPT"].notnull()]
clean_df[["char", "num1", "num2", "num3"]] = clean_df["IFOPT"].str.split(":", expand=True)

# TODO : drop Betreiber_Nr = null?

mask = (clean_df["char"].str.len() == 2) & \
       (clean_df["num1"].str.isnumeric()) & \
       (clean_df["num2"].str.isnumeric())

clean_df = clean_df[mask]
clean_df = clean_df.drop(["char", "num1", "num2", "num3"], axis=1)

print(clean_df.info())
print(clean_df.head())

# db_path = "/data/trainstops.sqlite"
# engine = create_engine(f"sqlite:///{db_path}")
# clean_df.to_sql('trainstops', engine, index=False, if_exists='replace')
# engine.dispose()

# db_path = '/data/your_database_name.db'
#
# # Create a SQLAlchemy engine with the specified database path
# engine = create_engine(f'sqlite:///{db_path}')
engine = create_engine('sqlite:///trainstops.sqlite')
# Write the DataFrame to a table in the SQLite database
clean_df.to_sql('trainstops', engine, index=False, if_exists='replace')

# Optionally, close the engine
engine.dispose()