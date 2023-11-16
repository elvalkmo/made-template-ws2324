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
valid_verkehr_list = ["FV", "RV", "nur DPN"]
clean_df = df[df["Verkehr"].isin(valid_verkehr_list)]

clean_df = clean_df[(clean_df["Laenge"] >= -90) & (clean_df["Laenge"] <= 90)]
clean_df = clean_df[(clean_df["Breite"] >= -90) & (clean_df["Breite"] <= 90)]

clean_df = clean_df[clean_df["IFOPT"].notnull()]
clean_df = clean_df[clean_df["Betreiber_Name"].notnull()]
clean_df = clean_df[clean_df["Betreiber_Nr"].notnull()]
clean_df["Betreiber_Nr"] = clean_df["Betreiber_Nr"].astype("int64")
clean_df[["char", "num1", "num2", "num3"]] = clean_df["IFOPT"].str.split(":", expand=True)

mask = (clean_df["char"].str.len() == 2) & \
       (clean_df["num1"].str.isnumeric()) & \
       (clean_df["num2"].str.isnumeric())

clean_df = clean_df[mask]
clean_df = clean_df.drop(["char", "num1", "num2", "num3"], axis=1)
clean_df = clean_df[clean_df["Betreiber_Nr"].notnull()]
print(clean_df.head())
print(clean_df.info())

engine = create_engine('sqlite:///trainstops.sqlite')
clean_df.to_sql('trainstops', engine, index=False, if_exists='replace')
engine.dispose()