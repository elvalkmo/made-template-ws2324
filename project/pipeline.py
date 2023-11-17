import pandas as pd
import zipfile
import urllib.request
import os
from io import BytesIO, StringIO
import requests
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 10)

worldbank_zip_url = "https://databank.worldbank.org/data/download/Gender_Stats_CSV.zip"
oecd_csv_url = "https://stats.oecd.org/sdmx-json/data/DP_LIVE/.WAGEGAP.../OECD?contentType=csv&detail=code&separator=comma&csv-lang=en"

#############################################################
### World Bank: zip file
#############################################################

response = requests.get(worldbank_zip_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Create a virtual file-like object from the zip file content
    zip_content = BytesIO(response.content)

    # Create a ZipFile object
    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        # Assuming you want to read the first file in the zip archive
        # first_file_name = zip_ref.namelist()[0]
        target_csv_file_name = "Gender_StatsData.csv"

        # Read CSV into Pandas DataFrame
        with zip_ref.open(target_csv_file_name) as file:
            worldbank_df = pd.read_csv(file)


# Get to know data
print(worldbank_df.shape)
#print(worldbank_df.info())

select_indicator_list = worldbank_df[worldbank_df["Indicator Name"].str.contains("share of graduates")]["Indicator Name"].unique().tolist()
graduate_df = worldbank_df[worldbank_df["Indicator Name"].isin(select_indicator_list)]
graduate_df = graduate_df.loc[:, ~graduate_df.columns.str.contains('^Unnamed')]

# last_updated_year = int(graduate_df.columns[-1])
# print(last_updated_year)
#
# # graduate_df['Check Empty Row'] = graduate_df.iloc[:, -4:-1].sum(axis=1)
#






graduate_db_path = '../data/graduate_field.sqlite'
engine = create_engine(f'sqlite:///{graduate_db_path}')
graduate_df.to_sql('graduate_field', engine, index=False, if_exists='replace')


#############################################################
### oecd
#############################################################

oecd_df = pd.read_csv(oecd_csv_url)
oecd_df = oecd_df.drop(["MEASURE", "FREQUENCY", "Flag Codes"], axis=1)

wage_gap_db_path = '../data/wage_gap.sqlite'
engine = create_engine(f'sqlite:///{wage_gap_db_path}')
oecd_df.to_sql('wage_gap', engine, index=False, if_exists='replace')

"""
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

"""