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

save_data_to_sql = True

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


select_indicator_list = worldbank_df[worldbank_df["Indicator Name"].str.contains("share of graduates")]["Indicator Name"].unique().tolist()
graduate_df = worldbank_df[worldbank_df["Indicator Name"].isin(select_indicator_list)]
graduate_df = graduate_df.loc[:, ~graduate_df.columns.str.contains('^Unnamed')]

print(graduate_df.info())

############################ Data Transformation ########################################
graduate_df['Check Empty Row'] = graduate_df.iloc[:, 5:-1].sum(axis=1)
print(graduate_df['Check Empty Row'])
print(graduate_df.shape)
graduate_df = graduate_df[graduate_df['Check Empty Row'] != 0]
graduate_df = graduate_df.drop('Check Empty Row', axis=1)
print(graduate_df.shape)


# Melting with column names
unpivot_keep_col = graduate_df.columns[0:4]
unpivot_melt_col = graduate_df.columns[5:-1]
graduate_df = graduate_df.melt(id_vars=unpivot_keep_col,
                               value_vars=unpivot_melt_col,
                               var_name=['Year'], value_name='Female Share')

graduate_df = graduate_df[graduate_df["Female Share"].notnull()]
graduate_df['Graduation Field'] = graduate_df["Indicator Name"].str.extract(r'(?:from|in) (.*?) (?:programmes|fields,)')
graduate_df = graduate_df.drop(["Indicator Code", "Indicator Name"], axis=1)
graduate_df = graduate_df[["Country Code", "Country Name", "Graduation Field", "Year", "Female Share"]]
print(graduate_df.head())


################################Safe data####################################

db_path = '../data/project.sqlite'
engine = create_engine(f'sqlite:///{db_path}')

if save_data_to_sql:
    graduate_df.to_sql('graduate_field', engine, index=False, if_exists='replace')


#############################################################
### oecd
#############################################################

oecd_df = pd.read_csv(oecd_csv_url)
oecd_df = oecd_df.drop(["MEASURE", "FREQUENCY", "Flag Codes", "INDICATOR"], axis=1)
print(oecd_df.head())


if save_data_to_sql:
    oecd_df.to_sql('wage_gap', engine, index=False, if_exists='replace')

# test

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