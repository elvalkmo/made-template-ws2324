import pandas as pd
import zipfile
from io import BytesIO, StringIO
import requests
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 10)

worldbank_zip_url = "https://databank.worldbank.org/data/download/Gender_Stats_CSV.zip"
oecd_csv_url = "https://stats.oecd.org/sdmx-json/data/DP_LIVE/.WAGEGAP.../OECD?contentType=csv&detail=code&separator=comma&csv-lang=en"

save_data_to_sql = True

#############################################################
#                   OECD - Wage Gap
#############################################################

wage_gap_df = pd.read_csv(oecd_csv_url)
wage_gap_df = wage_gap_df[wage_gap_df["SUBJECT"] == "EMPLOYEE"]
wage_gap_df = wage_gap_df.drop(["SUBJECT", "MEASURE", "FREQUENCY", "Flag Codes", "INDICATOR"], axis=1)
exclude_groups = ["OECD", "EU27"]
wage_gap_df = wage_gap_df[~wage_gap_df["LOCATION"].isin(exclude_groups)]

wage_gap_df.rename(columns={'Value': 'WAGE GAP'}, inplace=True)
print(wage_gap_df.head())
print(len(wage_gap_df["LOCATION"].unique()))

#############################################################
#             World Bank - Graduation Fields
#############################################################

response = requests.get(worldbank_zip_url)

if response.status_code == 200:
    zip_content = BytesIO(response.content)

    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        target_csv_file_name = "Gender_StatsData.csv"
        with zip_ref.open(target_csv_file_name) as file:
            worldbank_df = pd.read_csv(file)


select_indicator_list = worldbank_df[worldbank_df["Indicator Name"].str.contains("share of graduates")]["Indicator Name"].unique().tolist()
graduate_df = worldbank_df[worldbank_df["Indicator Name"].isin(select_indicator_list)]
graduate_df = graduate_df.loc[:, ~graduate_df.columns.str.contains('^Unnamed')]

#############################################################
#                     Data Cleaning
#############################################################

graduate_df['Check Empty Row'] = graduate_df.iloc[:, 5:-1].sum(axis=1)
print(graduate_df['Check Empty Row'])
print(graduate_df.shape)
graduate_df = graduate_df[graduate_df['Check Empty Row'] != 0]
graduate_df = graduate_df.drop('Check Empty Row', axis=1)
print(graduate_df.shape)

unpivot_keep_col = graduate_df.columns[0:4]
unpivot_melt_col = graduate_df.columns[5:-1]
graduate_df = graduate_df.melt(id_vars=unpivot_keep_col,
                               value_vars=unpivot_melt_col,
                               var_name=['Year'], value_name='Female Share')

graduate_df = graduate_df[graduate_df["Female Share"].notnull()]
graduate_df['Graduation Field'] = graduate_df["Indicator Name"].str.extract(r'(?:from|in) (.*?) (?:programmes|fields,)')
graduate_df = graduate_df.drop(["Indicator Code", "Indicator Name"], axis=1)
graduate_df = graduate_df[["Country Code", "Country Name", "Graduation Field", "Year", "Female Share"]]

drop_field_list = ["other fields than Science, Technology, Engineering and Mathematics", "unknown or unspecified"]
graduate_df = graduate_df[~graduate_df["Graduation Field"].isin(drop_field_list)]

graduate_df["Year"] = graduate_df["Year"].astype(int)

print(graduate_df.head())
print(graduate_df.info())

wage_gap_country_list = wage_gap_df["LOCATION"].unique()

graduate_df = graduate_df[graduate_df["Country Code"].isin(wage_gap_country_list)]
print(len(wage_gap_country_list))
print(len(graduate_df["Country Code"].unique()))

#############################################################
#                  Write Data into SQLite
#############################################################

db_path = '../data/project.sqlite'
engine = create_engine(f'sqlite:///{db_path}')

if save_data_to_sql:
    wage_gap_df.to_sql('wage_gap', engine, index=False, if_exists='replace')
    graduate_df.to_sql('graduate_field', engine, index=False, if_exists='replace')

#############################################################
#         Further transformation after exploration
#############################################################

# Step 1: Data Selection - Wage Gap Dataset

selected_wage_gap_df = wage_gap_df[wage_gap_df["TIME"] != 2022]
latest = selected_wage_gap_df["TIME"].max()
selected_years = latest - 20
selected_wage_gap_df = selected_wage_gap_df[selected_wage_gap_df["TIME"] >= selected_years]

pivot = selected_wage_gap_df.pivot_table(index="LOCATION", columns="TIME", values="WAGE GAP", fill_value=0)
zero_count_per_row = pivot.apply(lambda row: (row == 0).sum(), axis=1)
drop_rows =  zero_count_per_row[zero_count_per_row > 3].index
selected_wage_gap_df = selected_wage_gap_df[~selected_wage_gap_df["LOCATION"].isin(drop_rows)]

# Step 2: Data Selection - Graduation Field Dataset

selected_countries = selected_wage_gap_df["LOCATION"].unique()
selected_graduate_df = graduate_df[graduate_df["Country Code"].isin(selected_countries)]

selected_graduate_df = selected_graduate_df[selected_graduate_df["Year"] >= selected_years]

graduate_df_unique_count = selected_graduate_df.groupby(["Country Code"]).agg({"Year":'nunique'})
selected_countries = graduate_df_unique_count[graduate_df_unique_count["Year"] > 5].index
selected_wage_gap_df = selected_wage_gap_df[selected_wage_gap_df["LOCATION"].isin(selected_countries)]
selected_graduate_df = selected_graduate_df[selected_graduate_df["Country Code"].isin(selected_countries)]

# Step 3: Extract Transformation to Facilitate Visualization

selected_graduate_df["Non-Female Share"] = 100 - selected_graduate_df["Female Share"]

min_year = graduate_df["Year"].min()
early_year = (graduate_df["Year"] >= min_year) & graduate_df["Year"] < (min_year + 5)
early_year_field_distribution = graduate_df[early_year].groupby("Graduation Field").agg({"Female Share":'mean'}).sort_values("Female Share", ascending=False)
early_year_field_distribution = early_year_field_distribution.reset_index()

def categorize_female_share(share):
    if share > 66:
        return 'with Female Majority'
    elif share < 33:
        return 'with Male Majority'
    else:
        return 'with No Significant Majority'

early_year_field_distribution['Traditional Groups of Fields'] = early_year_field_distribution['Female Share'].apply(categorize_female_share)
trad_group_fields = early_year_field_distribution.drop("Female Share", axis=1)

grouped_selected_graduate_df = selected_graduate_df.merge(trad_group_fields, on="Graduation Field")

#############################################################
#                  Write Data into SQLite
#############################################################

if save_data_to_sql:
    selected_wage_gap_df.to_sql('selected_wage_gap', engine, index=False, if_exists='replace')
    grouped_selected_graduate_df.to_sql('selected_graduate_field', engine, index=False, if_exists='replace')
