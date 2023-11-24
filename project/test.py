import pandas as pd
from sqlalchemy import create_engine


pd.set_option('display.max_columns', 10)
db_path = '../data/project.sqlite'
engine = create_engine(f'sqlite:///{db_path}')

wage_gap_df = pd.read_sql('SELECT * FROM wage_gap', con=engine)
graduate_df = pd.read_sql('SELECT * FROM graduate_field', con=engine)

print(wage_gap_df.head())
print(graduate_df.head())

print(graduate_df["Indicator Name"].unique())
print(len(graduate_df["Indicator Name"].unique()))

print(graduate_df["Indicator Code"].unique())
print(len(graduate_df["Indicator Code"].unique()))

graduate_df['Graduation Field'] = graduate_df["Indicator Name"].str.extract(r'(?:from|in) (.*?) (?:programmes|fields,)')
graduate_df = graduate_df.drop(["Indicator Code", "Indicator Name"], axis=1)
print(graduate_df.head())