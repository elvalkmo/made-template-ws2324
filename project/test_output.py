import pandas as pd
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 10)
db_path = '../data/project.sqlite'
engine = create_engine(f'sqlite:///{db_path}')

wage_gap_df = pd.read_sql('SELECT * FROM wage_gap', con=engine)
graduate_df = pd.read_sql('SELECT * FROM graduate_field', con=engine)

if wage_gap_df.shape[0] > 1000 and graduate_df.shape[0] > 15000:
    print("Both wage gap and graduate dataset are valid")
elif wage_gap_df.shape[0] > 1000:
    print("Invalid : Graduate dataset from Word Bank")
elif graduate_df.shape[0] > 15000:
    print("Invalid : Wage gap dataset from OECD")
else:
    print("Error: unable to download datasets")
