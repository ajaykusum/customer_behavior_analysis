import pandas as pd
from sqlalchemy import create_engine
df = pd.read_csv('customer_shopping_behavior.csv')
#print(df.head())

df.info()
df.describe(include="all")  # Summary statistics for all columns,as describe shows only numeric by default

# print(df.isnull().sum())

df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))

#print(df.isnull().sum())

df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')

#print(df.columns)

df= df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

#print(df.columns)

#creating column age_group
lables = ['Young Adult', 'Adult','Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=lables)

#print(df[['age', 'age_group']].head(10))

#create column purchase_frequency_days

frequency_mapping={
    'Fortnightly':14,
    'Weekly':7,
    'Monthly':30,
    'Quarterly':90,
    'Bi-Weekly':14,
    'Annually':365,
    'Every 3 months':90
}

df['purchase_frequency_days']= df['frequency_of_purchases'].map(frequency_mapping)   

print(df[['purchase_frequency_days','frequency_of_purchases']].head(10))


#connect to postgresql and store cleaned data
username = 'postgres'
password = '87654321'
host = 'localhost'
port = '5432'
database = 'customer_behavior'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

table_name = 'customer'
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"DataFrame successfully stored in the '{table_name}' table of the '{database}' database.")   