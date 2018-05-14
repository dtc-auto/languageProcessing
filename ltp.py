import pymssql
import pandas as pd

server = "SQLDEV02\sql"
server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"

sql = '''select top 50 manipulation from src.Raw_Data'''
conn = pymssql.connect(server, user, password, database)
df = pd.read_sql_query(sql, conn)

print([item.manipulation for item in df.itertuples()])
