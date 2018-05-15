import pymssql
import pandas as pd
import os

LTP_DATA_DIR = 'C:\Users\Zhang Mingqiu\PycharmProjects\languageProcessing\ltp_data_v3.4.0'  # ltp模型目录的路径
cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model')
server = "SQLDEV02\sql"
server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"
conn = pymssql.connect(server, user, password, database)
sql = '''select top 50 * from dw.Comments_Unpivot'''


def read_the_sql(sql_sentence):
    df = pd.read_sql_query(sql_sentence, conn)
    return df

read = read_the_sql(sql)
print([item.value for item in read.itertuples()])

#pyltp suedo code
#分句
#for item in df.itertuples()
#sentencessplitter.split()


def split_into_sentences():
    sents = SentenceSplitter().split(sentence.value for sentence in read.itertuples())
    return sents

sentences = split_into_sentences()


def split_into_words():
    segmentor = Segmentor()
    segmentor.load(cws_model_path)
    words = segmentor.segment(for sentence in sentences)