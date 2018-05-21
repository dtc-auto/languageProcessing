import pymssql
import pandas as pd
import os
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import Parser
from pyltp import NamedEntityRecognizer


ltp_data_direction = r'H:\virtual environments\venv_ltp\ltp_data_v3.4.0'
cws_model_path = os.path.join(ltp_data_direction, 'cws.model')
pos_model_path = os.path.join(ltp_data_direction, 'pos.model')
ner_model_path = os.path.join(ltp_data_direction, 'ner.model')
par_model_path = os.path.join(ltp_data_direction, 'parser.model')

server = "SQLDEV02\sql"
server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"

conn = pymssql.connect(server, user, password, database)


def connect_db():
    conn = pymssql.connect(server, user, password, database)
    return conn


def load_model(segmentor, postagger, recognizer, parser):
    # 载入模型
    segmentor.load(cws_model_path)
    postagger.load(pos_model_path)
    recognizer.load(ner_model_path)
    parser.load(par_model_path)


def get_data(sql, conn):
    data = pd.read_sql_query(sql, conn)  # 读取sql语句
    return data


def process_data(data):
    # 整合处理文字的过程
    for sentence in data.value:
        result = sentence.value.split('，')
        words = list(segmentor.segment(result))
        for word_index, word in enumerate(words):
            tags = list(postagger.postag(word))
            for tags_index, tag in enumerate(tags):
                if word_index == tags_index:
                    netags = list(recognizer.recognize(word, tag))
                    arcs = list(parser.parse(word, tag))

                else:
                    break




def write_data(result):
    pass
    # pd.to_sql


def release_model(segmentor, postagger, recognizer, parser):
    # 释放模型
    segmentor.release()
    postagger.release()
    recognizer.release()
    parser.release()


if __name__ == '__main__':
    pass
    # sql = '''select top 50 * from dw.Comments_Unpivot'''
    segmentor = Segmentor()  # 初始化实例
    postagger = Postagger()  # 初始化实例
    recognizer = NamedEntityRecognizer()  # 初始化实例
    parser = Parser()  # 初始化实例
    # conn = connect_db()
    # load_model()
    # data = get_data(sql, conn)
    # result = process_data(data)
    # write_data(result)
    # release_model()