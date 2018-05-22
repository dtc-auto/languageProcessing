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


def load_model():
    # 载入模型
    segmentor.load(cws_model_path)
    postagger.load(pos_model_path)
    recognizer.load(ner_model_path)
    parser.load(par_model_path)


def get_data(sql, conn):
    data = pd.read_sql_query(sql, conn)  # 读取sql语句
    return data


def split_sentence(sql_data):
    for sentence in sql_data.value:
        sentence_index = 0
        result = sentence.split(', ')
        yield result


def process_data(data):
    # 整合处理文字的过程
    for sentence in data.value:
        result = sentence.split('，')  # 分句方式需要修改
        word_count = 1
        sentence_index = 0
        for item in result:
            sentence_index += 1
            words = list(segmentor.segment(item))
            tags = list(postagger.postag(words))
            netags = list(recognizer.recognize(words, tags))
            arcs = list(parser.parse(words, tags))
            word_index = 0
            while word_index < len(words):
                return_list = list()
                return_list.append(sentence_index)
                return_list.append(word_count)
                return_list.append(words[word_index])
                return_list.append(tags[word_index])
                return_list.append(netags[word_index])
                for arcs_index, arc in enumerate(arcs):
                    if arcs_index == word_index:
                        return_list.append(arc.head)
                        return_list.append(arc.relation)
                    elif arcs_index > word_index:
                        break
                yield return_list
                word_count += 1
                word_index += 1


def write_data(result):
    pass
    # pd.to_sql


def release_model():
    # 释放模型
    segmentor.release()
    postagger.release()
    recognizer.release()
    parser.release()


if __name__ == '__main__':
    pass
    sql = '''select top 50 * from dw.Comments_Unpivot'''
    segmentor = Segmentor()  # 初始化实例
    postagger = Postagger()  # 初始化实例
    recognizer = NamedEntityRecognizer()  # 初始化实例
    parser = Parser()  # 初始化实例
    conn = connect_db()
    load_model()
    data = get_data(sql, conn)
    # for sentence in split_sentence(data):
    #     print(sentence)
    # # result = process_data(data)
    # for result in process_data(data):
    #     print(result)
    df = pd.DataFrame(
        list(
            process_data(data)
        ),
        columns=[
            'sentenceId', 'word_index', 'word',
            'part_of_speech', 'named_entity', 'parent_node',
            'children_relation',
        ]
    )
    print(df)
    # write_data(result)
    release_model()