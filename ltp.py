import os
import pymssql
import re
from pyltp import NamedEntityRecognizer
from pyltp import Parser
from pyltp import Postagger
from pyltp import Segmentor

import pandas as pd
from sqlalchemy import create_engine

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


def connect_db():
    # 连接数据库
    connected = pymssql.connect(server, user, password, database)
    return connected


def load_model():
    # 载入模型
    segmentor = Segmentor()  # 初始化实例
    postagger = Postagger()  # 初始化实例
    recognizer = NamedEntityRecognizer()  # 初始化实例
    parser = Parser()  # 初始化实例
    segmentor.load(cws_model_path)
    postagger.load(pos_model_path)
    recognizer.load(ner_model_path)
    parser.load(par_model_path)
    return segmentor, postagger, recognizer, parser


def get_data(sql, conn):
    data = pd.read_sql_query(sql, conn)  # 读取sql语句
    return data


def split_sentence(sql_data):
    # 进行分句处理的过程
    sentence_index = 0
    for comments in sql_data.itertuples():
        sentence_list = re.split(pattern, comments.value)
        for sentence_pos, sentence_value in enumerate(sentence_list):
            sentence_index += 1
            return_list = list()
            if sentence_value != '':
                return_list.append(comments.commentId)
                return_list.append(sentence_pos + 1)
                return_list.append(sentence_value)
                yield return_list
            else:
                break


def process_data(sql_data):
    # 整合处理文字的过程
    counter = 0
    for sentences in sql_data.itertuples():
        word_count = 1
        counter += 1
        if counter % 100000 == 0:
            print(counter)
        words = list(segmentor.segment(sentences.sentenceValue))
        tags = list(postagger.postag(words))
        netags = list(recognizer.recognize(words, tags))
        arcs = list(parser.parse(words, tags))
        word_index = 0
        while word_index < len(words):
            return_list = list()
            return_list.append(sentences.sentenceId)
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


def write_data(engine, table_name, result):
    # 将数据分批写入数据库
    left = 0
    right = 10000
    length = len(result)
    while left < length:
        if right > length:
            right = length
        result[left:right].to_sql(
            table_name,
            engine,
            schema='dw',
            index=False,
            index_label=False,
            if_exists='append'
        )
        left += 10000
        right += 10000
        print('10000 data have been written')


def release_model(segmentor, postagger, recognizer, parser):
    # 释放模型
    segmentor.release()
    postagger.release()
    recognizer.release()
    parser.release()


if __name__ == '__main__':
    engine = create_engine('mssql+pymssql://dtc:asdf1234@sqldev02/Autohome_WOM')
    conn = connect_db()
    pattern = re.compile(
        u'\,|，|。|\?|？|\!|！|\;|；|\(|（|\)|）|…|：|\*|\n|\s|\t| |　|\.\.+'
    )

    segmentor, postagger, recognizer, parser = load_model()  # load_model

    # 分句
    sql = '''select * from dw.Comments_Unpivot'''
    data = get_data(sql, conn)
    df2 = pd.DataFrame(
        list(split_sentence(data)),
        columns=['commentId', 'sentencePos', 'sentenceValue']
    )

    # 写入数据库
    write_data(engine, 'Sentences', df2)

    # 分词
    sql = '''select * from dw.Sentences'''
    data = get_data(sql, conn)
    df = pd.DataFrame(
        list(process_data(data)),
        columns=[
            'sentenceId', 'word_pos', 'word_value',
            'part_of_speech', 'named_entity', 'parent_node',
            'children_relation',
        ]
    )
    # 写入数据库
    write_data(engine, 'Words', df)
    release_model(segmentor, postagger, recognizer, parser)
