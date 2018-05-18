import pymssql
import pandas as pd
import os
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import Parser
from pyltp import NamedEntityRecognizer
# from pyltp import SementicRoleLabeller


ltp_data_direction = r'H:\virtual environments\venv_ltp\ltp_data_v3.4.0'
cws_model_path = os.path.join(ltp_data_direction, 'cws.model')
pos_model_path = os.path.join(ltp_data_direction, 'pos.model')
ner_model_path = os.path.join(ltp_data_direction, 'ner.model')
par_model_path = os.path.join(ltp_data_direction, 'parser.model')
# srl_model_path = os.path.join(ltp_data_direction, 'pisrl.model')

server = "SQLDEV02\sql"
server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"
conn = pymssql.connect(server, user, password, database)


def read_the_sql(sql_sentence):
    df = pd.read_sql_query(sql_sentence, conn)  # 读取sql语句
    return df


def split_into_sentences(read_):
    for sentence in read_.itertuples():  # 提取出每一个句子
        yield sentence.value.split('，')  # 完整的句子拆分成一个个分句并添加到sentence列表中


def split_into_words(sentences_):
    segmentor = Segmentor()  # 初始化实例
    segmentor.load(cws_model_path)  # 载入模型
    for sentence in sentences_:  # sentence为sentences列表中的一个值
        for word in sentence:  # word为sentence列表中的一个值
            a_word = segmentor.segment(word)  # 把句子拆分成单个词
            yield list(a_word)  # 把拆分完的词添加到words列表中
    segmentor.release()  # 释放模型


def posttagger(words_):  # 参数为分词列表
    postagger = Postagger()  # 初始化实例
    postagger.load(pos_model_path)  # 载入模型
    for word in words_:  # word为words列表中的一个值
        posttags = postagger.postag(word)  # 为参数的分词列表中的词标注词性
        yield list(posttags)  # 将标注好的词性添加到tags列表中
    postagger.release()  # 释放模型


def named_entity_recognizer(words_, postags_):  # 参数为分词列表和词性列表
    recognizer = NamedEntityRecognizer()  # 初始化实例
    recognizer.load(ner_model_path)  # 载入模型
    i = 0
    while i < len(words_):  # 当i小于words列表的长度时循环
        netags = list(recognizer.recognize(words_[i], postags_[i]))  # 通过参数的分词和词性列表中的元素进行命名实体识别
        yield netags  # 将识别好的命名实体添加到recognized列表中
        i += 1
    recognizer.release()  # 释放模型


def parser_head(words_, postags_):  # 参数为分词列表和词性列表
    parser = Parser()  # 初始化实例
    parser.load(par_model_path)  # 载入模型
    i = 0
    while i < len(list(words_)):  # 当i小于words列表的长度时循环
        temp = list()  # 建立一个临时列表
        arcs_ = parser.parse(list(words_), list(postags_))  # 对参数的分词和词性列表中的元素进行依存句法分析
        for arc in arcs_:  # arc所有依存句法分析的结果其中的一个结果
            temp.append(arc.head)   # 将分析完毕的依存句法的父节点词的索引添加至临时列表中
        yield temp  # 将临时列表添加至heads列表中
        i += 1
    parser.release()   # 释放模型


def parser_relations(words_, postags_):  # 参数为分词列表和词性列表
    parser = Parser()  # 初始化实例
    parser.load(par_model_path)  # 载入模型
    i = 0
    while i < len(list(words_)):  # 当i小于words列表的长度时循环
        temp = list()  # 建立一个临时列表
        arcs = parser.parse(words_, postags_)  # 对参数的分词和词性列表中的元素进行依存句法分析
        for arc in arcs:  # arc所有依存句法分析的结果其中的一个结果
            temp.append(arc.relation)  # 将分析完毕的依存句法的依存弧的关系添加至临时列表中
        yield temp  # 将临时列表添加至heads列表中
        i += 1
    parser.release()  # 释放模型


# def parser_(words_, postags_):
#     arcs_ = list()
#     parser = Parser()
#     parser.load(par_model_path)
#     i = 0
#     while i < len(words_):
#         temp = list()
#         temp.append(parser.parse(words_[i], postags_[i]))
#         arcs_.append(temp)
#         i += 1
#     parser.release()
#     return arcs_


def combine(sentences_, words_, postags_, netags_,  heads_, relations_):  # 参数为分句列表，分词列表，词性列表，命名实体列表，依存句法索引列表，依存句法关系列表
    combination = list()
    for sentence_ind, sentence in enumerate(sentences_):  # sentence_ind为sentences列表中每个值的索引， sentence为其中一个值
        complete_sentence = list()
        str_sentence = ''.join(sentence)  # 将分句列表转换成一个完整的字符串
        for words_ind, word in enumerate(words_): # words_ind为words列表中每个值的索引， word为其中一个值
            i = 0
            j = 0
            words_count = 0
            complete_sentence += word
            str_complete_sentence = ''.join(complete_sentence)
            for word_ind, single_word in enumerate(word):  # word_ind为word列表中每个值的索引， word为其中一个值
                words_count += 1
                temp = list()
                complete = ''.join(word)
                while i < len(sentence):
                    if complete == sentence[i] and str_complete_sentence in str_sentence:
                        temp.append(sentence_ind + 1)
                        while j < len(complete_sentence):
                            if single_word == complete_sentence[j]:
                                temp.append(j + 1)
                                break
                            else:
                                j += 1
                        temp.append(single_word)
                        temp.append(postags_[words_ind][word_ind])
                        temp.append(netags_[words_ind][word_ind])
                        temp.append(heads_[words_ind][word_ind])
                        temp.append(relations_[words_ind][word_ind])
                        combination.append(temp)
                        break
                    else:
                        i += 1
    return combination


read = read_the_sql('''select top 50 * from dw.Comments_Unpivot''')
sentences = split_into_sentences(read)
words = split_into_words(sentences)
tags = posttagger(words)
recognized_tags = named_entity_recognizer(words, tags)
# pd.DataFrame()

heads = parser_head(words, tags)
relations = parser_relations(words, tags)

# for indx, word in enumerate(words):
#     print(indx)
#     print(word)
# arcs = list(parser_(words, tags))
# combined = combine(sentences, words, tags, recognized_tags, heads, relations)
# print(sentences)
# print(words)
# print(tags)
# print(list(arcs))
print(list(zip(heads, relations)))
# print(heads)
# print(relations)
# print(combined)