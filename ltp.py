import pymssql
import pandas as pd
import os
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import Parser
from pyltp import SementicRoleLabeller


ltp_data_direction = r'H:\virtual environments\venv_ltp\ltp_data_v3.4.0'
cws_model_path = os.path.join(ltp_data_direction, 'cws.model')
pos_model_path = os.path.join(ltp_data_direction, 'pos.model')
par_model_path = os.path.join(ltp_data_direction, 'parser.model')
srl_model_path = os.path.join(ltp_data_direction, 'pisrl.model')


server = "SQLDEV02\sql"
server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"
conn = pymssql.connect(server, user, password, database)


def read_the_sql(sql_sentence):
    df = pd.read_sql_query(sql_sentence, conn)
    return df


def split_into_sentences():
    sentence_list = list()
    for sentence in read.itertuples():
        sentence_list.append(sentence.value.split('，'))
    return sentence_list


def split_into_words():
    words = list()
    segmentor = Segmentor()
    segmentor.load(cws_model_path)
    for sentence in sentences:
        for word in sentence:
            a_word = segmentor.segment(word)
            words.append(list(a_word))
    segmentor.release()
    return words


def posttagger(words):
    tags = list()
    postagger = Postagger()
    postagger.load(pos_model_path)
    for word in words:
        posttags = postagger.postag(word)
        tags.append(list(posttags))
    postagger.release()
    return tags


def parser_head(words, postags):
    heads = list()
    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words):
        arcs = parser.parse(words[i], postags[i])
        for arc in arcs:
            heads.append(arc.head)
        i += 1
    parser.release()
    return heads


def parser_relations(words, postags):
    relations = list()
    temp = list()
    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words):
        arcs = parser.parse(words[i], postags[i])
        for arc in arcs:
            relations.append(arc.relation)
        i += 1
    parser.release()
    return relations


def parser_(words, postags):
    arcs = list()
    temp = list()
    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words):
        temp.append(parser.parse(words[i], postags[i]))
        arcs.append(temp)
        i += 1
    parser.release()
    return arcs


def label(words, postags, arcs):
    parser = Parser()
    role_names = list()
    labeller = SementicRoleLabeller()
    labeller.load(srl_model_path)
    i = 0
    while i < len(words):
        roles = labeller.label(words[i], postags[i], arcs[i])
        for role in roles:
            role_names.append(role.name)
        i += 1
    labeller.release()
    return role_names


# def make_table(sentences, words, )
read = read_the_sql('''select top 50 * from dw.Comments_Unpivot''')
sentences = split_into_sentences()
words = split_into_words()
# print(words)
tags = posttagger(words)
# print(tags)

heads = parser_head(words, tags)
relations = parser_relations(words, tags)
arcs = parser_(words, tags)
role_names = label(words, tags, arcs)
# print(len(sentences))
print(words)
print(len(tags))
# print(heads)
# print(relations)
print(role_names)

