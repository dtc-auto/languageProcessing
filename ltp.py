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
    df = pd.read_sql_query(sql_sentence, conn)
    return df


def split_into_sentences():
    sentence_list = list()
    for sentence in read.itertuples():
        sentence_list.append(sentence.value.split('，'))
    return sentence_list


def split_into_words():
    words_ = list()
    segmentor = Segmentor()
    segmentor.load(cws_model_path)
    for sentence in sentences:
        for word in sentence:
            a_word = segmentor.segment(word)
            words_.append(list(a_word))
    segmentor.release()
    return words_


def posttagger(words_):
    tags = list()
    postagger = Postagger()
    postagger.load(pos_model_path)
    for word in words_:
        posttags = postagger.postag(word)
        tags.append(list(posttags))
    postagger.release()
    return tags


def named_entity_recognizer(words_, postags_):
    recognized = list()
    recognizer = NamedEntityRecognizer()
    recognizer.load(ner_model_path)
    i = 0
    while i < len(words_):
        netags = list(recognizer.recognize(words_[i], postags_[i]))
        recognized.append(netags)
        i += 1
    return recognized


def parser_head(words_, postags_):
    heads_ = list()

    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words_):
        temp = list()
        arcs_ = parser.parse(words_[i], postags_[i])
        for arc in arcs_:
            temp.append(arc.head)
        heads_.append(temp)
        i += 1
    parser.release()
    return heads_


def parser_relations(words_, postags_):
    relations_ = list()
    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words_):
        temp = list()
        arcs = parser.parse(words_[i], postags_[i])
        for arc in arcs:
            temp.append(arc.relation)
        relations_.append(temp)
        i += 1
    parser.release()
    return relations_


def parser_(words_, postags_):
    arcs_ = list()
    parser = Parser()
    parser.load(par_model_path)
    i = 0
    while i < len(words_):
        temp = list()
        temp.append(parser.parse(words_[i], postags_[i]))
        arcs_.append(temp)
        i += 1
    parser.release()
    return arcs_


def combine(sentences_, words_, postags_, netags_,  heads_, relations_):
    combination = list()
    for sentence_ind, sentence in enumerate(sentences_):
        complete_sentence = list()
        str_sentence = ''.join(sentence)
        for words_ind, word in enumerate(words_):
            i = 0
            j = 0
            words_count = 0
            complete_sentence += word
            str_complete_sentence = ''.join(complete_sentence)
            for word_ind, single_word in enumerate(word):
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
sentences = split_into_sentences()
words = split_into_words()
tags = posttagger(words)
recognized_tags = named_entity_recognizer(words, tags)
# pd.DataFrame()

heads = parser_head(words, tags)
relations = parser_relations(words, tags)

# for indx, word in enumerate(words):
#     print(indx)
#     print(word)
# arcs = list(parser_(words, tags))
combined = combine(sentences, words, tags, recognized_tags, heads, relations)
# print(sentences)
# print(words)
# print(tags)
# print(list(arcs))
# print(list(zip(heads, relations)))
# print(heads)
print(combined)

