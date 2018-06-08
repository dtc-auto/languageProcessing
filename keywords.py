import pymssql
import pandas as pd

server = "127.0.0.1"
user = "dtc"
password = "asdf1234"
database = "Autohome_WOM"


def connect_db():
    # 连接数据库
    connected = pymssql.connect(server, user, password, database)
    return connected


def get_data(sql, conn):
    data = pd.read_sql_query(sql, conn)  # 读取sql语句
    return data


def recognizer(data):
    sentenceIds = data.sentenceId.unique()
    for senId in sentenceIds:
        att_coo_list.clear()
        adv_list.clear()
        sbv_list.clear()
        part_of_data = data[data['sentenceId'] == senId]
        key_word = part_of_data[part_of_data['children_relation'] == 'HED']
        key_word_pos = list(key_word.word_pos)
        coo_word = part_of_data[part_of_data['children_relation'] == 'COO']
        coo_word_parent = list(coo_word.parent_node)
        if list(key_word.part_of_speech) == 'n':
            noun_process(part_of_data, key_word)
        elif list(key_word.part_of_speech) == 'v':
            verb_process(part_of_data, key_word)
        elif list(key_word.part_of_speech) == 'a':
            adj_process(part_of_data, key_word)
        if key_word_pos == coo_word_parent:
            if list(coo_word.part_of_speech) == 'n':
                noun_process(part_of_data, coo_word)
            elif list(coo_word.part_of_speech) == 'v':
                verb_process(part_of_data, coo_word)
            elif list(coo_word.part_of_speech) == 'a':
                adj_process(part_of_data, coo_word)


def noun_process(data, key_word):
    coo_words = list()
    att_words = list()
    if data[['children_relation'] == 'ATT']:
        att_words.append(data[['children_relation'] == 'ATT'])
        for att_word in att_words:
            att_word_parent = list(att_word.parent_node)
            if att_word_parent == list(key_word.word_pos):
                att_coo_list.extend(att_word)
    if data[['children_relation'] == 'COO']:
        coo_words.append(data[['children_relation'] == 'COO'])
    for word_values in att_coo_list:
        if coo_words:
            for coo_word in coo_words:
                if list(coo_word.parent_node) == list(word_values.word_pos):
                    att_coo_list.append(coo_word)
        if list(word_values.part_of_speech) == 'n':
            noun_process(data, word_values)
        elif list(word_values.part_of_speech) == 'v':
            verb_process(data, word_values)
        elif list(word_values.part_of_speech) == 'a':
            adj_process(data, word_values)


def verb_process(data, key_word):
    pass


def adj_process(data, key_word):
    pass
    adv_words = list()
    if data[['children_relation'] == 'ADV']:
        adv_words.append(data[['children_relation'] == 'ADV'])
        for adv_word in adv_words:
            adv_word_parent = list(adv_word.parent_node)
            if adv_word_parent == list(key_word.word_pos):
                adv_list.extend(adv_word)
    for adv_word in adv_list:
        return list(adv_word.word_value)


if __name__ == '__main__':
    pass
    data_list = list()
    att_coo_list = list()
    adv_list = list()
    sbv_list = list()
    key_word_pos = list()
    conn = connect_db()
    sql = '''select Top 49 * from dw.Words'''
    sql_data = get_data(sql, conn)
    recognizer(sql_data)
