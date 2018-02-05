import os, sys
import datetime as d
import collections
import random
import csv
import numpy as np
import pandas as pd
import hashlib
from pandas.io import sql
import psycopg2
import psycopg2.pool
import requests
from threading import Thread
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
from sms_object_toolbox import *

def convert_args_to_list(arg, arg_type = None):
    arg_list = []
    if not isinstance(arg, list):
        arg_list.append(convert_arg(arg, arg_type))
    else:
        for item in arg:
            arg_list.append(convert_arg(item, arg_type))
    return arg_list

def convert_arg(arg, arg_type = None):
    #return arg
    if not isinstance(arg, basestring):
        if isinstance(arg, int):
            if arg_type == 'cp':
                arg = str(arg)
                if len(arg) == 5:
                    return arg
                elif len(arg) == 4:
                    return '0' + arg
                elif len(arg) <= 3:
                    return arg + ('0' * (5 - len(arg)))
                elif len(arg) > 5:
                    return arg[:5]
            else:
                arg = str(arg)
                return arg
        else:
            arg = str(arg)
            return arg
    else:
        return arg
    
def adjust_for_proxi(df, proxi = None, amplitude = 0.05, col_to_adjust = 'nb_sms', base = 1.2):
    if proxi:
        random_factor = 1
        if amplitude != 0: random_factor = random.uniform((1 - amplitude), (1 + amplitude))
        multi_factor = (base + (int(proxi) / 100)) * random_factor
        print "Multiply by %s for +%skm" % (str(multi_factor), str(proxi))
        try:
            if col_to_adjust in list(df):
                df["+%skm" % str(proxi)] = df[col_to_adjust].multiply(multi_factor).round(0).astype('int32')
        except:
            df = int(df * multi_factor)
    return df

def adjust_for_interest(df, interest_id, amplitude = 0.05, col_to_adjust = 'nb_sms', base = 1.2):
    # interest_dict = {1:'csp+', 2:'auto', 3:'demenagement'}
    interest_dict = {1:0.75, 2:0.85, 3:0.2}
    if interest_id:
        random_factor = 1
        if amplitude != 0: random_factor = random.uniform((1 - amplitude), (1 + amplitude))
        if interest_id in interest_dict.keys():
            multi_factor = interest_dict[interest_id] * random_factor
        else:
            multi_factor = 0.8 * random_factor
        try:
            if col_to_adjust in list(df):
                df[col_to_adjust] = df[col_to_adjust].multiply(multi_factor).round(0).astype('int32')
        except:
            df = int(df * multi_factor)
    return df

def create_cp_fix_df(cp_dict, cp_param_list):
    print cp_dict
    data = []
    two_digit = lambda x: '0' + str(x) if len(str(x)) == 1 else str(x)
    for n_cp, cp_list in cp_dict.iteritems():
        for cp in cp_list:
            cp_num = cp[1:-2]
            for cp_param in cp_param_list:
                if cp_num == cp_param[:n_cp]:
                    cp_show = str(cp_param)
                else:
                    cp_show = str(cp_num) + ('0' * (5 - n_cp))
            if n_cp == 4:
                for cpt in range(10):
                    data.append([str(cp_show), str(cp_num) + str(cpt)])
            elif n_cp == 3:
                for cpt in range(100):
                    data.append([str(cp_show), str(cp_num) + two_digit(cpt)])
    df = pd.DataFrame(data = data, columns = ['cp_show', 'cp'])
    show_df(df)
    return df

def sms_query_by_city(city_list = None, cp_nb_char_precision = 5,
                      age_min = None, age_max = None, civi = None,
                      sms_list = None,
                      count = True, groupby_count = True, groupby_count_depth = 1, groupby_count_field = None,
                      select_field = None,
                      sms_table = 'sms', sms_id_table = 'id'):
    db_conn = pg.get_connection()
    join_field = 'cp'
    cp_list, city_cp_df = get_cp_from_city(city_list, db_conn, strict = True)
    if count:
        q = sms_query_builder(cp_list, None, cp_nb_char_precision, age_min, age_max, civi, sms_list,
                          count, groupby_count, groupby_count_depth, groupby_count_field,
                          select_field, sms_table, sms_id_table)
        df = pd.read_sql(q, db_conn)
        show_df(df)
        if groupby_count:
            df = pd.merge(allow_df_for_cp_join(df, cp_nb_char_precision), allow_df_for_cp_join(city_cp_df, cp_nb_char_precision))
            show_df(df)
            df = df.groupby('ville').sum()
        else:
            pass
    else:
        if select_field:
            if not isinstance(select_field, list): select_field = [select_field]
            if join_field in select_field:
                pass
            else:
                select_field.append(join_field)
        else:
            select_field = [join_field]
        q = sms_query_builder(cp_list, None, cp_nb_char_precision, age_min, age_max, civi, sms_list,
                              count, groupby_count, groupby_count_depth, groupby_count_field,
                              select_field, sms_table, sms_id_table)
        df = pd.read_sql(q, db_conn)
        show_df(df)
        df = pd.merge(allow_df_for_cp_join(df, cp_nb_char_precision), allow_df_for_cp_join(city_cp_df, cp_nb_char_precision))
    show_df(df)
    return df
    #show_df(df * 0.5, 6, -2)

def allow_df_for_cp_join(df, cp_nb_char_precision = 4):
    if 'cp' in list(df):
        if cp_nb_char_precision > 4:
            return df
        elif cp_nb_char_precision <= 4:
            df['cp_join'] = df['cp'].apply(lambda x: str(x)[:cp_nb_char_precision])
            df.drop('cp', axis = 1, inplace = True)
            return df

def sms_query_builder(cp_list = None, city_list = None, cp_nb_char_precision = 5,
                      age_min = None, age_max = None, civi = None,
                      sms_list = None,
                      count = True, groupby_count = False, groupby_count_depth = 1, groupby_count_field = None,
                      select_field = None, limit = None,
                      sms_table = 'sms', sms_id_table = 'id', print_query = True):
    field_dict = {'sms' : 's.sms AS sms',
                  'sms_id' : 's.id AS sms_id',
                  'cp' : 'id.cp AS cp',
                  'dpt' : 'LEFT(id.cp,2) AS dpt',
                  'cp2' : 'LEFT(id.cp,2) AS cp2',
                  'cp3' : 'LEFT(id.cp,3) AS cp3',
                  'cp4' : 'LEFT(id.cp,4) AS cp4',
                  'cp5' : 'id.cp AS cp5',
                  'ville' : 'id.ville AS ville',
                  'city' : 'id.ville AS city',
                  'nom' : 'id.nom AS nom',
                  'prenom' : 'id.prenom AS prenom',
                  'civi' : 'id.civilite AS civi',
                  'civilite' : 'id.civilite AS civilite',
                  'gender' : "CASE WHEN id.civilite=1 THEN 'M' WHEN id.civilite IN (2, 3) THEN 'F' ELSE 'None' END AS gender",
                  'genre' : "CASE WHEN id.civilite=1 THEN 'M' WHEN id.civilite IN (2, 3) THEN 'F' ELSE 'None' END AS genre",
                  'birth' : 'id.birth AS birth',
                  'age' : "DATE_PART('year', AGE(id.birth)) AS age",
                  'year_old' : "DATE_PART('year', AGE(id.birth)) AS year_old",
                  'mail' : 'b.mail AS mail',
                  'mail_id' : 'sm.mail_id AS mail_id',
                  'md5_mail' : 'md5.md5 AS md5_mail',
                  'mail_md5' : 'md5.md5 AS mail_md5',
                  'sms_md5' : 'MD5(s.sms) AS sms_md5',
                  'md5_sms' : 'MD5(s.sms) AS md5_sms'}
    from_clause = "FROM %s AS s JOIN sms_mail AS sm ON s.id = sm.sms_id RIGHT JOIN %s AS id ON id.mail_id = sm.mail_id " \
                  % (sms_table, sms_id_table)
    limit_clause = ""
    set_groupby_clause_after_where_analysis = False
    groupby_sql = ['s.id']
    if count:
        select_clause = "SELECT COUNT(DISTINCT s.id) AS nb_sms "
        groupby_clause = ""
        if groupby_count:
            set_groupby_clause_after_where_analysis = True
    else:
        select_clause = "SELECT DISTINCT ON (s.id) "
        if select_field:
            field_sql = []

            if isinstance(select_field, list):
                for field in select_field:
                    if field in field_dict.keys():
                        field_sql.append(field_dict[field])
                        groupby_sql.append(field)
            else:
                field = select_field
                if field in field_dict.keys():
                    field_sql.append(field_dict[field])
                    groupby_sql.append(field)
            if len(field_sql) > 0:
                if len(field_sql) == 1:
                    select_clause = select_clause + field_sql[0] + " "
                else:
                    select_clause = select_clause + ", ".join(field_sql) + " "
                groupby_clause = "GROUP BY %s " % ", ".join(groupby_sql)
                if 'b.mail' in select_clause:
                    from_clause = from_clause + "JOIN base AS b ON b.id = sm.mail_id "
                if "md5.md5" in select_clause:
                    from_clause = from_clause + "JOIN md5 ON md5.mail_id = sm.mail_id "
            else:
                select_clause = select_clause + field_dict['sms'] + " "
                groupby_sql.append('sms')
                groupby_clause = "GROUP BY %s" % ', '.join(groupby_sql)
        else:
            select_clause = select_clause + field_dict['sms'] + " "
            groupby_sql.append('sms')
            groupby_clause = "GROUP BY %s" % ', '.join(groupby_sql)
        if limit:
            if str(limit).isdigit():
                limit_clause = "LIMIT %s" % str(limit)

    where_list = []
    if age_min:
        if str(age_min).isdigit():
            where_list.append("(DATE_PART('year', AGE(id.birth)) >= %s) " % str(age_min))
    if age_max:
        if str(age_max).isdigit():
            where_list.append("(DATE_PART('year', AGE(id.birth)) <= %s) " % str(age_max))
    if civi:
        civi_arg = []
        if isinstance(civi,list):
            for arg in civi:
                if arg == 'M':
                    civi_arg.append('1')
                elif arg == 'F':
                    civi_arg.append('2')
                    civi_arg.append('3')
        else:
            if civi == 'M':
                civi_arg.append('1')
            elif civi == 'F':
                civi_arg.append('2')
                civi_arg.append('3')
        if len(civi_arg) > 0 and len(civi_arg) < 3:
            if len(civi_arg) == 1:
                where_list.append("(id.civilite = %s) " % civi_arg[0])
            else:
                civi_string = "(" + ",".join(civi_arg) + ")"
                where_list.append("(id.civilite IN %s) " % civi_string)
    if city_list:
        city_sql_list = []
        if isinstance(city_list, list):
            for city in city_list:
                city = str(city)
                if not city.isdigit():
                    city = clean_city_name(city)
                    city_sql_list.append("'" + city + "'")
        else:
            city = str(city_list)
            if not city.isdigit():
                city = clean_city_name(city)
                city_sql_list.append("'" + city + "'")
        if len(city_sql_list) > 0:
            if len(city_sql_list) > 1:
                where_list.append("(id.ville IN (%s)) " % ", ".join(city_sql_list))
            else:
                where_list.append("(id.ville = %s) " % city_sql_list[0])

    if sms_list:
        add_plus_sign = lambda x : x if x[0] == "+" else "+" + x
        sms_list = [add_plus_sign(sms) for sms in sms_list]
        parenthesis = lambda x : "('" + x + "')"
        sms_string = ", ".join([parenthesis(sms) for sms in sms_list])
        where_list.append("s.sms IN (VALUES %s)" % sms_string)

    if cp_list:
        if cp_nb_char_precision < 2 or cp_nb_char_precision > 5:
            cp_nb_char_precision = 5
        cp_sql_list = []
        if isinstance(cp_list, list):
            for cp in cp_list:
                if str(cp).isdigit():
                    cp = str(cp)
                    len_cp = len(cp)
                    if len_cp == 5:
                        if cp_nb_char_precision == 5:
                            cp_sql_list.append(cp)
                        elif cp_nb_char_precision < 5:
                            cp_sql_list.append(cp[:cp_nb_char_precision] + '%')
                    elif len_cp < 5:
                        if cp_nb_char_precision >= len_cp:
                            cp_sql_list.append(cp + '%')
                        elif cp_nb_char_precision < len_cp:
                            cp_sql_list.append(cp[:cp_nb_char_precision] + '%')
        else:
            if str(cp_list).isdigit():
                cp = str(cp_list)
                len_cp = len(cp)
                if len_cp == 5:
                    if cp_nb_char_precision == 5:
                        cp_sql_list.append(cp)
                    elif cp_nb_char_precision < 5:
                        cp_sql_list.append(cp[:cp_nb_char_precision] + '%')
                elif len_cp < 5:
                    if cp_nb_char_precision >= len_cp:
                        cp_sql_list.append(cp + '%')
                    elif cp_nb_char_precision < len_cp:
                        cp_sql_list.append(cp[:cp_nb_char_precision] + '%')
        if len(cp_sql_list) > 0:
            cp_sql_dict = {}
            for cpt in range(2,6):
                cp_sql_dict[cpt] = []
            for cp_sql in cp_sql_list:
                if '%' in cp_sql:
                    cp_sql_dict[len(cp_sql[:-1])].append("'" + cp_sql + "'")
                else:
                    cp_sql_dict[len(cp_sql)].append("'" + cp_sql + "'")
            cp_sql_expression = []
            for nb_char, sorted_cp_sql in cp_sql_dict.iteritems():
                if len(sorted_cp_sql) > 0:
                    if nb_char == 5:
                        if len(sorted_cp_sql) == 1:
                            cp_sql_expression.append("(id.cp = %s) " % sorted_cp_sql[0])
                        else:
                            cp_sql_expression.append("(id.cp IN (%s)) " % ", ".join(sorted_cp_sql))
                    else:
                        if len(sorted_cp_sql) == 1:
                            cp_sql_expression.append("(id.cp LIKE %s) " % sorted_cp_sql[0])
                        else:
                            cp_sql_expression.append("(id.cp LIKE ANY (ARRAY[%s])) " % ", ".join(sorted_cp_sql))
            if len(cp_sql_expression) > 0:
                if len(cp_sql_expression) > 1:
                    where_list.append("(" + "OR ".join(cp_sql_expression)[:-1] + ") ")
                else:
                    where_list.append(cp_sql_expression[0] + " ")

    if len(where_list) >= 1:
        where_clause = "WHERE " + "AND ".join(where_list)
    else:
        where_clause = ""

    if count:
        if groupby_count:
            select_sql = []
            groupby_sql = []
            if groupby_count_field:
                groupby_field_dict = {}
                for field in ['cp','ville','city','civi','civilite','gender','genre','age','year_old']:
                    groupby_field_dict[field] = field_dict[field]
                if isinstance(groupby_count_field, list):
                    for field in groupby_count_field:
                        if field in groupby_field_dict.keys():
                            select_sql.append(groupby_field_dict[field])
                            groupby_sql.append(field)
                else:
                    field = groupby_count_field
                    if field in groupby_field_dict.keys():
                        select_sql.append(groupby_field_dict[field])
                        groupby_sql.append(field)
            else:
                if city_list:
                    if len(city_sql_list) > 0:
                        select_sql.append(field_dict['ville'])
                        groupby_sql.append('ville')
                if cp_list:
                    if len(cp_sql_list) > 0:
                        if len(cp_sql_dict[2]) > 0:
                            select_sql.append("LEFT(id.cp,2) AS dpt")
                            groupby_sql.append("dpt")
                        else:
                            select_sql.append(field_dict['cp'])
                            groupby_sql.append("cp")
                if civi:
                    if len(civi_arg) > 0:
                        select_sql.append(field_dict['genre'])
                        groupby_sql.append("genre")
                    #if len(civi_arg) > 0: groupby_sql.append("CASE WHEN id.civilite=1 THEN 'M' WHEN id.civilite IN (2, 3) THEN 'F' ELSE 'None' END AS gender")
                if groupby_count_depth:
                    try:
                        select_sql = select_sql[:groupby_count_depth]
                        groupby_sql = groupby_sql[:groupby_count_depth]
                    except:
                        pass
            select_sql.append("COUNT(DISTINCT s.id) AS nb_sms ")
            select_clause = "SELECT " + ", ".join(select_sql)
            groupby_clause = "GROUP BY %s" % ', '.join(groupby_sql) + " ORDER BY nb_sms DESC"

    groupby_clause = groupby_clause.replace("age", "year_old")

    if print_query:
        print select_clause
        print from_clause
        print where_clause
        print groupby_clause
        print limit_clause

    if where_clause == "":
        query = "".join([select_clause, from_clause, groupby_clause]) + ";"
    else:
        query = "".join([select_clause, from_clause, where_clause, groupby_clause]) + ";"
    if limit_clause: query = query.replace(";", limit_clause + ";")
    return query

def get_cp_from_city(city_list, connection, id_table = 'postal_ville_cp', strict = False, debug = True):
    usual_city_dict = {'Paris' : ['Paris ' + digit_to_str(arrdt) for arrdt in range(1,21)],
                       'Neuilly' : 'Neuilly sur Seine',
                       'Boulogne' : 'Boulogne Billancourt',
                       'Levallois' : 'Levallois Perret',
                       'Velizy' : 'Velizy Villacoublay',
                       'Villiers' : 'Villiers le Bel',
                       'Reuil' : 'Reuil Malmaison'}
    if city_list:
        if isinstance(city_list, list):
            city_list = [usual_city_dict[city] if city in usual_city_dict.keys() else city for city in city_list]
            city_list = flatten_list(city_list)
            if len(city_list) > 1:
                if strict:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.ville IN (%s) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_city_for_cp_retrieval(city, id_table) + "'" for city in city_list]))
                else:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.ville LIKE ANY (ARRAY[%s]) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_city_for_cp_retrieval(city, id_table) + "%'" for city in city_list]))
            else:
                q = "SELECT DISTINCT ON (id.cp)id.ville, id.cp FROM %s AS id WHERE id.ville = '%s' GROUP BY id.cp, id.ville" % \
                    (id_table, format_city_for_cp_retrieval(city_list[0], id_table))
        else:
            if city_list in usual_city_dict.keys(): city_list = usual_city_dict[city_list]
            q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.ville = '%s' GROUP BY id.cp, id.ville" % \
                (id_table, format_city_for_cp_retrieval(city_list, id_table))
        if debug:
            print "--- Querying %s table to get corresponding cp ---" % str(id_table)
            print q
        df = pd.read_sql(q, connection)
        df = df.drop_duplicates()
        cp_list = list(df.cp.unique())
        if debug:
            #show_group_by_df(df, 'ville')
            show_df(df)
            print "%s cities --> %s records for %s distinct cp" % (str(len(city_list)), str(len(df.index)), str(len(cp_list)))
            #print cp_list
        return (cp_list, df)
    else:
        return False

def get_city_from_cp(cp_list, connection, id_table = 'postal_ville_cp', strict = True, debug = True):
    if cp_list:
        if isinstance(cp_list, list):
            if len(cp_list) > 1:
                if strict:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.cp IN (%s) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_cp(cp) + "'" for cp in cp_list]))
                else:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.cp LIKE ANY (ARRAY[%s]) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_cp(cp) + "%'" for cp in cp_list]))
            else:
                q = "SELECT DISTINCT ON (id.cp)id.ville, id.cp FROM %s AS id WHERE id.cp = '%s' GROUP BY id.cp, id.ville" % \
                    (id_table, format_cp(cp_list[0]))
        else:
            q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.cp = '%s' GROUP BY id.cp, id.ville" % \
                (id_table, format_cp(cp_list))
        if debug: print q
        df = pd.read_sql(q, connection)
        df = df.drop_duplicates()
        city_list = list(df.ville.unique())
        if debug:
            show_group_by_df(df, 'cp')
            show_df(df)
            print city_list
        return (city_list, df)
    else:
        return False

def flatten_list(input_list):
    temp_list = []
    for item in input_list:
        if isinstance(item, list):
            for subitem in item:
                temp_list.append(subitem)
        else:
            temp_list.append(item)
    return temp_list

def digit_to_str(value, nb_char = 2, prefix = '0'):
    value = str(value)
    l = len(value)
    if l < nb_char:
        return (prefix * (nb_char - l)) + value
    else:
        return value

def format_cp(value):
    if isinstance(value, basestring):
        return format_cp_string(value)
    elif isinstance(value, (int, long)):
        return format_cp_string(str(value))

def format_cp_string(value):
    if value.isdigit():
        len_value = len(str(value))
        if len_value == 5:
            return str(value)
        elif len_value == 4:
            value = "0" + str(value)
            return str(value)
        elif len_value == 3:
            value = "0" + str(value) + "0"
            return str(value)
        else:
            return ""
    else:
        return ""

def format_city_for_cp_retrieval(city, id_table):
    city = str(city).replace("\\'", "<'")
    if id_table == 'postal_ville_cp':
        return str(city).upper()
    else:
        return clean_city_name(str(city))

def format_region_for_cp_retrieval(region, id_table):
    return str(region).upper()

def get_neighbors(cp_list):
    pass

def clean_separator(value, separator_dict, universal_separator, city_mode = False):
    city_word = ['le', 'la', 'de', 'du', 'des', 'les', 'a', 'aux', 'lez', 'en', 'dans', 'sous', 'sur']
    particule_word = ['de', 'du', 'des']
    try:
        for separator, separator_list in separator_dict.iteritems():
            for item in separator_list:
                if item in value:
                    word_list = list(value.split(item))
                    new_word_list = []
                    for word in word_list:
                        word = word.strip()
                        if not word.isdigit():
                            if city_mode and word in city_word:
                                word = word.lower()
                            elif not city_mode and word in particule_word:
                                word = [word.lower(), 'particule']
                            else:
                                try:
                                    if "d'" in word:
                                        pos = word.find("d'")
                                        word = word[:pos+1].lower() + word[pos+2].upper() + word[pos+3:].lower()
                                    else:
                                        word = word[0].upper() + word[1:].lower()
                                except:
                                    pass
                            new_word_list.append(word)
                    if len(new_word_list) > 1:
                        if not city_mode:
                            for word in new_word_list:
                                if isinstance(word, list):
                                    if word[1] == 'particule':
                                        particule_list = []
                                        for word in new_word_list:
                                            if isinstance(word, list):
                                                particule_list.append(word[0].lower())
                                            else:
                                                particule_list.append(word[0].upper() + word[1:].lower())
                                        try:
                                            new_value  = str(" ".join(particule_list))
                                        except:
                                            new_value = value
                                        return new_value
                    try:
                        if universal_separator:
                            new_value = str(universal_separator.join(new_word_list))
                        else:
                            new_value = str(separator.join(new_word_list))
                    except:
                        new_value = value
                    return new_value
        if not value.isdigit():
            new_value = value[0].upper() + value[1:].lower()
            return new_value
        else:
            return value
    except:
        return value

def clean_city_name(value, separator_check = True, city_mode = True):
    separator_dict = {'-' : ['----', '---', '--', '-'], \
                      '/' : ['////', '///', '//', '/'], \
                      '_' : ['____', '___', '__', '_'], \
                      ":" : ["::::",":::", "::", ":"], \
                      "#" : ["####", "###", "##", "#"], \
                      '~' : ['~~~~', '~~~', '~~', '~'], \
                      "'" : ["''''","'''", "''"], \
                      ',' : [',,,,', ',,,', ',,'], \
                      ';' : [';;;;', ';;;', ';;']}
    if isinstance(value, basestring):
        value = ' '.join(value.split())
        value = value.replace('\\', '')
        value = value.replace('?', 'e')
        if separator_check and separator_dict:
            value = clean_separator(value, separator_dict, '-', city_mode = city_mode)
        else:
            if not value.isdigit():
                try:
                    value = value[0].upper() + value[1:].lower()
                except:
                    pass
        if len(value) > 40:
            value = value[:40]
        return value
    else:
        return ""

def show_df(df, n = 5, m = 0):
    if n>0:
        print df.head(n)
    if m>0:
        print df.loc[:-m].head(m)
    print str(len(df.index)) + " lines."

def show_group_by_df(df, group_by_col):
    if group_by_col in list(df):
        gr = df.groupby(group_by_col)
        for value, sub_df in gr:
            print value
            show_df(sub_df)

def cap_commun_size_on_group_by_col(df, group_by_col, n_sample):
    if group_by_col in list(df):
        return df.groupby(group_by_col).apply(lambda df: sample_or_same(df, n_sample))
    else:
        return False

def cap_multiple_size_group_by_col(df, group_by_col, n_sample_dict, debug = True):
    if group_by_col in list(df):
        grouped = df.groupby(group_by_col)
        sampled_group = []
        for col_value, col_value_df in grouped:
            if col_value in n_sample_dict.keys():
                n_sample = n_sample_dict[col_value]
                sampled_col_value_df = sample_or_same(col_value_df, n_sample)
                sampled_group.append(sampled_col_value_df)
        if len(sampled_group) > 0:
            sampled = pd.concat(sampled_group)
            if debug: show_group_by_df(sampled, group_by_col)
            return sampled
        else:
            print "Error : retry with keys in the dict in STRING"
            return False
    else:
        print "Error : %s NOT in column list !" % str(group_by_col)
        return False

def generate_sending_lists_case_nissan(df, group_by_col_sample, n_sample_dict, ref_col_send, send_cases,
                                       folder, debug = True):
    if debug: show_df(df)
    archive_df = df.copy(deep = True)
    for send_case in send_cases:
        df = df.loc[df[ref_col_send] == ""]
        send_df = cap_multiple_size_group_by_col(df, group_by_col_sample, n_sample_dict, debug = False)
        send_df['sms'].to_csv(folder + "/nissan_capped_for_%s.csv" % str(send_case), header = False, index = False)
        archive_df = tag_records_in_df_with_other_df(archive_df, send_df, tag_value = send_case, tag_col = ref_col_send)
        df = tag_records_in_df_with_other_df(df, send_df, tag_value = send_case, tag_col = ref_col_send)
    archive_df.to_csv(folder + "/full_extract_nissan_age_gender_send-dates.csv")


def tag_records_in_df_with_other_df(df, tag_df, tag_value = d.datetime.now().date().isoformat(),
                                    tag_col = "send_date", join_key = "sms"):
    tag_df = tag_df[[join_key]]
    res = map_existing_rows(df, tag_df, join_key = join_key)
    if res[0]:
        df = res[1]
        if tag_col not in list(df):
            df[tag_col] = ""
        df.loc[df.exist == True, tag_col] = tag_value
        #df.loc[df.exist == False, tag_col] = ""
        df.drop(['exist','new'], axis = 1, inplace = True)
        return df

def filter_df_with_other_df(df_ref, df_filter, join_key = 'mail', res_col = None, ref_name = None):
    initial_col = list(df_ref)
    res = map_existing_rows(df_ref, df_filter, join_key = join_key)
    if res[0]:
        df = res[1]
        if not res_col:
            res_col = initial_col
        df = df.loc[df['new'] == False, res_col]
        comment = 'results-%sk-after-filter-operation-of-%sk' % (str(len(df.index)/1000), str(len(df_filter.index)/1000))
        print "Below the resulting df after the filter :"
        print comment
        show_df(df)
        if ref_name:
            try:
                new_name = ref_name.replace('ref', comment)
            except:
                new_name = comment + "_" + ref_name
            return df, new_name
        else:
            return df, comment
    else:
        return False, None

def map_existing_rows(df, ref_df, join_key = 'md5', output = 'df', exist_key = 'exist', new_key = 'new'):
    ref_df['flag'] = True
    header = list(df.columns)
    df[exist_key] = False
    df[new_key] = False
    nb_records_dict = {}
    nb_records_dict['initial'] = len(df.index)
    ref_header = list(ref_df.columns)
    common_fields = [field for field in header if field in ref_header]
    if len(common_fields) == 0:
        message = "Pb. with map_existing_rows(). There are no fields common to the databases. Will return all records as new records."
        if output in ['row', 'rows', 'index']:
            return [True, df.index, ""]
        else:
            return [True, df, nb_records_dict]
    else:
        if isinstance(join_key, list):
            new_join_key = []
            for item in join_key:
                if item in common_fields:
                    new_join_key.append(item)
            if new_join_key != join_key:
                if new_join_key:
                    message = "Info : map_existing_rows(). Specified join_key '%s' has fields not common to the databases. " % str(join_key) + \
                            "Will use '%s' instead as join_key (this field(s) is(are) common to both tables)." % str(new_join_key)

                    join_key = new_join_key
                else:
                    message = "Info : map_existing_rows(). Specified join_key '%s' does not exist in both databases. " % str(join_key) + \
                            "Will use '%s' instead as join_key (ie. all field(s) common to both tables)." % str(common_fields)

                    join_key = common_fields
        else:
            if not join_key in common_fields:
                message = "Info : map_existing_rows(). Specified join_key '%s' does not exist in dataframes. " % str(join_key) + \
                            "Will use '%s' instead as join_key (this field is common to both tables)." % str(common_fields[0])

                join_key = common_fields[0]
        try:
            new_df = pd.merge(df, ref_df, on=join_key, how='left')
            message = "OK : Records matched using the fields %s." % str(join_key)

        except:
            message = "Pb. with pd.merge() : Unable to merge dataframes."

            e = sys.exc_info()
            for item in e:
                message = str(item)

            return [False]
        #show_df(new_df)
        common_rows_with_ref_df = new_df[new_df['flag'] == True].index
        new_rows_unknown_to_ref_df = new_df[new_df['flag'] != True].index
        new_df = new_df.drop('flag',1)
        if output in ['row', 'rows', 'index']:
            return [True, common_rows_with_ref_df, new_rows_unknown_to_ref_df]
        else:
            new_df.loc[common_rows_with_ref_df, exist_key] = True
            new_df.loc[new_rows_unknown_to_ref_df, new_key] = True
            show_df(new_df)
            print "Above is the result of merging on : " + str(join_key)
            nb_records_dict['sorted'] = len(new_df.index)
            nb_records_dict[exist_key] = len(common_rows_with_ref_df)
            nb_records_dict[new_key] = len(new_rows_unknown_to_ref_df)
            print str(nb_records_dict)
            return [True, new_df, nb_records_dict]

def create_path_if_does_not_exist(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print "path created :", str(path)
    else:
        print "path already existed :", str(path)

def sample_or_same(df, n_sample):
    if len(df.index) > n_sample:
        return df.sample(n = n_sample)
    else:
        return df

def sample_list(x_list, n):
    if n <= len(x_list):
        return random.sample(x_list, n)
    else:
        while len(x_list) <= n:
            x_list = x_list.extend(x_list)
        return random.sample(x_list, n)

def enlarge_sample(sample_dict, hlr_factor = 1.25, error_factor = 1.1, debug = True):
    enlarge_factor = hlr_factor * error_factor
    sample_dict_modified = None
    if isinstance(sample_dict, dict):
        sample_dict_modified = {}
        for k,v in sample_dict.iteritems():
            sample_dict_modified[k] = int(enlarge_factor * int(v))
    elif isinstance(sample_dict, list):
        sample_dict_modified = []
        for v in sample_dict:
            sample_dict_modified.append(int(enlarge_factor * int(v)))
    else:
        try:
            sample_dict_modified = int(enlarge_factor * int(sample_dict))
        except:
            print "Warning : Unable to modify the argument passed to 'enlarge_sample()' function"
            return sample_dict
    if sample_dict_modified:
        if debug:
            print "--- Modifying limits by factors : HLR = %s & Error = %s ---" % (str(hlr_factor), str(error_factor))
            print "client_limit %s becomes %s internally" % (str(sample_dict), str(sample_dict_modified))
        return sample_dict_modified

def filter_good_sms_hlr_file(folder, file, debug = True):
    df = pd.read_csv(folder + "/" + file, dtype = basestring)
    df['sms'] = df.MSISDN.apply(lambda num: "+" + str(num))
    if debug:
        print "--- Downloaded file after HLR Lookup : %s ---" % str(folder + "/" + file)
        show_df(df)
    res_df = df.loc[df['Error Code'] == '0', 'sms']
    if debug:
        show_df(res_df)
    res_df.to_csv(folder + "/" + file.replace(".csv", "ok.csv"))
    return res_df

def clean_router_df(router_df):
    for col in list(router_df):
        if "Champ" in col:
            router_df.drop(col, axis = 1, inplace = True)
    router_df.rename(columns = {router_df.columns[0]: "sms",
                                     router_df.columns[1]: "status",
                                     router_df.columns[2]: "send_date",
                                     router_df.columns[3]: "receive_date",
                                     router_df.columns[4]: "stop"}, inplace = True)
    try:
        router_df.rename(columns = {router_df.columns[5]: "unknown"}, inplace = True)
    except:
        pass
    return router_df

def create_age_range_df(age_range = [25,35,45,55,65], upper_limit = None):
    data = []
    for pos in range(len(age_range)):
        if pos == 0:
            age_min = 0
            age_max = age_range[pos]
        else:
            age_min = age_range[pos - 1]
            age_max = age_range[pos]
        label = "%s-%s" % (str(age_min), str(age_max - 1))
        #print str(pos), label
        for x in range(age_min, age_max):
            data.append([x,label])
    if upper_limit:
        age_min = age_range[-1]
        age_max = upper_limit
        label = "%s-%s" % (str(age_min), str(age_max))
        #print str(len(age_range)+1), label
        for x in range(age_min, age_max):
            data.append([x,label])
    res = pd.DataFrame(data, columns = ['age', 'age_range'])
    show_df(res, 30)
    return res

def change_global_debug_query_value(value = False):
    global debug_query
    debug_query = value
    return debug_query

def get_primary_stats(df, debug = False, write_to_file = False, folder = None, file = None, header = None):
    stats = collections.OrderedDict()
    stats['total_sms'] = len(df.index)
    stats['status_sum'] = 0
    if 'status' in list(df):
        status_df = df.groupby('status').count()
        if debug: print status_df['sms']
        for status in ['received', 'npai', 'temp_error']:
            try:
                stats[status] = status_df.get_value(status, 'sms')
            except KeyError:
                stats[status] = 0
            stats['status_sum'] += stats[status]
            stats[status + '_%'] = round(100 * int(stats[status]) / float(stats['total_sms']), 2)
    if 'stop' in list(df):
        stop_df = df.groupby('stop').count()
        if debug: print stop_df['sms']
        try:
            stats['stop'] = stop_df.get_value(1, 'sms')
        except KeyError:
            stats['stop'] = 0
        stats['stop_%'] = round(100 * int(stats['stop']) / float(stats['total_sms']), 2)
    #for item in ['received', 'npai', 'temp_error', 'stop']:
    #    stats[item + '_%'] = round(100 * int(stats[item]) / float(stats['total_sms']), 2)
    if stats['status_sum'] != stats['total_sms']:
        print "Warning ! the sum of status (%s) is not equal to the sum of the sms (%s)" % \
              (str(stats['status_sum']), str(stats['total_sms']))
        if not debug: debug = True
    if debug:
        print "Statistiques de campagne (chiffres bruts et en pourcentage sur %s SMS envoyes)" % str(stats['total_sms'])
        for k,v in sorted(stats.iteritems()):
            print k, ":", str(v)
    if write_to_file:
        with open(folder + "/" + file , 'ab+') as csv_file:
            w = csv.writer(csv_file, delimiter=';')
            if header: w.writerow([header])
            for k, v in sorted(stats.iteritems()):
                w.writerow([k, v])
            line_sep = '-'
            w.writerow([line_sep])
    return dict(stats)

def write_stats_alexandra(df, csv_writer, age_range_case = False, age_range = None, debug = False):
    res = get_primary_stats(df, debug = debug)
    if age_range_case:
        if not age_range: age_range = ""
        csv_writer.writerow([str(age_range), str(res['total_sms']), str(res['temp_error']), str(res['temp_error_%']),
                             str(res['npai']), str(res['npai_%']), str(res['received']), str(res['received_%'])])
        #if age_range == "TOTAL" : csv_writer.writerow(["-"])
    else:
        csv_writer.writerow(['ENVOYES', str(res['total_sms']), '100 %'])
        csv_writer.writerow(['RECUS', str(res['received']), str(res['received_%']) + " %"])
        csv_writer.writerow(['EXPIRES', str(res['temp_error']), str(res['temp_error_%']) + " %"])
        csv_writer.writerow(['ECHEC', str(res['npai']), str(res['npai_%']) + " %"])
        csv_writer.writerow(['EN ATTENTE', "0", "0 %"])
        csv_writer.writerow(["-"])
    return res

def create_incr():
    incr = {}
    for field in ['total_sms', 'received', 'temp_error', 'npai']:
        incr[field] = 0
    return incr

def add_up_stats_res(incr, res):
    for field in ['total_sms', 'received', 'temp_error', 'npai']:
        incr[field] += res[field]
    return incr

def calc_pourcentage_in_incr(incr):
    for field in ['received', 'temp_error', 'npai']:
        incr[field + "_%"] = round(100 * int(incr[field]) / float(incr['total_sms']), 2)
    return incr

def shake_dict(my_dict, amplitude = 0.4):
    new_dict = {}
    if amplitude == 0:
        for k, v in my_dict.iteritems():
            try:
                new_dict[k] = v * random.uniform((1 - amplitude), (1 + amplitude))
            except:
                new_dict[k] = v
        return new_dict
    else:
        return my_dict

def pretty(value, htchar='\t', lfchar='\n', indent=3):
    nlch = lfchar + htchar * (indent + 1)
    if type(value) is dict:
        items = [
            nlch + repr(key) + ': ' + pretty(value[key], htchar, lfchar, indent + 1)
            for key in value
        ]
        return '{%s}' % (','.join(items) + lfchar + htchar * indent)
    elif type(value) is list:
        items = [
            nlch + pretty(item, htchar, lfchar, indent + 1)
            for item in value
        ]
        return '[%s]' % (','.join(items) + lfchar + htchar * indent)
    elif type(value) is tuple:
        items = [
            nlch + pretty(item, htchar, lfchar, indent + 1)
            for item in value
        ]
        return '(%s)' % (','.join(items) + lfchar + htchar * indent)
    else:
        return repr(value)

def read_string_dates(input, isoformat = True, milliseconds = False, out_format = '%d/%m/%y', dayfirst = True):
    if input:
        try:
            parsed_date = dparser.parse(input, fuzzy = True, dayfirst = dayfirst)
            if isoformat:
                if milliseconds:
                    return parsed_date.isoformat()
                else:
                    return parsed_date.replace(microsecond=0).isoformat()
            else:
                return parsed_date.strftime('%d/%m/%y')
        except:
            print "Warning in read_string_dates() function: unable to convert : %s" % str(input)
    else:
        return ""

def hash_to_md5(string):
    string = str(string).lower().encode()
    hash_object = hashlib.md5(string)
    return hash_object.hexdigest()