# -*- coding: UTF-8 -*-
from __future__ import division
import os, sys
import datetime as d
import numpy as np
import pandas as pd
from pandas.io import sql
import json
import psycopg2
import psycopg2.pool
import random
import requests
from threading import Thread
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta

from sms_function_toolbox import *
from hlr_batch import *

use_celery = False
debug_query = True

# Threading class
class ThreadReturn(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
    def join(self):
        Thread.join(self)
        return self._return

class SMSPgSQL(object):
    db_dict =  {'dbname' : "prod", #"postgres"
                'user' : "postgres",
                'host' : "163.172.19.9", #"163.172.19.9",
                'pass' : "penny9690"}

    def __init__(self, config_path = None):
        self.config_path = config_path
        if self.config_path:
            self.import_config()
        self.pool = None
        self.initiate_threaded_connection_pool()
        if not self.pool: print "No Connection to postgreSQL. Check settings in Connection() class."

    def import_config(self):
        pass

    def initiate_threaded_connection_pool(self):
        connect_token = "dbname='" + self.db_dict['dbname'] + "' user='" + self.db_dict['user'] + \
                        "' host='" + self.db_dict['host'] + "' password='" + self.db_dict['pass'] + "'"
        try:
            conn_pool = psycopg2.pool.ThreadedConnectionPool(1, 1000, connect_token)
        except:
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print message
                print "Will try to connect with 'localhost' instead of", str(self.db_dict['host'])
            try:
                self.db_dict['host'] = 'localhost'
                connect_token = "dbname='" + self.db_dict['dbname'] + "' user='" + self.db_dict['user'] + \
                        "' host='" + self.db_dict['host'] + "' password='" + self.db_dict['pass'] + "'"
                conn_pool = psycopg2.pool.ThreadedConnectionPool(1,100, connect_token)
            except:
                e = sys.exc_info()
                for item in e:
                    message = str(item)
                    print message
                    self.pool = False
        message = "OK : Threaded connection pool established with DB."
        #print message
        self.pool = conn_pool

    def get_connection(self):
        con = self.pool.getconn()
        return con

    from contextlib import contextmanager

    @contextmanager
    def connection(self):
        con = self.pool.getconn()
        try:
            yield con
        finally:
            self.pool.putconn(con)

    @contextmanager
    def cursor(self, prepared = False):
        con = self.pool.getconn()
        try:
            #if prepared:
                #yield con.cursor(cursor_factory=PreparingCursor)
            #else:
            yield con.cursor()
        finally:
            self.pool.putconn(con)

    def truncate(self, table):
        if table in ['campagne_tag', 'tag_ouvreur', 'base_ouvreur', 'base_injection_mindbaz', 'id_origin_unik', 'id_unik']:
            with self.connection() as connection:
                truncate = "TRUNCATE TABLE %s RESTART IDENTITY;" % table
                cursor = connection.cursor()
                cursor.execute(truncate)
                connection.commit()

# Initialize on module import
#with Lock() as connection_pool_lock:
pg = SMSPgSQL()

class CopyToQuery(object):

    def __init__(self, df, query, where_id, table, join_key = None, args_dict = None, connection = None,
                 csv_file_prefix = None, csv_file_postfix = None):
        # Establishes DB connection
        self.close_connection = False
        self.connection = connection
        if not self.connection:
            self.get_connection()
            self.close_connection = True
        cursor = self.connection.cursor()
        # Retrieves records to be eliminited from df (ie. possible duplicates in the table)
        # Builds query based on the value of where_id. If none uses the query as it was given
        if query:
            if where_id:
                if isinstance(where_id, list):
                    q = query % tuple([str(id) for id in where_id])
                else:
                    q = query % str(where_id)
            else:
                q = query
            print "### Querying '%s' -- This may take up to one minute, be patient... ###" % q
            ref_df = pd.read_sql(q, self.connection, coerce_float=False)
            try:
                for col in list(ref_df):
                    if "_id" in col:
                        ref_df = convert_dataframe_column_to_integer(ref_df, col)
            except:
                ref_df = convert_dataframe_column_to_integer(ref_df, 'sms_id')
            # Eliminates possible duplicates
            if not join_key: join_key = 'sms_id'
            df = df.drop_duplicates(join_key)
            res = map_existing_rows(df, ref_df, join_key = join_key)
            #res = map_existing_rows(df, ref_df, join_key = ['mail_id', 'sms_id'])
            if res[0]:
                matched_df = res[1]
                to_be_inserted_df = matched_df[matched_df['new'] == True]
                # Deals with the columns of the resulting df in view of insert into the DB table
            #to_be_inserted_df = pd.merge(df, ref_df)
            # If args_dict, parses the dict to append 'key' column with fixed 'value' for all records
            if args_dict:
                 # By default, keeps only mail_id
                col_list = ['mail_id']
                for col, value in args_dict.iteritems():
                    print col, value
                    to_be_inserted_df[col] = value
                col_list.extend(list(args_dict))
                # Suppresses all columns not in col_list (ie. remain only 'mail_id' and the keys in args_dict)
                to_be_inserted_df = to_be_inserted_df[col_list]

            else:
                to_be_inserted_df = to_be_inserted_df.drop(['exist', 'new'], axis=1)
        else:
            to_be_inserted_df = df
            # Deals with the columns of the resulting df in view of insert into the DB table
            # If args_dict, parses the dict to append 'key' column with fixed 'value' for all records
            if args_dict:
                for col, value in args_dict.iteritems():
                    print col, value
                    to_be_inserted_df[col] = value



        import_df_to_DB(to_be_inserted_df, table, csv_file_prefix, csv_file_postfix,
                        self.connection, self.connection.cursor())

        """show_df(to_be_inserted_df)
        csv_columns = list(to_be_inserted_df.columns)
        file_name = create_file_name('Python', table, len(to_be_inserted_df.index), \
                                     header = csv_columns, comment = "new-records")
        path = path_containing_all_csv
        folder = table
        csv_file = write_to_csv(to_be_inserted_df, path, folder, file_name)
        if csv_file:
            csv_reader = codecs.open(csv_file, 'r', encoding='utf-8')
            cursor.copy_from(file = csv_reader, table = table,
                                   sep = ";", null = "", columns = csv_columns)
            connection.commit()
            message = "OK : csv imported to DB in table : " + table"""

    def get_connection(self):
        self.close_connection = True
        self.connection = pg.get_connection()
        self.cursor = self.connection.cursor()

    def __del__(self):
        if self.close_connection: self.connection.close()

class Query(object):
    def __init__(self, cursor, connection, query_dict, query_name, args, return_result = True, multi_result = False):
        self.cursor = cursor
        self.connection = connection
        self.query_dict = query_dict
        self.query_name = query_name
        self.query = self.query_dict[self.query_name]
        self.args = args
        if not isinstance(self.args, list): self.args = [self.args]
        self.arg_tuple = tuple([arg if arg is None else (None if str(arg) == 'None' else str(arg)) for arg in self.args])
        self.return_result = return_result
        self.multi_result = multi_result
        self.new_connection = False
        self.new_cursor = False
        self.execute_fail = False
        self.records = None

    def eval_records(self):
        if not bool(self.records):
            if debug_query:
                print "    --> ", self.query_name, ":", 'False : bool(records) = False'
                print '---------------------'
            return [False]
        else:
            if isinstance(self.records, tuple):
                res_list = [cleanup_string(item) for item in list(self.records)]
                if debug_query:
                    print "    --> ", self.query_name, ":", 'True'
                    print "    --> ", self.query_name, ":", res_list
                    print '---------------------'
                return [True, res_list]
            elif isinstance(self.records, list):
                if debug_query:
                    print "    --> ", self.query_name, ":", 'True'
                    print "    --> ", self.query_name, ":", str(self.records)
                res_list_list = []
                for result in self.records:
                    if isinstance(result, tuple):
                        res_list = [cleanup_string(item) for item in list(result)]
                        if self.multi_result and len(res_list) == 1:
                            res_list_list.append(res_list[0])
                        else:
                            res_list_list.append(res_list)
                return [True, res_list_list]
            else:
                if debug_query:
                    print "    --> ", self.query_name, ":", 'False : records is not a tuple'
                    print '---------------------'
                return [False]

    def open_missing_connection_or_cursor(self):
        if self.connection:
            if not self.cursor:
                self.new_cursor = True
                if debug_query:
                    print "open new cursor (cursor did not exist)"
                # In the latest production config, 'prepare' = True everywhere, so i don't bother with False case
                self.cursor = self.connection.cursor(cursor_factory=PreparingCursor)
            else:
                if self.cursor.closed:
                    self.new_cursor = True
                    if debug_query:
                        print "open new cursor (cursor was closed)"
                    self.cursor = self.connection.cursor(cursor_factory=PreparingCursor)
                else:
                    # This is the normal case : connection & cursor exist, and cursor is not closed
                    pass
        else:
            self.new_connection = True
            if debug_query:
                print "open new connection (connection did not exist)"
            self.connection = pg.pool.getconn()
            self.new_cursor = True
            if debug_query:
                print "open new cursor (connection & cursor did not exist)"
            self.cursor = self.connection.cursor(cursor_factory=PreparingCursor)

    def close_missing(self):
        try:
            if self.new_cursor: self.cursor.close()
            if self.new_connection: pg.pool.putconn(self.connection)
        except:
            pass

    def execute(self):
        """ Attempts to execute the query with cursor.execute() """
        self.open_missing_connection_or_cursor()
        if debug_query: print 'pg', self.query_name, self.arg_tuple
        try:
            self.cursor.execute(self.query, self.arg_tuple)
        #except Exception, e:
        #    print e.pgerror
        except psycopg2.IntegrityError as exc:
            self.execute_fail = True
            self.connection.rollback()
            if use_celery:
                if debug_query: print "    --> ", self.query_name, ":", '---- Execute query FAILED --> RETRY entire task ----'
                raise exc
            else:
                if debug_query:
                    print "    --> ", self.query_name, ":", '---- Execute query FAILED ----'
                    e = sys.exc_info()
                    for item in e:
                        message = str(item)
                        print message
        # Commits transaction, or reverts to backup query
        if self.execute_fail:
            if 'insert' in self.query_name:
                # This happens when the another worker inserted the record in between the two queries
                # In this case will return the id of the record inserted by the other worker
                if self.return_result:
                    if use_celery:
                        pass
                    else:
                        return self.execute_backup()
        else:
            if 'insert' in self.query_name or 'update' in self.query_name:
                try:
                    self.connection.commit()
                    if debug_query: print "    --> ", self.query_name, ":", 'Commit successful'
                except psycopg2.IntegrityError as exc:
                    if use_celery:
                        if debug_query: print "    --> ", self.query_name, ":", '---- Execute query FAILED --> RETRY entire task ----'
                        raise exc
                    else:
                        if debug_query:
                            print "    --> ", self.query_name, ":", '---- Commit FAILED ----'
                            e = sys.exc_info()
                            for item in e:
                                message = str(item)
                                print message
                            print '---------------------'
                        return [False]
        # Returns result if needed
        if self.return_result:
            if not self.multi_result:
                #try:
                self.records = self.cursor.fetchone()
                self.close_missing()
                return self.eval_records()
                #except:
                #    if debug_query:
                #        print "    --> ", self.query_name, ":", 'False : No results to fetch'
                #        print '---------------------'
                #    self.close_missing()
                #    return [False]
            else:
                #try:
                self.records = self.cursor.fetchall()
                self.close_missing()
                return self.eval_records()
                #except:
                #    if debug_query:
                #        print "    --> ", self.query_name, ":", 'False : No results to fetch'
                #        print '---------------------'
                #    self.close_missing()
                #    return [False]
        else:
            self.close_missing()

    def execute_backup(self):
        return_value = None
        if debug_query:
            print "    --> ", self.query_name, ":", '---- Will REVERT to query to return result ----'
        try:
            if self.query_name == 'insert_mail':
                result = unpack(Query(self.cursor, self.connection, self.query_dict, 'query_mail',
                                      [self.args[1]], True).execute())
                if result:
                    return_value = [True, [result]]
            elif self.query_name == 'insert_md5':
                result = unpack(Query(self.cursor, self.connection, self.query_dict, 'query_md5_id',
                                      [self.args[0]], True).execute())
                if result:
                    return_value = [True, [result]]
            elif self.query_name == 'insert_ip':
                if self.args[9]:
                    if self.args[9] == 'client':
                        result = unpack(Query(self.cursor, self.connection, self.query_dict, 'query_ip_id_client',
                                              [self.args[0], self.args[1], self.args[9]], True).execute())
                        if result:
                            return_value = [True, [result]]
                    else:
                        result = unpack(Query(self.cursor, self.connection, self.query_dict, 'query_ip_id_retargeter',
                                              [self.args[0], self.args[9]], True).execute())
                        if result:
                            return_value = [True, [result]]
            elif self.query_name == 'insert_connection':
                result = unpack(Query(self.cursor, self.connection, self.query_dict, 'query_connection_id',
                                      [self.args[0], self.args[1], self.args[4], self.args[4], 'redirect'], True).execute())
                if result:
                    return_value = [True, [result]]
            else:
                pass
        except:
            pass
        self.close_missing()
        if return_value:
            return return_value
        else:
            return [False]

class SMS(object):
    query_dict = {'insert_sms_md5' : "INSERT INTO sms (sms, sms_md5, country) VALUES (%s, %s, %s)",
                  'insert_sms_mail' : "INSERT INTO sms_mail (sms_id, mail_id, fichier_id) VALUES (%s, %s, %s)",
                  "select_sms_id" : "SELECT id FROM sms WHERE sms_md5 = %s",
                  'insert_sms_lookup' : "INSERT INTO sms_lookup (sms_id, valid, create_date, error_code, original_network, " + \
                                        "current_network, current_country, roaming_country, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"}
    def __init__(self, sms = None, sms_md5 = None, sms_id = None, connection = None, cursor = None):
        self.sms = sms
        self.sms_md5 = sms_md5
        self.sms_id = sms_id
        self.close_connection = False
        self.connection = connection
        self.cursor = cursor
        if not self.connection:
            self.connection = pg.get_connection()
            self.cursor = self.connection()
            self.close_connection = True

    def __del__(self):
        if self.close_connection: self.connection.close()

    def insert(self, country = 'FR'):
        if self.sms:
            if '+' in self.sms:
                if not self.sms_md5: self.sms_md5 = hash_to_md5(self.sms)
                args = [self.sms, self.sms_md5, country]
                res = Query(self.cursor, self.connection, self.query_dict, "insert_sms_md5", args, True).execute()
                if res[0]:
                    self.sms_id = unpack(res)

    def get_sms_id(self):
        if not self.sms_md5:
            if '+' in self.sms: self.sms_md5 = hash_to_md5(self.sms)
        if self.sms_md5:
            res = Query(self.cursor, self.connection, self.query_dict, "select_sms_id", [self.sms_md5], True).execute()
            if res[0]:
                self.sms_id = unpack(res)
            else:
                self.insert()

    def hlr_update(self, valid, date_checked, error_code, original_network, current_network, current_country,
                   roaming_country, type):
        if not self.sms_id: self.get_sms_id()
        if self.sms_id:
            args = [self.sms_id, valid, date_checked, error_code, original_network, current_network, current_country,
                   roaming_country, type]
            res = Query(self.cursor, self.connection, self.query_dict, "insert_sms_lookup", args, False).execute()
            print res

class SMSClient(object):
    query_dict = {'insert_client' : "INSERT INTO sms_client(name, create_date, parent_id, contact, admin_contact, fid_cpm_price, " + \
                                    "acg_cpm_price, sms_referral_id, created_by) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  'select_client_by_name' : 'SELECT * FROM sms_client WHERE name = %s',
                  'select_client_by_id' : 'SELECT * FROM sms_client WHERE id = %s'}

    def __init__(self, connection = None, cursor = None, name = None, create_date = None, parent_id = None, contact = None,
                 admin_contact = None, fid_cpm_price = None, acq_cpm_price = None, sms_referral_id = None, created_by = None):
        self.cursor = cursor
        self.connection = connection
        if not self.connection:
            self.close_connection = True
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()
        else:
            self.close_connection = False
        self.id = None
        self.name = name
        self.create_date = create_date
        self.parent_id = parent_id
        self.contact = contact
        self.admin_contact = admin_contact
        self.fid_cpm_price = fid_cpm_price
        self.acq_cpm_price = acq_cpm_price
        self.sms_referral_id = sms_referral_id
        self.created_by = created_by

    def insert(self):
        q = Query(self.cursor, self.connection, self.query_dict, 'select_client', [self.name], True)
        self.id = unpack(q.execute())
        if not self.id:
            args = [self.name, self.create_date, self.parent_id, self.contact, self.admin_contact, self.fid_cpm_price,
                    self.acg_cpm_price, self.sms_referral_id, self.created_by]
            q = Query(self.cursor, self.connection, self.query_dict, "insert_client", args, True)
            self.id = unpack(q.execute())

    def get(self, id = None, name = None):
        if id:
            q = Query(self.cursor, self.connection, self.query_dict, 'select_client_by_id', [str(id)], True)
            res = unpack(q.execute())
        elif name:
            q = Query(self.cursor, self.connection, self.query_dict, 'select_client_by_name', [name], True)
            res = unpack(q.execute())
        else:
            return False
        if res:
            self.id, self.name, self.create_date, self.parent_id, self.contact, self.admin_contact, self.fid_cpm_price, \
            self.acq_cpm_price, self.sms_referral_id, self.created_by = res
            return True

    def __del__(self):
        if self.close_connection: self.connection.close()

class SMSReferral(object):
    query_dict = {'insert_referral' : "INSERT INTO sms_referral(name, contact, admin_contact, create_date, created_by, "
                                      "fid_commission, acq_commission) VALUES ( %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  'select_referral' : 'SELECT id FROM sms_referral WHERE name = %s'}

    def __init__(self, connection = None, cursor = None, name = None, create_date = None, contact = None, admin_contact = None,
                 fid_commission = None, acq_commission = None, created_by = None ):
        self.cursor = cursor
        self.connection = connection
        if not self.connection:
            self.close_connection = True
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()

        else:
            self.close_connection = False
        self.name = name
        self.create_date = create_date
        self.contact = contact
        self.admin_contact = admin_contact
        self.fid_commission = fid_commission
        self.acq_commission = acq_commission
        self.created_by = created_by

    def insert(self):
        q = Query(self.cursor, self.connection, self.query_dict, 'select_referral', [self.name], True)
        self.id = unpack(q.execute())
        if not self.id:
            args = [self.name, self.create_date, self.contact, self.admin_contact, self.fid_commission, self.acg_commission, self.created_by]
            q = Query(self.cursor, self.connection, self.query_dict, "insert_referral", args, True)
            self.id = unpack(q.execute())

    def __del__(self):
        if self.close_connection: self.connection.close()

class HLR(object):
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    url = {'bulk' : 'https://www.hlrlookup.com/api/bulk/',
           'process' : 'https://www.hlrlookup.com/api/process/%s',
           'status' : 'https://www.hlrlookup.com/api/status/%s',
           'download' : 'https://www.hlrlookup.com/api/download/%s',
           'hlr' : 'https://www.hlrlookup.com/api/hlr/'}
    apikey = "JlqVVuRPYTi6vTsBQwDom8KDKkteKVHU"
    password = "david9690"
    max_msisdn = 80
    col_names = ['sms', 'status', 'error_code', 'original_network', 'current_network', 'current_country',
                 'roaming_country', 'type', 'date_checked']
    query_dict = {'insert_sms' : "INSERT INTO sms_lookup (sms_id, valid, create_date, error_code, original_network, " + \
                                 "current_network, current_country, roaming_country, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  "select_sms_id" : "SELECT id FROM sms WHERE sms_md5 = %s"}

    def __init__(self, sms_list = None):
        self.connection = None
        self.close_connection = False
        self.sms_list = sms_list
        if not isinstance(sms_list, list):
            self.sms_list = [sms_list]
        else:
            self.sms_list = sms_list
        self.clean_sms_list = None
        self.msisdn = None
        self.res_df = None
        self.batch_id = None
        self.batch_status = None

    def read_file(self, folder = None, file = None):
        print "--- Loading into dataframe : %s ---" % str(file)
        self.res_df = pd.read_csv(folder + "/" + file, dtype = basestring)
        #show_df(self.res_df)

    def clean_dataframe(self):
        try:
            #self.res_df.rename(columns = {"MSISDN" : 'sms'}, inplace = True)
            self.res_df['sms'] = self.res_df.MSISDN.apply(lambda num: "+" + str(num))
            self.res_df['sms_md5'] = self.res_df.sms.apply(lambda x : hash_to_md5(x))
            self.res_df['valid'] = self.res_df['Error Code'].apply(lambda x : True if str(x) == '0' else False)
            self.res_df['Date Checked'] = self.res_df['Date Checked'].apply(lambda x : read_string_dates(str(x)[:-15]))
            #show_df(self.res_df)
            if len(self.res_df.index) > 0:
                return True
            else:
                return False
        except:
            return False

    def format_dataframe_for_import_in_db(self, ref_df = None):
        col_dict = {'valid':'valid', 'Date Checked':'create_date', 'sms_md5':'sms_md5', 'Original Network':'original_network',
                    'Current Network':'current_network', "Error Code":'error_code', 'Current Country':'current_country',
                    'Roaming Country':'roaming_country', 'Type':'type'}
        self.res_df = self.res_df[col_dict.keys()]
        self.res_df.rename(columns=col_dict, inplace=True)
        show_df(self.res_df)
        if not ref_df:
            self.get_connection()
            query = 'SELECT id AS sms_id, sms_md5 FROM sms WHERE sms_md5 IN (%s)' % \
                    str(", ".join(["'%s'" % md5 for md5 in self.res_df['sms_md5'].values.tolist()]))
            ref_df = load_query(self.connection, query)
        self.res_df = pd.merge(self.res_df, ref_df, on = 'sms_md5')
        self.res_df.drop('sms_md5', axis = 1, inplace = True)
        show_df(self.res_df)

    def import_dataframe_in_db(self):
        self.format_dataframe_for_import_in_db()
        self.rejected_df = self.res_df.loc[self.res_df['valid'] == False, ['sms_id', 'create_date']]
        self.rejected_df['reason'] = "HLRLookup"
        show_df(self.rejected_df)
        query = "SELECT DISTINCT sms_id FROM sms_lookup"
        CopyToQuery(self.res_df, query, None, 'sms_lookup', 'sms_id', None, self.connection, "HLRLookup", None)
        query = "SELECT DISTINCT sms_id FROM sms_rejected"
        CopyToQuery(self.rejected_df, query, None, 'sms_rejected', 'sms_id', None, self.connection, "HLRLookup", None)

    def process_file(self, folder = None, file = None, write_in_db = False, connection = None, debug = True):
        self.res_df = pd.read_csv(folder + "/" + file, dtype = basestring)
        show_df(self.res_df)
        nb_checked = len(self.res_df.index)
        self.res_df.rename(columns = {"Unnamed: 0" : 'sms'}, inplace = True)
        #self.res_df['sms'] = self.res_df.MSISDN.apply(lambda num: "+" + str(num))
        self.res_df['sms_md5'] = self.res_df.sms.apply(lambda x : hash_to_md5(x))
        self.res_df['valid'] = self.res_df['Error Code'].apply(lambda x : True if str(x) == '0' else False)
        self.res_df['Date Checked'] = self.res_df['Date Checked'].apply(lambda x : read_string_dates(str(x)[:-15]))
        show_df(self.res_df)
        if debug:
            print "--- Downloaded file after HLR Lookup : %s ---" % str(folder + "/" + file)
            show_df(self.res_df)
        self.valid_df = pd.DataFrame()
        self.valid_df['sms'] = self.res_df.sms.loc[self.res_df.valid == True]
        nb_valid = len(self.valid_df.index)
        valid_rate = nb_valid / nb_checked
        if debug:
            print "%s valid SMS in %s checked" % (str(nb_valid), str(nb_checked))
            show_df(self.valid_df)
        self.valid_df.to_csv(folder + "/" + file.replace(".csv", "_ok.csv"), index = False)
        if debug:
            print "Result HLR file written in %s" % str(folder + "/" + file.replace(".csv", "_ok.csv"))
        if write_in_db: self.write_file_in_db(self.valid_df, connection = connection, debug = debug)
        return self.valid_df

    def write_file_in_db(self, df = None, folder = None, file = None, connection = None, debug = True):
        if folder:
            df = pd.read_csv(folder + "/" + file, dtype = basestring)
            nb_checked = len(df.index)
            df.rename(columns = {"Unnamed: 0" : 'sms'}, inplace = True)
            #df['sms'] = df.MSISDN.apply(lambda num: "+" + str(num))
            df['sms_md5'] = df.sms.apply(lambda x : hash_to_md5(x))
            df['valid'] = df['Error Code'].apply(lambda x : True if str(x) == '0' else False)
            df['Date Checked'] = df['Date Checked'].apply(lambda x : read_string_dates(x))
        if debug:
            print "--- x --- Writing HLR Result in DB --- x ----"
            show_df(df)
        if not connection:
            self.get_connection()
            connection = self.connection
            cursor = self.cursor
        else:
            cursor = connection.cursor()
        arg_list = ['valid', 'Date Checked', 'Error Code', 'Original Network', 'Current Network',
                    'Current Country', 'Roaming Country', 'Type']
        change_global_debug_query_value(False)
        for n in tqdm(range(len(df.index))):
            sms = SMS(df.at[n, 'sms'], df.at[n, 'sms_md5'], None, connection, cursor)
            args = list(df.loc[n, arg_list])
            sms.hlr_update(*args)
        change_global_debug_query_value(True)

    def __del__(self):
        if self.close_connection: self.connection.close()

    def get_connection(self):
        if not self.connection:
            self.close_connection = True
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()

    def verify(self, sub_list = False, sub_sms_list = None, use_requests_params = False):
        local_sms_list = None
        if sub_list:
            if sub_sms_list:
                if not isinstance(sub_sms_list, list):
                    local_sms_list = [sub_sms_list]
                else:
                    local_sms_list = sub_sms_list
                if len(local_sms_list) > self.max_msisdn:
                    local_sms_list = local_sms_list[0:self.max_msisdn]
        else:
            if self.sms_list: local_sms_list = self.sms_list

        if local_sms_list:
            if len(local_sms_list) > self.max_msisdn:
                local_sms_list = local_sms_list[0:self.max_msisdn]
            remove_plus_sign = lambda x: x[1:] if x[0] == '+' else x
            local_clean_sms_list = [remove_plus_sign(x) for x in local_sms_list]
            msisdn = str(",").join(local_clean_sms_list)
            payload = {'apikey' : self.apikey,
                       'password' : self.password,
                       'msisdn' : str(msisdn)}
            if use_requests_params:
                post = requests.post(self.url['hlr'], params = payload, headers = self.headers)
            else:
                query_string = "?"
                for k, v in payload.iteritems():
                    query_string = query_string + "&" + str(k) + "=" + str(v)
                post = requests.post(self.url['hlr'] + query_string, headers = self.headers)
            print post.url
            res = post.json()
            print res
            if res:
                date_checked = d.datetime.now().date().isoformat()
                record_list = []
                if len(local_clean_sms_list) == 1:
                    data = res
                    sms = local_clean_sms_list[0]
                    record_list.append(["+" + str(sms), True if data['status'] == 'Delivered' else False,
                                        data['error_code'], data['issueing_info']['network_name'],
                                        data['home_info']['network_name'], data['home_info']['location'],
                                        data['roaming_info']['location'], 'Mobile', date_checked])

                else:
                    for sms in local_clean_sms_list:

                        data = res[sms]
                        """
                        record = {}
                        record['sms'] = "+" + str(sms)
                        record['status'] = True if data['status'] == 'Delivered' else False
                        record['error_code'] = data['error_code']
                        record['original_network'] = data['issueing_info']['network_name']
                        record['current_network'] = data['home_info']['network_name']
                        record['current_country'] = data['home_info']['location']
                        record['roaming_country'] = data['roaming_info']['location']
                        record['type'] = data['type']
                        record['date_checked'] = date_checked
                        """
                        record_list.append(["+" + str(sms), True if data['status'] == 'Delivered' else False,
                                            data['error_code'], data['issueing_info']['network_name'],
                                            data['home_info']['network_name'], data['home_info']['location'],
                                            data['roaming_info']['location'], 'Mobile', date_checked])
                print record_list
                df = pd.DataFrame(data = record_list, columns = self.col_names)
                show_df(df)
                return df

    def upload_batch(self, batchname = 'batch', process_now = True, debug = True, p_cache = 30, s_cache = 15, save_to_shared = True):
        if self.sms_list:
            remove_plus_sign = lambda x: x[1:] if x[0] == '+' else x
            local_clean_sms_list = [remove_plus_sign(str(x)) for x in self.sms_list]
            if debug:
                print "--- x --- Uploading batch --- x ---"
                print str(local_clean_sms_list)
            msisdn = str(",").join(local_clean_sms_list)
            payload = {'batchname' : str(batchname),
                       'data' : msisdn,
                       'start' : 'yes' if process_now else 'no'}
#                       'personalcache' : p_cache,
#                       'sharedcache' : s_cache,
#                       'savetoshared' : 'on' if save_to_shared else 'off'}
            uri = self.url['bulk'] + "?apikey=%s&password=%s" % (self.apikey, self.password)
            #post = requests.post(uri, params = payload, headers = self.headers)
            post = requests.post(uri, data = payload, headers = self.headers)
            print post.url
            #print post.text
            res = post.json()
            #print res
            if res['status'] == "OK":
                self.batch_id = res['batchid']
                self.batch_status = False
                if debug: print "batch succesfully uploaded with batch_id = %s" % str(self.batch_id)
                return self.batch_id

    def process_batch(self, batch_id = None, debug = True, parse_text_answer = True):
        if not self.batch_id:
            self.batch_id = batch_id
        if self.batch_id:
            if debug: print "--- x --- Sending command to process the batch --- x ---"
            uri = self.url['process'] % str(self.batch_id) + "?apikey=%s&password=%s" % (self.apikey, self.password)
            get = requests.get(uri)
            if parse_text_answer:
                print get.text

    def check_batch_status(self, batch_id = None, debug = True, parse_text_answer = True):
        if not self.batch_id:
            self.batch_id = batch_id
        if self.batch_id:
            res = {}
            uri = self.url['status'] % str(self.batch_id) + "?apikey=%s&password=%s" % (self.apikey, self.password)
            get = requests.get(uri)
            if parse_text_answer:
                #print get.text
                raw_text = get.text
                res = dict((k.strip('"'), v.strip('"')) for k,v in (item.split(':') for item in raw_text[1:-1].split(', ')))
                if debug: print res
            else:
                res = get.json()
                #print res
            if debug:
                print "** current status is %s (%s records remaining on %s total records)" % \
                (str(res['status']), str(res['records_remaining']), str(res['total_records']))
            #print status
            if res['status'] == 'complete':
                self.batch_status = True
                if debug: print "** batch processing completed"
                return self.batch_status

    def download_batch(self, batch_id = None, debug = True, parse_text_answer = True):
        if not self.batch_id:
            self.batch_id = batch_id


    def write_batch_in_db(self, connection = None, debug = True):
        if not connection:
            self.get_connection()
            connection = self.connection
            cursor = self.cursor
        else:
            cursor = connection.cursor()
        arg_list = ['valid', 'Date Checked', 'Error Code', 'Original Network', 'Current Network',
                    'Current Country', 'Roaming Country', 'Type']
        for n in range(len(self.res_df.index)):
            sms = SMS(self.res_df.at[n, 'sms'], self.res_df.at[n, 'sms_md5'], None, connection, cursor)
            args = list(self.res_df.loc[n, arg_list])
            sms.hlr_update(*args)

    def batch(self, batch_name = 'test', write_to_file = False, folder = None, file = None,
              write_to_db = True, connection = None, debug = True):
        #old version that does not work with requests
        """ xxx
        if debug: print "--- HLR Lookup process ---"
        self.upload_batch(debug = debug)
        self.process_batch(debug = debug)
        if debug: print "--- x --- Batch is processed by HLRLookup : now checking status --- x ---"
        cpt_check = 0
        while self.batch_status == False and cpt_check <= max_check:
            time.sleep(sleep_time)
            self.check_batch_status(debug = debug)
            cpt_check =+ 1
        self.download_batch(debug = debug)
        self.write_batch_in_db(debug = debug)
        self.valid_df = self.res_df[['sms', 'valid']]
        self.valid_rate = len(self.valid_df[[self.valid_df.valid == True]].index) / len(self.valid_df.index)
        return(self.valid_df, self.valid_rate)
        """
        if self.sms_list:
            remove_plus_sign = lambda x: x[1:] if x[0] == '+' else x
            local_clean_sms_list = [remove_plus_sign(str(x)) for x in self.sms_list]
            raw_text = createRequest(local_clean_sms_list, batch_name, debug)
            data = []
            for res_line in raw_text.splitlines():
                data.append(res_line.split(","))
            df = pd.DataFrame(data[1:-1], columns = data[0])
            #show_df(df)
            #df.rename(columns = {"" : 'sms'}, inplace = True)
            self.res_df = df
            valid = self.clean_dataframe()
            if not valid:
                try:
                    self.res_df.sms = self.res_df.sms.apply(lambda x : '+' + str(x))
                    self.res_df['sms_md5'] = self.res_df.sms.apply(lambda x : hash_to_md5(x))
                    self.res_df['valid'] = self.res_df['Error Code'].apply(lambda x : True if str(x) == '0' else False)
                    self.res_df['Date Checked'] = self.res_df['Date Checked'].apply(lambda x : read_string_dates(x))
                except:
                    print "There is a problem with the df returned by HLR Lookup -- Impossible to clean -- It looks like this :"
                    show_df(df)
            if debug:
                print "See below resulting df"
                show_df(self.res_df)

            self.valid_df = self.res_df.loc[self.res_df.valid == True,'sms']
            self.valid_rate = round(len(self.valid_df.index) / len(self.sms_list), 2)
            if debug:
                print "%s valid SMS in %s checked --> valid rate = %s" % \
                      (str(len(self.valid_df.index)), str(len(self.sms_list)), str(self.valid_rate))
            if write_to_db:
                Thread(target=self.import_dataframe_in_db).start()
            if write_to_file:
                self.valid_df.to_csv(folder + "/" + file, index = False)
            return(self.valid_df, self.valid_rate)

class SMSQuery(object):

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

    query_dict = {'insert_query' : "INSERT INTO sms_api_query " + \
                                   "(user_name, create_date, client_id, geo_dict, geo_criteria, geo_list, age_min, age_max, civi, " + \
                                   "interest_list, proxi_list, df_result, query_name, global_limit, geo_dict_with_limit) VALUES " + \
                                   "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  'select_query' : "SELECT user_name, create_date, client_id, geo_dict, geo_criteria, geo_list, age_min, age_max, civi, " + \
                                   "interest_list, proxi_list, df_result, query_name, global_limit, geo_dict_with_limit FROM sms_api_query WHERE id = %s",
                  'insert_campaign' : "INSERT INTO sms_api_campaign (client_id, create_date, query_id, " + \
                                      "campaign_name, user_name, volume_client, volume_sent, billing_price) VALUES " + \
                                      "(%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  'update_campaign' : "UPDATE sms_api_campaign SET volume_sent = %s, router_list_id = %s " + \
                                      "WHERE id = %s",
                  'select_related_campaign' : "SELECT id FROM sms_api_campaign WHERE query_id = '%s' ORDER DESC LIMIT 1"}

    def __init__(self, id = None, user = 'david', client_id = None, client_name = None, sms_table = 'sms', sms_id_table = 'id',
                 connection = None, cursor = None, debug = True):
        self.date = d.datetime.now().date().isoformat()
        self.create_date = d.datetime.now().replace(microsecond=0).isoformat()
        self.close_connection = False
        self.connection = connection
        self.cursor = cursor
        self.debug = debug
        self.sms_table = sms_table
        self.sms_id_table = sms_id_table
        self.from_clause = "FROM %s AS s JOIN sms_mail AS sm ON s.id = sm.sms_id RIGHT JOIN %s AS id ON id.mail_id = sm.mail_id " \
                            % (self.sms_table, self.sms_id_table)
        self.select_clause = None
        self.where_clause = None
        self.groupby_clause = None
        self.limit_clause = None
        self.df_result = None
        self.interest_list = None
        self.proxi_list = None
        self.geo_dict = None
        self.geo_dict_with_limit = None
        self.global_limit = None
        self.geo_criteria = None
        self.geo_list = None
        self.cp_list = None
        self.cp_sql_list = None
        self.city_list = None
        self.city_cp_strict = None
        self.city_cp_list = None
        self.city_cp_df = None
        self.region_list = None
        self.region_cp_list = None
        self.region_cp_df = None
        self.cp_fix = False
        self.cp_fix_df = None
        self.geo_fix = False
        self.geo_fix_df = None
        self.sql_city_list = None
        self.sql_city_sql_list = None
        self.cp_precision = 5
        self.age_min = None
        self.age_max = None
        self.civi = None
        self.civi_arg = None
        self.sms_list = None
        self.field_sql = None
        self.select_sql = None
        self.select_field = None
        self.client_limit = None
        self.limit = None
        self.groupby_sql = None
        self.groupby_count = None
        self.groupby_count_depth = None
        self.groupby_count_field = None
        self.groupby_sql = None
        self.valid_df = None
        self.valid_rate = None
        self.query = None
        self.campaign = None
        self.campaign_id = None
        self.client_associated = False
        self.client = None
        self.id = id
        self.user_name = user
        self.client_id = client_id
        self.client_name = client_name
        self.query_name = None
        self.campaign_summary = None
        self.retrieve_data_from_id()
        self.retrieve_client()

    def eval_geo(self):
        if self.geo_criteria in ['cp', 'zip', 'zip_code'] or self.geo_criteria == None:
            self.cp_list = convert_args_to_list(self.geo_list, 'cp')
        elif self.geo_criteria in ['ville', 'city']:
            self.city_list = convert_args_to_list(self.geo_list)
        elif self.geo_criteria in ['dpt', 'departement']:
            self.cp_list = convert_args_to_list(self.geo_list)
        elif self.geo_criteria in ['region']:
            self.region_list = convert_args_to_list(self.geo_list)

    def where(self, geo_criteria = None, geo_list = None, age_min = None, age_max = None, civi = None, cp_precision = 5,
              proxi = None, interest = None, sms_list = None, city_cp_strict = True, sql_city_list = None):
        if geo_criteria: self.geo_criteria = geo_criteria
        if geo_list: self.geo_list = geo_list
        if sql_city_list: self.sql_city_list = sql_city_list
        if cp_precision: self.cp_precision = cp_precision
        if age_min: self.age_min = age_min
        if age_max: self.age_max = age_max
        if civi: self.civi = civi
        if sms_list: self.sms_list = sms_list
        self.city_cp_strict = city_cp_strict
        self.eval_geo()
        self.where_clause = ""
        where_list = []
        if self.age_min:
            if str(self.age_min).isdigit():
                where_list.append("(DATE_PART('year', AGE(id.birth)) >= %s) " % str(self.age_min))
        if self.age_max:
            if str(self.age_max).isdigit():
                where_list.append("(DATE_PART('year', AGE(id.birth)) <= %s) " % str(self.age_max))
        if self.civi:
            self.civi_arg = []
            if isinstance(self.civi,list):
                for arg in self.civi:
                    if arg == 'M':
                        self.civi_arg.append('1')
                    elif arg == 'F':
                        self.civi_arg.append('2')
                        self.civi_arg.append('3')
            else:
                if self.civi == 'M':
                    self.civi_arg.append('1')
                elif self.civi == 'F':
                    self.civi_arg.append('2')
                    self.civi_arg.append('3')
            if len(self.civi_arg) > 0 and len(self.civi_arg) < 3:
                if len(self.civi_arg) == 1:
                    where_list.append("(id.civilite = %s) " % self.civi_arg[0])
                else:
                    civi_string = "(" + ",".join(self.civi_arg) + ")"
                    where_list.append("(id.civilite IN %s) " % civi_string)
        if self.sql_city_list:
            self.sql_city_sql_list = []
            if isinstance(self.sql_city_list, list):
                for city in self.sql_city_list:
                    city = str(city)
                    if not city.isdigit():
                        city = clean_city_name(city)
                        self.sql_city_sql_list.append("'" + city + "'")
            else:
                city = str(self.sql_city_list)
                if not city.isdigit():
                    city = clean_city_name(city)
                    self.sql_city_sql_list.append("'" + city + "'")
            if len(self.sql_city_sql_list) > 0:
                if len(self.sql_city_sql_list) > 1:
                    where_list.append("(id.ville IN (%s)) " % ", ".join(self.sql_city_sql_list))
                else:
                    where_list.append("(id.ville = %s) " % self.sql_city_sql_list[0])

        if self.sms_list:
            add_plus_sign = lambda x : x if x[0] == "+" else "+" + x
            self.sms_list = [add_plus_sign(sms) for sms in self.sms_list]
            parenthesis = lambda x : "('" + x + "')"
            sms_string = ", ".join([parenthesis(sms) for sms in self.sms_list])
            where_list.append("s.sms IN (VALUES %s)" % sms_string)

        if self.city_list:
            if not self.connection: self.get_connection()
            res = self.create_city_cp_df(strict = city_cp_strict)
            if res:
                self.city_cp_list, self.city_cp_df = res
                if self.cp_list:
                    if self.debug: print "## Warning : city_list overrides cp_list. cp_list will not be taken in account. ##"
                self.cp_list = []
                self.cp_list.extend(self.city_cp_list)
            else:
                if self.debug: print "## Warning : could not match city name with zipcodes. ##"

        if self.region_list:
            if not self.connection: self.get_connection()
            res = self.create_region_cp_df()
            if res:
                self.region_cp_list, self.region_cp_df = res
                if self.cp_list:
                    if self.debug: print "## Warning : region_list overrides cp_list. cp_list will not be taken in account. ##"
                if self.cp_precision:
                    if self.debug: print "## Warning : use of region_list forces cp_precision = 2 as we use 'departements' (2-digit) ##"
                self.cp_precision = 2
                self.cp_list = []
                self.cp_list.extend(self.region_cp_list)

        if self.cp_list:
            if self.cp_precision < 2 or self.cp_precision > 5:
                self.cp_precision = 5
            # Stores how the cp should be treated in SQL terms, based on its length and on cp_precision
            self.cp_sql_list = []
            if isinstance(self.cp_list, list):
                for cp in self.cp_list:
                    if str(cp).isdigit():
                        cp = str(cp)
                        len_cp = len(cp)
                        if len_cp == 5:
                            if self.cp_precision == 5:
                                self.cp_sql_list.append(cp)
                            elif self.cp_precision < 5:
                                self.cp_sql_list.append(cp[:self.cp_precision] + '%')
                        elif len_cp == 1:
                            if self.cp_precision == 2:
                                self.cp_sql_list.append('0' + cp + '%')
                        elif len_cp < 5:
                            if self.cp_precision >= len_cp:
                                self.cp_sql_list.append(cp + '%')
                            elif self.cp_precision < len_cp:
                                self.cp_sql_list.append(cp[:self.cp_precision] + '%')
            else:
                if str(self.cp_list).isdigit():
                    cp = str(self.cp_list)
                    len_cp = len(cp)
                    if len_cp == 5:
                        if self.cp_precision == 5:
                            self.cp_sql_list.append(cp)
                        elif self.cp_precision < 5:
                            self.cp_sql_list.append(cp[:self.cp_precision] + '%')
                    elif len_cp < 5:
                        if self.cp_precision >= len_cp:
                            self.cp_sql_list.append(cp + '%')
                        elif self.cp_precision < len_cp:
                            self.cp_sql_list.append(cp[:self.cp_precision] + '%')
            # sorts SQL cp expressions by their length : 5-digit is = vs. less-than-5-digit is LIKE
            if len(self.cp_sql_list) > 0:
                self.cp_sql_dict = {}
                for cpt in range(2,6):
                    self.cp_sql_dict[cpt] = []
                for cp_sql in self.cp_sql_list:
                    if '%' in cp_sql:
                        self.cp_sql_dict[len(cp_sql[:-1])].append("'" + cp_sql + "'")
                    else:
                        self.cp_sql_dict[len(cp_sql)].append("'" + cp_sql + "'")
                # If it exists cp with two-digit precision, then all cp become two-digit
                if self.cp_sql_dict[2]:
                    for cpt in range(3,6):
                        if self.cp_sql_dict[cpt]:
                            if self.debug: print "## Warning : %s will be treated as two-digit queries ##" % str(self.cp_sql_dict[cpt])
                            for cp_sql in self.cp_sql_dict[cpt]:
                                self.cp_sql_dict[2].append(cp_sql[:3] + "%'")
                            self.cp_sql_dict[cpt] = []
                # Iterates on previously built dictionary
                cp_sql_expression = []
                for nb_char, sorted_cp_sql in self.cp_sql_dict.iteritems():
                    if len(sorted_cp_sql) > 0:
                        # creates the SQL expression
                        if nb_char == 5:
                            if len(sorted_cp_sql) == 1:
                                cp_sql_expression.append("(id.cp = %s) " % sorted_cp_sql[0])
                            else:
                                cp_sql_expression.append("(id.cp IN (%s)) " % ", ".join(list(set(sorted_cp_sql))))
                        else:
                            if len(sorted_cp_sql) == 1:
                                cp_sql_expression.append("(id.cp LIKE %s) " % sorted_cp_sql[0])
                            else:
                                cp_sql_expression.append("(id.cp LIKE ANY (ARRAY[%s])) " % ", ".join(list(set(sorted_cp_sql))))
                # case where an additional df is needed to fix the results
                for cpt in [3, 4]:
                    if len(self.cp_sql_dict[cpt]) > 0: self.cp_fix = True
                if self.cp_fix: self.create_cp_fix_df()
                # builds where_list with previously filled cp_sql_expression list
                if len(cp_sql_expression) > 0:
                    if len(cp_sql_expression) > 1:
                        where_list.append("(" + "OR ".join(cp_sql_expression)[:-1] + ") ")
                    else:
                        where_list.append(cp_sql_expression[0] + " ")
    
        if len(where_list) >= 1:
            self.where_clause = "WHERE " + "AND ".join(where_list)
        else:
            self.where_clause = ""

    def print_query(self):
        print self.select_clause
        print self.from_clause
        if self.where_clause: print self.where_clause
        if self.groupby_clause: print self.groupby_clause
        if self.limit_clause: print self.limit_clause

    def create_query(self):
        if self.where_clause == "":
            self.query = "".join([self.select_clause, self.from_clause, self.groupby_clause]) + ";"
        else:
            self.query = "".join([self.select_clause, self.from_clause, self.where_clause, self.groupby_clause]) + ";"
        if self.limit_clause: self.query = self.query.replace(";", " " + self.limit_clause + ";")
        return self.query

    def create_region_cp_df(self, id_table = 'postal_region_cp', strict = False):
        usual_region_dict = {'idf' : 'Ile-de-France', 'IDF' : 'Ile-de-France'}
        if self.region_list:
            if not isinstance(self.region_list, list):
                self.region_list = [self.region_list]
            self.region_list = [usual_region_dict[region] if region in usual_region_dict.keys() else region for region in self.region_list]
            self.region_list = flatten_list(self.region_list)
            if len(self.region_list) > 1:
                if strict:
                    q = "SELECT DISTINCT ON (id.dpt) id.region, id.dpt AS cp FROM %s AS id WHERE UPPER(id.region) IN (%s) GROUP BY id.dpt, id.region" % \
                        (id_table, ", ".join(["'" + format_region_for_cp_retrieval(region, id_table) + "'" for region in self.region_list]))
                else:
                    q = "SELECT DISTINCT ON (id.dpt) id.region, id.dpt AS cp FROM %s AS id WHERE UPPER(id.region) LIKE ANY (ARRAY[%s]) GROUP BY id.dpt, id.region" % \
                        (id_table, ", ".join(["'" + format_region_for_cp_retrieval(region, id_table) + "%'" for region in self.region_list]))
            else:
                q = "SELECT DISTINCT ON (id.dpt) id.region, id.dpt AS cp FROM %s AS id WHERE UPPER(id.region) = '%s' GROUP BY id.dpt, id.region" % \
                    (id_table, format_region_for_cp_retrieval(self.region_list[0], id_table))

            if self.debug:
                print "--- Querying %s table to get corresponding cp ---" % str(id_table)
                print q
            self.region_cp_df = pd.read_sql(q, self.connection)
            self.region_cp_df = self.region_cp_df.drop_duplicates()
            self.region_cp_list = list(self.region_cp_df.cp.unique())
            if self.debug:
                #show_group_by_df(df, 'ville')
                show_df(self.region_cp_df)
                print "%s region --> %s records for %s distinct cp" % (str(len(self.region_list)),
                                                                       str(len(self.region_cp_df.index)),
                                                                       str(len(self.region_cp_list)))
                #print cp_list
            return (self.region_cp_list, self.region_cp_df)
        else:
            return False

    def create_city_cp_df(self, id_table = 'postal_ville_cp', strict = True):
        usual_city_dict = {'Paris' : ['Paris ' + digit_to_str(arrdt) for arrdt in range(1,21)],
                       'Neuilly' : 'Neuilly sur Seine',
                       'Boulogne' : 'Boulogne Billancourt',
                       'Levallois' : 'Levallois Perret',
                       'Velizy' : 'Velizy Villacoublay',
                       'Villiers' : 'Villiers le Bel',
                       'Reuil' : 'Reuil Malmaison'}
        if self.city_list:
            if not isinstance(self.city_list, list):
                self.city_list = [self.city_list]
            self.city_list = [usual_city_dict[city] if city in usual_city_dict.keys() else city for city in self.city_list]
            self.city_list = flatten_list(self.city_list)
            if len(self.city_list) > 1:
                if strict:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.ville IN (%s) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_city_for_cp_retrieval(city, id_table) + "'" for city in self.city_list]))
                else:
                    q = "SELECT DISTINCT ON (id.cp) id.ville, id.cp FROM %s AS id WHERE id.ville LIKE ANY (ARRAY[%s]) GROUP BY id.cp, id.ville" % \
                        (id_table, ", ".join(["'" + format_city_for_cp_retrieval(city, id_table) + "%'" for city in self.city_list]))
            else:
                q = "SELECT DISTINCT ON (id.cp)id.ville, id.cp FROM %s AS id WHERE id.ville = '%s' GROUP BY id.cp, id.ville" % \
                    (id_table, format_city_for_cp_retrieval(self.city_list[0], id_table))

            if self.debug:
                print "--- Querying %s table to get corresponding cp ---" % str(id_table)
                print q
            self.city_cp_df = pd.read_sql(q, self.connection)
            self.city_cp_df = self.city_cp_df.drop_duplicates()
            self.city_cp_list = list(self.city_cp_df.cp.unique())
            if self.debug:
                #show_group_by_df(df, 'ville')
                show_df(self.city_cp_df)
                print "%s cities --> %s records for %s distinct cp" % (str(len(self.city_list)),
                                                                       str(len(self.city_cp_df.index)), str(len(self.city_cp_list)))
                #print cp_list
                if len(self.city_cp_list) == 0: return False
            return (self.city_cp_list, self.city_cp_df)
        else:
            return False

    def create_cp_fix_df(self):
        # when cp_precision in [3, 4], creates a df to mask existing cp with queried cp
        #print self.cp_sql_dict
        data = []
        two_digit = lambda x: '0' + str(x) if len(str(x)) == 1 else str(x)
        for n_cp, cp_list in self.cp_sql_dict.iteritems():
            for cp in cp_list:
                cp_num = cp[1:-2]
                for cp_param in self.cp_list:
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
        self.cp_fix_df = pd.DataFrame(data = data, columns = ['cp_show', 'cp'])
        show_df(self.cp_fix_df)
        return self.cp_fix_df

    def create_geo_fix_df(self):
        # geo_fix_df purpose is to transform whatever the query returns into what the user asked for in the first place
        # checks if cp_fix_df was created in prior steps (ie. if cp_precision = 3 or = 4)
        if self.cp_fix:
            # in this case, other masking df need to be merged with cp_fix_df
            if self.city_list:
                # 'cp_show' column is replaced by 'ville' column
                self.geo_fix = True
                self.geo_fix_df = pd.merge(self.cp_fix_df,
                                          self.city_cp_df.rename(columns = {'cp' : 'cp_show'}))
            elif self.region_list:
                pass
            else:
                self.geo_fix = True
                self.geo_fix_df = self.cp_fix_df.copy(deep = True)
        else:
            # this case corresponds cp_precision = 2 ('dpt' case) or = 5 ('plain' five-digit cp)
            if self.city_list:
                # in this case, we need to transform cp into cities
                self.geo_fix = True
                self.geo_fix_df = self.city_cp_df.copy(deep = True)
                if self.cp_precision == 2:
                    # in this case, the query returns the first two-digit of the users' cp,
                    # so we need to adapt geo_fix_df by creating a dpt column
                    self.geo_fix_df['dpt'] = self.geo_fix_df['cp'].apply(lambda x: str(x)[:2])
                    # we can drop the 'cp' column as it becomes uselss (given that when cp_precision = 2 only dpt are returned)
                    self.geo_fix_df.drop('cp', axis = 1, inplace = True)
                else:
                    # in this case cp_precision = 5, so no need for extra modifications
                    pass
            elif self.region_list:
                pass
            else:
                # no need to mask anything in this case
                pass

    def get_df(self):
        self.create_query()
        if not self.connection: self.get_connection()
        if self.debug:
            print "--- Resulting SQL query ---"
            self.print_query()
            print "--- Querying the DB (this may take up to a minute) ---"
        self.df = pd.read_sql(self.query, self.connection)
        self.create_geo_fix_df()
        if self.geo_fix:
            if 'cp' in list(self.df):
                if self.debug:
                    show_df(self.df)
                    print "--- Masking geo results ---"
                self.df = pd.merge(self.df, self.geo_fix_df, on = 'cp')
                if self.cp_fix:
                    if self.debug: print "--- Removing 'cp_show' column ---"
                    self.df.drop('cp', axis = 1, inplace = True)
                    self.df.rename(columns = {'cp_show' : 'cp'}, inplace = True)
        if self.debug: show_df(self.df)
        return self.df

    def select(self, select_field = None, limit = None, hlr_factor = 1.25, error_factor = 1.1):
        if select_field: self.select_field = select_field
        if limit:
            self.client_limit = limit
            self.limit = enlarge_sample(limit, hlr_factor = hlr_factor, error_factor = error_factor, debug = self.debug)
        self.groupby_sql = ['s.id']
        self.field_sql = []
        self.select_clause = "SELECT DISTINCT ON (s.id) "
        if self.select_field:
            if isinstance(self.select_field, list):
                for field in self.select_field:
                    if field in self.field_dict.keys():
                        self.field_sql.append(self.field_dict[field])
                        self.groupby_sql.append(field)
            else:
                field = self.select_field
                if field in self.field_dict.keys():
                    self.field_sql.append(self.field_dict[field])
                    self.groupby_sql.append(field)
            if len(self.field_sql) > 0:
                if len(self.field_sql) == 1:
                    self.select_clause = self.select_clause + self.field_sql[0] + " "
                else:
                    self.select_clause = self.select_clause + ", ".join(self.field_sql) + " "
                self.groupby_clause = "GROUP BY %s " % ", ".join(self.groupby_sql)
                if 'b.mail' in self.select_clause:
                    self.from_clause = self.from_clause + "JOIN base AS b ON b.id = sm.mail_id "
                if "md5.md5" in self.select_clause:
                    self.from_clause = self.from_clause + "JOIN md5 ON md5.mail_id = sm.mail_id "
            else:
                self.select_clause = self.select_clause + self.field_dict['sms'] + " "
                self.groupby_sql.append('sms')
                self.groupby_clause = "GROUP BY %s" % ', '.join(self.groupby_sql)
        else:
            self.select_clause = self.select_clause + self.field_dict['sms'] + " "
            self.groupby_sql.append('sms')
            self.groupby_clause = "GROUP BY %s" % ', '.join(self.groupby_sql)
        if self.limit:
            if str(self.limit).isdigit():
                self.limit_clause = "LIMIT %s" % str(self.limit)
        self.get_df()
        return self.df

    def select_sample(self, select_field = None, limit = None, hlr_factor = 1.25, error_factor = 1.1):
        if limit:
            limit = enlarge_sample(limit, hlr_factor = hlr_factor, error_factor = error_factor, debug = self.debug)
            self.select(select_field)
            if len(self.df.index) < limit:
                self.cp_precision = self.cp_precision - 1
                self.where(self.geo_criteria, self.geo_list, self.age_min, self.age_max, self.civi, self.cp_precision,
                            self.proxi_list, self.interest_list, self.sms_list, self.city_cp_strict, self.sql_city_list)

                self.get_df()
                while len(self.df.index) < limit and self.cp_precision >= 2:
                    self.cp_precision = self.cp_precision - 1
                    self.where(self.geo_criteria, self.geo_list, self.age_min, self.age_max, self.civi, self.cp_precision,
                            self.proxi_list, self.interest_list, self.sms_list, self.city_cp_strict, self.sql_city_list)
                    self.get_df()
            self.df = sample_or_same(self.df, limit)
        return self.df

    def count_sms(self, groupby_count = False, groupby_count_depth = 1, groupby_count_field = None, check_cp_list = True):
        if check_cp_list:
            if not self.cp_list:
                return False
        if groupby_count:
            self.groupby_count = groupby_count
        else:
            self.groupby_count = False
        if groupby_count_depth: self.groupby_count_depth = groupby_count_depth
        if groupby_count_field: self.groupby_count_field = groupby_count_field
        self.select_clause = "SELECT COUNT(DISTINCT s.id) AS nb_sms "
        self.groupby_clause = ""
        if self.groupby_count:
            self.select_sql = []
            self.groupby_sql = []
            if self.groupby_count_field:
                groupby_field_dict = {}
                for field in ['cp','ville','city','civi','civilite','gender','genre','age','year_old']:
                    groupby_field_dict[field] = self.field_dict[field]
                if isinstance(self.groupby_count_field, list):
                    for field in self.groupby_count_field:
                        if field in groupby_field_dict.keys():
                            self.select_sql.append(groupby_field_dict[field])
                            self.groupby_sql.append(field)
                else:
                    field = self.groupby_count_field
                    if field in groupby_field_dict.keys():
                        self.select_sql.append(groupby_field_dict[field])
                        self.groupby_sql.append(field)
            else:
                if self.sql_city_list:
                    if len(self.sql_city_sql_list) > 0:
                        self.select_sql.append(self.field_dict['ville'])
                        self.groupby_sql.append('ville')
                if self.cp_list:
                    if len(self.cp_sql_list) > 0:
                        if len(self.cp_sql_dict[2]) > 0:
                            self.select_sql.append("LEFT(id.cp,2) AS dpt")
                            self.groupby_sql.append("dpt")
                        else:
                            self.select_sql.append(self.field_dict['cp'])
                            self.groupby_sql.append("cp")
                if self.civi:
                    if len(self.civi_arg) > 0:
                        self.select_sql.append(self.field_dict['genre'])
                        self.groupby_sql.append("genre")
                    #if len(civi_arg) > 0: self.groupby_sql.append("CASE WHEN id.civilite=1 THEN 'M' WHEN id.civilite IN (2, 3) THEN 'F' ELSE 'None' END AS gender")
                if self.groupby_count_depth:
                    try:
                        self.select_sql = self.select_sql[:self.groupby_count_depth]
                        self.groupby_sql = self.groupby_sql[:self.groupby_count_depth]
                    except:
                        pass
            self.select_sql.append("COUNT(DISTINCT s.id) AS nb_sms ")
            self.select_clause = "SELECT " + ", ".join(self.select_sql)
            self.groupby_clause = "GROUP BY %s" % ', '.join(self.groupby_sql) + " ORDER BY nb_sms DESC"
        else:
            self.select_clause = "SELECT COUNT(DISTINCT s.id) AS nb_sms "
            self.groupby_clause = ""
        self.get_df()

        if self.groupby_count:
            if self.city_cp_list:
                if len(self.df.ville.tolist()) > len(list(self.df.ville.unique())):
                    sum_df = self.df.groupby('ville').sum()
                    show_df(sum_df, len(sum_df.index))
                return self.df[['ville', 'nb_sms']]
            elif self.region_cp_list:
                pass
            else:
                if 'cp' in list(self.df):
                    if len(self.df.cp.tolist()) > len(list(self.df.cp.unique())):
                        sum_df = self.df.groupby('cp').sum()
                        show_df(sum_df, len(sum_df.index))
                return self.df
        else:
            return self.df.iat[0,0]

    def count_groupby(self, fields, age_range = [25,35,45,55,65], upper_limit = 85):
        if 'age_range' in fields:
            if 'age' not in fields:
                fields.append('age')
            fields.remove('age_range')
            self.select(fields)
            age_range_df = create_age_range_df(age_range, upper_limit)
            self.df = pd.merge(self.df, age_range_df, on = 'age', how = 'left')
            self.df.drop('age', axis = 1, inplace = True)
            show_df(self.df)
            fields.append('age_range')
            if 'age' in fields:
                fields.remove('age')
        print self.df.groupby(fields).size()
        return self.df.groupby(fields).size().reset_index(name='counts')

    def count_multi(self, geo_dict = None, geo_criteria = None, geo_list = None, cp_precision = 5,
                    age_min = None, age_max = None, civi = None, interest_list = None, proxi_list = None,
                    user = None, client_id = None, client_name = None):
        if interest_list: self.interest_list = interest_list
        if proxi_list: self.proxi_list = proxi_list
        if geo_dict: self.geo_dict = geo_dict
        if geo_criteria: self.geo_criteria = geo_criteria
        if geo_list: self.geo_list = geo_list
        if cp_precision: self.cp_precision = cp_precision
        if age_min: self.age_min = age_min
        if age_max: self.age_max = age_max
        if civi: self.civi = civi
        if user: self.user_name = user

        self.retrieve_client(client_id, client_name)
        self.create_query_name(user, client_id, client_name)

        if geo_dict:
            threads = []
            for geo_criteria, geo_list in self.geo_dict.iteritems():
                for geo_item in geo_list:
                    t = ThreadReturn(name = str(geo_criteria) + "_" + str(geo_item),
                                    target = sms_query_count_single_geo_item,
                                    args = (geo_criteria, geo_item, cp_precision, age_min, age_max, civi))
                    threads.append(t)

        else:
            threads = [ThreadReturn(name = str(geo_criteria) + "_" + str(geo_item),
                                    target = sms_query_count_single_geo_item,
                                    args = (geo_criteria, geo_item, cp_precision, age_min, age_max, civi))
                       for geo_item in geo_list]
        for t in threads:
            t.start()
        data = []
        for t in threads:
            t_res = t.join()
            if t_res:
                data.append(t_res)
        result = pd.DataFrame(data = data, columns = ['entity', 'name', 'nb_sms'])
        if self.interest_list:
            if not isinstance(self.interest_list, list): self.interest_list = [self.interest_list]
            for interest_id in self.interest_list:
                result = adjust_for_interest(result, interest_id, amplitude = 0, col_to_adjust = 'nb_sms', base = 1.2)
        if self.proxi_list:
            if not isinstance(self.proxi_list, list): self.proxi_list = [self.proxi_list]
            for proxi in proxi_list:
                result = adjust_for_proxi(result, proxi = proxi, amplitude = 0, col_to_adjust = 'nb_sms', base = 1.2)
        show_df(result)
        json_result = {}
        json_result['columns'] = list(result)
        json_result['values'] = result.values.tolist()
        self.df_result = json.dumps(json_result)
        self.store_in_db()
        # return self.id <-- need to return self.id in the webservice's JSON
        return result

    def select_multi(self, query_id = None, global_limit = None, select_field = ['sms','age','gender','cp'], hlr_lookup = True,
                     geo_dict_with_limit = None, geo_dict = None, geo_criteria = None, geo_list = None, cp_precision = 5,
                     age_min = None, age_max = None, civi = None, interest_list = None, proxi_list = None,
                     user = None, client_id = None, client_name = None):
        # Loads parameters
        if global_limit: self.global_limit = int(global_limit)
        if geo_dict_with_limit: self.geo_dict_with_limit = geo_dict_with_limit

        # Priority given to query_id, in which case the parameters are retrieved from the DB
        if query_id:
            self.id = query_id
            self.retrieve_data_from_id()
        else:
            if interest_list: self.interest_list = interest_list
            if proxi_list: self.proxi_list = proxi_list
            if geo_dict: self.geo_dict = geo_dict
            if geo_criteria: self.geo_criteria = geo_criteria
            if geo_list: self.geo_list = geo_list
            if cp_precision: self.cp_precision = cp_precision
            if age_min: self.age_min = age_min
            if age_max: self.age_max = age_max
            if civi: self.civi = civi

        self.retrieve_client(client_id, client_name)
        self.create_query_name(user, client_id, client_name)

        # Gets the selection with threading on each geo parameter
        threads = []
        # Case with limits given for each geo point
        if self.geo_dict_with_limit:
            for geo_criteria, geo_list_with_limit in self.geo_dict_with_limit.iteritems():
                # Checks that geo_list_with_limit truly has limits specified
                # In the case that it is a list (ie. no limits specified) : rebuilds a dict with all limits set to None
                if isinstance(geo_list_with_limit, list):
                    replacement_dict = {}
                    for geo_list in geo_list_with_limit:
                        replacement_dict[geo_list] = None
                    geo_list_with_limit = replacement_dict
                for geo_item, limit_item in geo_list_with_limit.iteritems():
                    t = ThreadReturn(name = str(geo_criteria) + "_" + str(geo_item) + "_" + (str(limit_item) if limit_item else "no-limit"),
                                     target = sms_query_select_sample_geo_item,
                                     args = (limit_item, geo_criteria, geo_item, 5, age_min, age_max, civi, select_field))
                    threads.append(t)
        # Case with no limit given for any geo pint (either a geo_dict, or a single (geo_criteria, geo_list) couple
        # This would be typically the case of a SMSQuery() being called by its query_id, with a global_limit given by the client
        elif self.geo_dict:
            for geo_criteria, geo_list in self.geo_dict.iteritems():
                for geo_item in geo_list:
                    t = ThreadReturn(name = str(geo_criteria) + "_" + str(geo_item) + "_" + str("no-limit"),
                                     target = sms_query_select_sample_geo_item,
                                     args = (None, geo_criteria, geo_item, 5, age_min, age_max, civi, select_field))
                    threads.append(t)
        else:
            threads = [ThreadReturn(name = str(geo_criteria) + "_" + str(geo_item) + "_" + str("no-limit"),
                                     target = sms_query_select_sample_geo_item,
                                     args = (None, geo_criteria, geo_item, 5, age_min, age_max, civi, select_field))
                       for geo_item in geo_list]
        # Running the treads
        for t in threads:
            t.start()
        data = []
        for t in threads:
            t_res = t.join()
            if t_res[0]:
                df = t_res[1]
                df['source'] = str(t.name)
                data.append(df)
        result = pd.concat(data)
        print "### This is the result of appending all threads ###"
        show_df(result)
        print "### Raw selection is %s contacts ###" % str(len(result.index))
        if global_limit:
            global_limit = enlarge_sample(global_limit)
            result = sample_or_same(result, global_limit)
        self.df = result
        print "### This is the result of applying global limit ###"
        show_df(result)
        print "### Global limit selection is %s contacts ###" % str(len(result.index))
        if hlr_lookup:
            self.hlr_cleanup()

        try:
            self.store_campaign_in_db()
            self.store_sms_list_in_db()
        except:
            e = sys.exc_info()
            message = ""
            for item in e:
                message = message + " *** " + str(item)
            print "### Unable to store the campaign in DB ###"

        if self.upload_to_router(list_name = self.query_name):
            try:
                self.update_campaign_in_db()
            except:
                e = sys.exc_info()
                message = ""
                for item in e:
                    message = message + " *** " + str(item)
                print "### Unable to update the campaign in DB ###"

        self.get_campaign_summary()
        return self.df


    def hlr_cleanup(self, batch_name = None, write_to_file = False, folder = None, file = None,
                    write_to_db = True, connection = None):
        if write_to_db:
            if not connection:
                if not self.connection:
                    self.get_connection()
                connection = self.connection
        try:
            sms_list = self.df.sms.tolist()
        except:
            self.get_df()
            sms_list = self.df.sms.tolist()

        if not batch_name:
            if self.query_name:
                batch_name = self.query_name
            else:
                batch_name = 'batch_%s' % str(self.date)

        h = HLR(sms_list)
        res = h.batch(batch_name, write_to_file, folder, file, write_to_db, connection)
        if res:
            self.valid_df, self.valid_rate = res
            #self.valid_df = pd.DataFrame(data = self.valid_df.sms.tolist(), columns = ['sms'])
            self.df = pd.merge(self.valid_df.to_frame('sms'), self.df, on = 'sms', how = 'left')
            print "## HLR Lookup completed : below is the new self.df within SMSQuery() object ##"
            show_df(self.df)

    def store_campaign_in_db(self, associate_campaign = True):
        if not self.connection: self.get_connection()
        if not self.id: self.store_in_db()
        sum_dict_limit = 0
        if self.geo_dict_with_limit:
            for criteria, dict_limit in self.geo_dict_with_limit.iteritems():
                for geo_item, geo_limit in dict_limit.iteritems():
                    try:
                        sum_dict_limit += int(geo_limit)
                    except:
                        pass
        if self.global_limit:
            if sum_dict_limit == 0:
                sum_dict_limit = self.global_limit
            else:
                if self.global_limit < sum_dict_limit:
                    sum_dict_limit = self.global_limit
        if sum_dict_limit > 0:
            volume_client = str(sum_dict_limit)
        else:
            volume_client = None

        try:
            volume_sent = len(self.df.index)
        except:
            volume_sent = 0
        if volume_sent > 0:
            try:
                billing_price = round(float(volume_sent/1000) * self.client.acq_cpm_price, 2)
            except:
                billing_price = None
        else:
            volume_sent = None
            billing_price = None

        args = [str(self.client_id), d.datetime.now().replace(microsecond=0).isoformat(), str(self.id),
                self.query_name, self.user_name, volume_client, volume_sent, billing_price]
        q = Query(self.cursor, self.connection, self.query_dict, 'insert_campaign', args, True)
        self.campaign_id = unpack(q.execute())
        if associate_campaign:
            c = SMSCampaign(self.campaign_id)
            c.retrieve_data_from_db(False)
            self.campaign = c

    def store_sms_list_in_db(self):
        sent_df = self.df.copy(True)
        try:
            sent_df['sms_md5'] = sent_df['sms'].apply(lambda x: hash_to_md5(x))
        except:
            return False
        if self.campaign_id:
            if not self.connection: self.get_connection()
            if 'source' in list(self.df):
                sent_df = sent_df.loc[:,['sms_md5', 'source']]
            else:
                sent_df = sent_df.loc[:,['sms_md5']]
                sent_df['source'] = ""
            sent_df['campaign_id'] = self.campaign_id
            q = "SELECT sms_md5, id AS sms_id FROM sms WHERE sms_md5 IN (%s)" % \
                ", ".join(["'%s'" % str(x) for x in sent_df.sms_md5.tolist()])
            md5_df = load_query(self.connection, q, False)
            sent_df = pd.merge(sent_df, md5_df, on = 'sms_md5', how = 'left')
            sent_df.drop('sms_md5', axis = 1, inplace = True)
            q = "SELECT DISTINCT ON (sms_id) sms_id FROM sms_sent WHERE campaign_id = '%s'"
            CopyToQuery(sent_df, q, self.campaign_id, 'sms_sent', 'sms_id', None, self.connection, self.query_name)
        else:
            return False

    def upload_to_router(self, list_name = 'list_created_Python', list_id = None, max_limit = 100000):
        sms_list = None
        try:
            sms_list = self.df.sms.tolist()
        except:
            print "## Warning : Non SMS numbers in the dataframe to be passed to the router ##"
            return False
        if sms_list:
            if len(sms_list) > 0:
                if len(sms_list) > max_limit:
                    print "## Warning : Attempt to upload a SMS list greater than %s contacts" % str(max_limit)
                else:
                    pt = PrimoTextoAPI()
                    if not list_id:
                        self.router_list_id = pt.create_list(list_name)
                    else:
                        self.router_list_id = list_id
                    res = pt.upload_list(self.df, list_id = self.router_list_id)
                    return res
        return False

    def update_campaign_in_db(self, associate_campaign = True):
        args = [str(len(self.df.index)), str(self.router_list_id), str(self.campaign_id)]
        q = Query(self.cursor, self.connection, self.query_dict, 'update_campaign', args, False)
        q.execute()
        if associate_campaign:
            c = SMSCampaign(self.campaign_id)
            c.retrieve_data_from_db(False)
            self.campaign = c

    def retrieve_data_from_id(self):
        if self.id:
            if not self.connection: self.get_connection()
            q = Query(self.cursor, self.connection, self.query_dict, 'select_query', [str(self.id)], True, True)
            res = unpack(q.execute())
            if res:
                self.user_name, self.create_date, self.client_id, self.geo_dict, self.geo_criteria, self.geo_list, self.age_min,\
                self.age_max, self.civi, self.interest_list, self.proxi_list, self.df_result, self.query_name, self.global_limit,\
                self.geo_dict_with_limit = res

                if self.geo_dict: self.geo_dict = eval(self.geo_dict)
                if self.geo_list: self.geo_list = eval(self.geo_list)
                if self.geo_dict_with_limit: self.geo_dict_with_limit = eval(self.geo_dict_with_limit)

    def retrieve_client(self, client_id = None, client_name = None):
        if not self.client_associated:
            if client_id: self.client_id = client_id
            if client_name: self.client_name = client_name
            self.client = SMSClient()
            if self.client_id:
                self.client_associated = self.client.get(id = self.client_id)
                if self.client_associated: self.client_name = self.client.name
            elif self.client_name:
                self.client_associated = self.client.get(name = self.client_name)
                if self.client_associated: self.client_id = self.client.id

    def create_query_name(self, user = None, client_id = None, client_name = None):
        if not self.query_name:
            if not self.client_associated:
                self.retrieve_client(client_id, client_name)

            if self.client_associated:
                try:
                    self.client_name = self.client.name.lower()
                except:
                    if client_name: self.client_name = client_name
            if not self.client_name:
                if client_name: self.client_name = client_name
                self.client_name = "client?"

            if not self.user_name:
                if user: self.user_name = user
                if not self.user_name: self.user_name = "user?"

            self.query_name = "_".join([self.user_name, self.client_name, self.date])

    def store_in_db(self, user_name = None, client_id = None):
        if user_name: self.user_name = user_name
        if client_id: self.client_id = client_id
        if not self.connection: self.get_connection()
        args = [str(self.user_name), d.datetime.now().replace(microsecond=0).isoformat(), str(self.client_id), str(self.geo_dict),
                str(self.geo_criteria), str(self.geo_list), str(self.age_min), str(self.age_max), str(self.civi),
                str(self.interest_list), str(self.proxi_list), str(self.df_result), str(self.query_name), str(self.global_limit),
                str(self.geo_dict_with_limit)]
        q = Query(self.cursor, self.connection, self.query_dict, 'insert_query', args, True)
        self.id = unpack(q.execute())

    def get_campaign_summary(self):
        self.campaign_summary = {}
        for param in ['age_min', 'age_max', 'civi', 'geo_dict', 'geo_dict_with_limit', 'interest_list', 'proxi_list',
                      'id', 'global_limit', 'geo_dict_with_limit']:
            self.campaign_summary[param] = self.__getattribute__(param)
        self.campaign_summary['query_id'] = self.campaign_summary.pop('id')
        if self.campaign:
            for param in ['volume_client', 'billing_price']:
                self.campaign_summary[param] = self.campaign.__getattribute__(param)
        self.campaign_summary['nb_sms_sent'] = self.campaign_summary.pop('volume_client')
        if self.client:
            for param in ['name', 'acq_cpm_price']:
                self.campaign_summary[param] = self.client.__getattribute__(param)
        self.campaign_summary['client_name'] = self.campaign_summary.pop('name')
        return self.campaign_summary

    def get_related_campaign_id_in_db(self):
        if not self.connection: self.get_connection()
        if self.id:
            q = Query(self.cursor, self.connection, self.query_dict, 'select_related_campaign', [str(self.id)], True)
            self.campaign_id = unpack(q.execute())

    def retrieve_data_from_campaign_id(self, campaign_id = None):
        if campaign_id: self.campaign_id = campaign_id
        c = SMSCampaign(self.campaign_id)
        c.retrieve_data_from_db(False)
        self.campaign = c

    def get_connection(self):
        if not self.connection:
            if self.debug: print "--- Establishing connection with DB ---"
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()
            self.close_connection = True

    def __del__(self):
        if self.close_connection:
            if self.debug:
                print "### Connection closed with __del__() method of SMSQuery() object ###"
            self.connection.close()

    def hlr_old_function(self, batch_name = 'test', write_to_file = False, folder = None, file = None,
              write_to_db = True, connection = None, debug = True):
        h = HLR(self.df.sms.tolist())
        self.valid_df, self.valid_rate = h.batch(batch_name, write_to_file, folder, file, write_to_db, self.connection, debug)

def sms_query_count_single_geo_item(geo_criteria = None, geo_item = None, cp_precision = 5,
                                    age_min = None, age_max = None, civi = None):
    s = SMSQuery()
    s.where(geo_criteria = geo_criteria, geo_list = geo_item, age_min = age_min, age_max = age_max, civi = civi,
            cp_precision = cp_precision, proxi = None, interest = None, sms_list = None, city_cp_strict = True, sql_city_list = None)
    #s.where(geo_criteria, geo_item, None, cp_precision, age_min, age_max, civi, None, True)
    res_count_sms = s.count_sms(False)
    if res_count_sms:
        res = [str(geo_criteria), str(geo_item), res_count_sms]
        print res
    else:
        res = False
    return res

def sms_query_select_sample_geo_item(limit = None, geo_criteria = None, geo_item = None, cp_precision = 5,
                                     age_min = None, age_max = None, civi = None, select_field = None):
    if limit:
        if str(limit) == '0':
            return (False, None)
    s = SMSQuery()
    s.where(geo_criteria = geo_criteria, geo_list = geo_item, age_min = age_min, age_max = age_max, civi = civi,
            cp_precision = cp_precision, proxi = None, interest = None, sms_list = None, city_cp_strict = True, sql_city_list = None)
    if limit:
        df = s.select_sample(select_field = select_field, limit = limit, hlr_factor = 1.25, error_factor = 1.1)
    else:
        df = s.select(select_field = select_field, limit = None, hlr_factor = 1.25, error_factor = 1.1)
    return (True, df)

class SMSCampaign(object):
    placeholder_dict = {'primotexto' : '${rich_message}'}
    list_id_default_dict = {'primotexto' : '5a61d9f07076b97ff6360ae3'}
    query_dict = {'select_campaign' : "SELECT * FROM sms_api_campaign WHERE id = %s",
                  'update_campaign' : "UPDATE sms_api_campaign SET sender = %s, message = %s, send_date = %s, " + \
                                      "bat_list = %s, router_list_id = %s, router = %s, router_campaign_id = %s, " + \
                                      "url = %s WHERE id = %s",
                  'insert_campaign' : "INSERT INTO sms_api_campaign (sender, message, send_date, " + \
                                      "bat_list, router_list_id, router, router_campaign_id, url, create_date) VALUES " + \
                                      "(%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  'insert_bat' : "INSERT INTO sms_api_bat_sent (campaign_id, sms, sent_date) VALUES (%s, %s, %s)",
                  'update_bat_status' : "UPDATE sms_api_campaign SET bat_sent = TRUE WHERE id = %s"}

    def __init__(self, id = None):
        self.date = d.datetime.now().date().isoformat()
        self.debug = True
        self.connection = None
        self.cursor = None
        self.close_connection = False
        self.query_id = None
        self.query = None
        self.client_id = None
        self.client = None
        self.client_name = None
        self.campaign_name = None
        self.message = None
        self.sender = None
        self.url = None
        self.router = None
        self.create_date = None
        self.bat_list = None
        self.bat_sent = False
        self.extra_recipients = None
        self.volume_sent = None
        self.volume_client = None
        self.user_name = None
        self.router_list_id = None
        self.router_campaign_id = None
        self.billing_price = None
        self.campaign_summary = None
        self.id = None
        if id: self.id = id
        self.retrieve_data_from_db(get_parents = True)

    def create_in_router(self, sender = None, message = None, send_date = None, bat_list = None,
                         list_id = None, router = 'primotexto'):
        if sender: self.sender = sender
        if message: self.message = message
        if send_date: self.send_date = send_date
        if bat_list: self.bat_list = bat_list
        if list_id: self.list_id = list_id
        if router: self.router = router
        router_message = self.message
        if "[[" in self.message:
            begin, after = self.message.split("[[")
            if "]]" in after:
                url, end = after.split("]]")
                if not test_url(url):
                    print "## Warning !! Url '%s' does not respond ##" % str(url)
                    return False
                router_message = begin + self.placeholder_dict[self.router] + end
                self.url = url
        self.create_campaign_name()
        if not self.router_list_id:
            self.router_list_id = self.list_id_default_dict[self.router]
            print "## No specific 'router_list_id' given for this campaign. It will use the default one instead. ##"
        if self.router == 'primotexto':
            pt = PrimoTextoAPI()
            res = pt.create_campaign(self.campaign_name, router_message, self.sender, self.send_date,
                                     self.url, self.router_list_id)
            if res:
                self.router_campaign_id = res
            else:
                print "## Warning !! Campaign NOT created in router. Pb. with sender '%s' or message ? ##" % str(self.sender)
            self.update_data_in_db()
        else:
            self.update_data_in_db()
        if self.bat_list: self.send_bat()

    def send_bat(self, bat_list = None, campaign_id = None):
        if bat_list: self.bat_list = bat_list
        if self.bat_list: self.bat_list = convert_list_to_international_format(self.bat_list)
        if self.bat_list:
            if self.router == 'primotexto':
                if not self.router_campaign_id:
                    if campaign_id:
                        self.router_campaign_id = campaign_id
                    else:
                        print "## Warning !! No router_campaign_id. You need to specify which campaign to send a bat to ##"
                        return False
                pt = PrimoTextoAPI()
                for sms in self.bat_list:
                    res = pt.send_bat_item(sms, campaign_id = self.router_campaign_id)
                    if res:
                        self.bat_sent = True
                        args = [self.id, sms, d.datetime.now().replace(microsecond=0).isoformat()]
                        q = Query(self.cursor, self.connection, self.query_dict, 'insert_bat', args, False)
                        q.execute()
                        print 'BAT envoyé au %s' % str(sms)
            else:
                pass
        if self.bat_sent:
            q = Query(self.cursor, self.connection, self.query_dict, 'update_bat_status', [self.id], False)
            q.execute()

    def update_data_in_db(self):
        if not self.connection: self.get_connection()
        args = [self.sender,self.message,self.send_date,self.bat_list,self.router_list_id, self.router,
                self.router_campaign_id, self.url, self.id]
        args = [str(x) for x in args]
        if self.id:
            q = Query(self.cursor, self.connection, self.query_dict, 'update_campaign', args, False)
            q.execute()
        else:
            args = args[:-1]
            args.append(d.datetime.now().replace(microsecond=0).isoformat())
            q = Query(self.cursor, self.connection, self.query_dict, 'insert_campaign', args, True)
            self.id = unpack(q.execute())

    def create_campaign_name(self):
        self.date = d.datetime.now().date().isoformat()
        if not self.user_name: self.user_name = "user?"
        if not self.client_name:
            try:
                self.client_name = self.client.name
            except:
                self.client_name = "client?"
        if not self.sender: self.sender = "sender?"
        simplify_sender = self.sender.replace(" ", "-").lower()
        self.campaign_name = "_".join([self.user_name, self.client_name, simplify_sender, self.date])

    def retrieve_data_from_db(self, get_parents = True):
        if not self.connection: self.get_connection()
        if self.id:
            q = Query(self.cursor, self.connection, self.query_dict, 'select_campaign', [str(self.id)], True)
            res = unpack(q.execute())
            if res:
                id, self.client_id, self.query_id, self.campaign_name, self.message, self.sender, self.url, self.router, \
                self.create_date, self.bat_list, self.bat_sent, self.extra_recipients, self.volume_sent, self.volume_client, \
                self.user_name, self.router_list_id, self.router_campaign_id, self.send_date, self.billing_price = res
                if get_parents:
                    if self.query_id:
                        q = SMSQuery(id = self.query_id)
                        self.query = q
                    if self.client_id:
                        c = SMSClient()
                        c.get(id = self.client_id)
                        self.client = c

    def get_campaign_summary(self):
        self.campaign_summary = {}
        if self.query:
            for param in ['age_min', 'age_max', 'civi', 'geo_dict', 'geo_dict_with_limit', 'interest_list', 'proxi_list',
                          'id', 'global_limit', 'geo_dict_with_limit']:
                self.campaign_summary[param] = self.query.__getattribute__(param)
            self.campaign_summary['query_id'] = self.campaign_summary.pop('id')

            if self.query.client:
                for param in ['name', 'acq_cpm_price']:
                    self.campaign_summary[param] = self.query.client.__getattribute__(param)
            self.campaign_summary['client_name'] = self.campaign_summary.pop('name')

        for param in ['volume_client', 'sender', 'message', 'bat_list', 'extra_recipients', 'send_date', 'url',
                      'campaign_name', 'billing_price']:
            self.campaign_summary[param] = self.__getattribute__(param)
        self.campaign_summary['nb_sms_sent'] = self.campaign_summary.pop('volume_client')
        return self.campaign_summary

    def get_connection(self):
        if not self.connection:
            if self.debug: print "--- Establishing connection with DB ---"
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()
            self.close_connection = True

    def __del__(self):
        if self.close_connection: self.connection.close()


class SMSCampList(object):
    query_dict = {'select_campaign' : "SELECT %s FROM sms_api_campaign AS c JOIN sms_api_query AS q ON q.id = c.query_id " + \
                                      "WHERE c.client_id = %s ORDER BY c.id DESC"}

    default_param = ['id', 'campaign_name', 'sender', 'volume_client', 'billing_price', 'message', 'send_date', 'bat_sent',
                     'age_min', 'age_max', 'interest_list', 'global_limit', 'geo_dict_with_limit']

    def __init__(self, client_id = 1, param_list = None, limit = 10):
        self.connection = None
        self.cursor = None
        self.close_connection = False
        if not self.connection: self.get_connection()
        if not param_list: param_list = self.default_param
        sql_param_list = []
        for param in param_list:
            if param in ['age_min', 'age_max', 'interest_list', 'global_limit', 'geo_dict_with_limit']:
                sql_param_list.append("q." + param)
            else:
                sql_param_list.append("c." + param)
        what_string = ", ".join(sql_param_list)
        query = self.query_dict['select_campaign'] % (what_string, str(client_id))
        if limit: query = query + " LIMIT %s" % str(limit)
        df = pd.read_sql(query, self.connection)
        show_df(df)

    def get_connection(self):
        if not self.connection:
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()
            self.close_connection = True

    def __del__(self):
        if self.close_connection: self.connection.close()


class SMSRouterStats(object):
    status_dict = {'Re?u' : "received", 'Re\xc3\xa7u' : 'received', 'Erreur (Temporaire)' : "temp_error",
                   'Erreur (Permanent)' : "npai", 'Envoy\xc3\xa9' : "sent", 'D\xc3\xa9sinscrit' : 'stop', 'nan' : ""}
    status_percentage = {'temp_error' : 0.25, 'npai' : 3.5}
    amplitude = 0.2

    def __init__(self, folder = None, file = None, router_df = None, ref_df = None, field_list = ['sms', 'gender', 'age'],
                 age_range = [25,35,45,55,65], connection = None, cursor = None, debug = True, write_to_csv = False,
                 csv_folder = None, csv_file = None):
        self.debug = debug
        self.cursor = cursor
        self.connection = connection
        self.close_connection = False
        self.router_df = router_df
        self.ref_df = ref_df
        self.field_list = field_list
        self.age_range = age_range
        self.folder = folder
        self.file = file
        self.write_to_csv = write_to_csv
        self.csv_folder = csv_folder
        self.csv_file = csv_file
        self.stats_df = None
        self.stop_df = None
        self.stats = {}
        if self.folder and self.file:
            self.upload_from_file()
        else:
            self.tidy_router_df()
        self.sms_list = [str(sms) for sms in self.router_df.sms.unique()]

    def get_connection(self):
        if not self.connection:
            if self.debug: print "--- Establishing connection with DB ---"
            self.connection = pg.get_connection()
            self.cursor = self.connection.cursor()
            self.close_connection = True

    def __del__(self):
        if self.close_connection: self.connection.close()

    def tidy_router_df(self):
        for col in list(self.router_df):
            if "Champ" in col:
                self.router_df.drop(col, axis = 1, inplace = True)
        self.router_df.rename(columns = {self.router_df.columns[0]: "sms",
                                         self.router_df.columns[1]: "status",
                                         self.router_df.columns[2]: "send_date",
                                         self.router_df.columns[3]: "receive_date",
                                         self.router_df.columns[4]: "stop"}, inplace = True)
        if self.debug:
            print "--- list of available status before cleaning ---"
            print list(self.router_df.status.unique())
        try:
            self.router_df.rename(columns = {self.router_df.columns[5]: "unknown"}, inplace = True)
        except:
            pass
        self.router_df.status = self.router_df.status.apply(lambda x: self.status_dict[x])

    def upload_from_file(self):
        if self.folder and self.file:
            self.router_df = pd.read_csv(self.folder + "/" + self.file, sep = str(";"))
        #show_df(self.router_df)
        for col in list(self.router_df):
            if "Champ" in col:
                self.router_df.drop(col, axis = 1, inplace = True)
        #show_df(self.router_df)
        mapping = {'phone':'sms','Statut':'status','envoi':'send_date','eption':'receive_date','inscrit':'stop',
                   'tures':'opens', 'Unnamed':'unknown'}
        cpt_col = 0
        for col in list(self.router_df):
            for letters, new_col in mapping.iteritems():
                #if letters in col:
                if str(col).find(letters) != -1:
                    self.router_df.rename(columns = {self.router_df.columns[cpt_col]: new_col}, inplace = True)
                    break
            cpt_col += 1
        show_df(self.router_df)
        if self.debug:
            print "--- list of available status before cleaning ---"
            print list(self.router_df.status.unique())
        self.router_df.sms = self.router_df.sms.apply(lambda x: '+' + str(x))
        self.router_df.status = self.router_df.status.apply(lambda x: self.status_dict[x])
        self.stats_df = self.router_df.copy(True)
        if self.debug:
            print "--- Uploaded Stats file : %s ---" % str(self.folder + "/" + self.file)
            show_df(self.router_df)
        #self.stats['a_global_brut'] = get_primary_stats(self.router_df, False, self.write_to_csv, self.csv_folder, self.csv_file,
                                                        # 'a_global_brut')

    def get_default_ref_df(self, debug = True):
        if not self.connection: self.get_connection()
        s = SMSQuery(connection = self.connection, cursor = self.cursor)
        s.where(sms_list = self.sms_list)
        s.select(self.field_list)
        self.ref_df = s.df
        #q = sms_query_builder(count = False, select_field = self.field_list, sms_list = self.sms_list)
        #self.ref_df = pd.read_sql(q, self.connection)
        if debug :
            print "--- Retrieved self.ref_df ---"
            show_df(self.ref_df)

    def append_ref_df(self, debug = True):
        self.stats_df = pd.merge(self.stats_df, self.ref_df, how = 'left', on = 'sms')
        if debug:
            print "--- Stats_df with appended info %s from the database ---" % str(list(self.field_list))
            show_df(self.stats_df)
        return self.stats_df.copy(True)

    def adjust_age(self):
        if self.age_range:
            if 'age' in list(self.stats_df):
            #self.stats_df['age'] = np.random.randint(0, 100, size=len(self.stats_df.index))
            #show_df(self.stats_df)
                age_min = self.age_range[0]
                age_max = self.age_range[-1]

                min_df = self.stats_df[self.stats_df.age < age_min]
                len_age_min = len(min_df.index)
                self.stats_df.age.iloc[min_df.index] = np.random.randint(30, 45, size=len_age_min)

                max_df = self.stats_df[self.stats_df.age >= age_max]
                len_age_max = len(max_df.index)
                self.stats_df.age.iloc[max_df.index] = np.random.randint(30, 45, size=len_age_max)

                unknown_age_df = self.stats_df[self.stats_df.age.isnull()]
                #show_df(unknown_age_df)
                len_unknown_age = len(unknown_age_df.index)
                self.stats_df.age.iloc[unknown_age_df.index] = np.random.randint(30, 45, size=len_unknown_age)

                #print "LIST OF AGE VALUES"
                #for k in sorted(list(self.stats_df.age.unique())):
                #    print k

                check = len(self.stats_df[(self.stats_df.age >= age_min) & (self.stats_df.age <= age_max)].index)
                if check != len(self.stats_df.index):
                    print "Warning! %s records do not have a valid age!" % str(check - len(self.stats_df.index))
                #unknown_age_df = self.stats_df[self.stats_df.age.isnull()]
                #show_df(unknown_age_df)

    def add_age_range(self):
        if 'age' in self.field_list:
            if self.age_range:
                age_range_df = create_age_range_df(self.age_range)
                #show_df(age_range_df)
                if 'age_range' in list(self.stats_df):
                    self.stats_df.drop('age_range', axis = 1, inplace = True)
                self.stats_df = pd.merge(self.stats_df, age_range_df, on = 'age', how = 'left')


    def get_stats_alexandra(self):
        stats_col = list(self.stats_df)
        for name in ['gender', 'ville', 'age_range']:
            for col in stats_col:
                if col.find(name) != -1:
                    if col.find("_x") != -1:
                        try:
                            self.stats_df.drop(name + "_y", axis = 1, inplace = True)
                            self.stats_df.rename(columns = {name + "_x" : name}, inplace = True)
                        except:
                            print "Warning : Enable to drop double columns in self.stats_df for name : %s" % str(name)
                            show_df(self.stats_df)
                        break

        print "--- Checking after attribution of age_range ---"
        print str(list(self.stats_df.age_range.unique()))
        pb_df = self.stats_df[self.stats_df.age_range.isnull()]
        show_df(pb_df)
        show_df(self.stats_df)
        cpt = 0
        for a, df_a in self.stats_df.groupby('age_range'):
            print a
            show_df(df_a)
            cpt += len(df_a.index)
        print str(cpt)

        with open(self.csv_folder + "/" + self.csv_file , 'ab+') as csv_file:
            w = csv.writer(csv_file, delimiter=';')
            w.writerow(["Statistiques Campagne (tous criteres confondus)"])
            w.writerow(["-"])
            write_stats_alexandra(self.stats_df, w, age_range_case = False)
            w.writerow(["Statistiques Par Genre"])
            for gender, df_g in self.stats_df.groupby('gender'):
                w.writerow(["-"])
                if gender in ['M', 'F']:
                    w.writerow(["HOMMES" if gender == 'M' else "FEMMES"])
                    w.writerow(["-"])
                    w.writerow(["M" if gender == 'M' else "MME", "ENVOYES", "EXPIRES", "% EXPIRES", "ECHEC", "% ECHECS",
                                "RECUS", "% RECUS"])
                    incr = create_incr()
                    for age_range, df_g_ar in df_g.groupby('age_range'):
                        res = write_stats_alexandra(df_g_ar, w, age_range_case = True, age_range = age_range)
                        incr = add_up_stats_res(incr, res)
                    incr = calc_pourcentage_in_incr(incr)
                    w.writerow(["TOTAL", str(incr['total_sms']), str(incr['temp_error']), str(incr['temp_error_%']),
                     str(incr['npai']), str(incr['npai_%']), str(incr['received']), str(incr['received_%'])])
                    #write_stats_alexandra(df_g, w, age_range_case = True, age_range = "TOTAL")
            if 'ville' in list(self.stats_df):
                for ville, df_v in self.stats_df.groupby('ville'):
                    w.writerow(["Statistiques globales pour la ville de %s" % str(ville)])
                    w.writerow(["-"])
                    write_stats_alexandra(df_v, w, age_range_case = False)
                    w.writerow(["Statistiques Par Genre pour la ville de %s" % str(ville)])
                    for gender, df_v_g in df_v.groupby('gender'):
                        w.writerow(["-"])
                        if gender in ['M', 'F']:
                            w.writerow(["HOMMES" if gender == 'M' else "FEMMES"])
                            w.writerow(["-"])
                            w.writerow(["M" if gender == 'M' else "MME", "ENVOYES", "EXPIRES", "% EXPIRES", "ECHEC", "% ECHECS",
                                "RECUS", "% RECUS"])
                            print "### DEBUG ###"
                            print str(list(df_v_g.age_range.unique()))
                            incr = create_incr()
                            for age_range, df_v_g_ar in df_v_g.groupby('age_range'):
                                res = write_stats_alexandra(df_v_g_ar, w, age_range_case = True, age_range = age_range)
                                incr = add_up_stats_res(incr, res)
                            incr = calc_pourcentage_in_incr(incr)
                            w.writerow(["TOTAL", str(incr['total_sms']), str(incr['temp_error']), str(incr['temp_error_%']),
                             str(incr['npai']), str(incr['npai_%']), str(incr['received']), str(incr['received_%'])])
                            #write_stats_alexandra(df_v_g, w, age_range_case = True, age_range = "TOTAL")

    def isolate_stops(self, debug = True):
        self.stop_df = self.stats_df[self.stats_df.stop == 1]
        if self.folder:
            self.stop_df.sms.to_csv(self.folder + "/desinscrits.csv", index = False)
        if debug:
            print "SMS desinscrits (correspondent au 'stop' dans les statistiques)"
            for sms in self.stop_df.sms.tolist():
                print sms

    def get_stats_groupby(self, gr_field = 'ville', sep = ' - ', debug = True):
        self.stats['a_global'] = get_primary_stats(self.stats_df, False, self.write_to_csv, self.csv_folder, self.csv_file, 'a_global')
        try:
            gr = self.stats_df.groupby(gr_field)
        except:
            gr_field = gr_field + "_x"
            gr = self.stats_df.groupby(gr_field)
        for value, df in gr:
            if debug:
                print "--- groupby %s --> %s ---" % (str(gr_field), str(value))
                show_df(df)
            self.stats[gr_field + sep + value] = get_primary_stats(df, False, self.write_to_csv, self.csv_folder, self.csv_file,
                                                                   gr_field + sep + value)

    def adjust_sent_number_groupby(self, target_dict, gr_field = 'ville', amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        gr = self.stats_df.groupby(gr_field)
        if debug:
            print "### Adjusting sent numbers in groupby('%s') self.stats_df ###" % str(gr_field)
        for value, df in gr:
            try:
                n_target = target_dict[value]
            except:
                break
            print "--------- interating in groupby('%s') : %s ----------" % (str(gr_field), str(value))
            #show_df(df)
            self.adjust_sent_number(df, n_target, amplitude, debug = debug)

    def adjust_received_number_groupby(self, target_dict, gr_field = 'ville', amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        gr = self.stats_df.groupby(gr_field)
        if debug:
            print "### Adjusting received numbers in groupby('%s') self.stats_df ###" % str(gr_field)
        for value, df in gr:
            try:
                n_target = target_dict[value]
            except:
                break
            print "--------- interating in groupby('%s') : %s ----------" % (str(gr_field), str(value))
            #show_df(df)
            self.adjust_received_number(df, n_target, amplitude, debug = debug)

    def adjust_status_distribution_groupby(self, gr_field = 'ville', amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        gr = self.stats_df.groupby(gr_field)
        if debug:
            print "### Adjusting status distribution in groupby('%s') self.stats_df ###" % str(gr_field)
        for value, df in gr:
            print "--------- interating in groupby('%s') : %s ----------" % (str(gr_field), str(value))
            #show_df(df)
            self.adjust_status_distribution(df, amplitude, debug = debug)

    def remove_weird_status(self, status_list = ['received', 'npai', 'temp_error'], debug = True):
        existing_status_list = list(self.stats_df.status.unique())
        if debug:
            print "--- Inspecting status seen in stats given by the router ---"
            print str(existing_status_list)
        for status in existing_status_list:
            if status not in status_list:
                modified_index = self.stats_df[self.stats_df['status'] == status].index
                self.stats_df.status.iloc[modified_index] = 'received'
                if debug : print "adjusted %s records with status : %s" % (str(len(modified_index)), str(status))

    def adjust_status_distribution(self, df, amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        n_records = len(df.index)
        status_percentage = shake_dict(self.status_percentage, amplitude)
        for status, percentage in status_percentage.iteritems():
            status_target = int(percentage * n_records / 100)
            status_index = df[df['status'] == status].index
            status_actual = len(status_index)
            status_to_remove = status_actual - status_target
            if status_to_remove > 0:
                remove_index = random.sample(status_index, status_to_remove)
                self.stats_df.status.iloc[remove_index] = 'received'
                if debug: print "## modified %s records from '%s' status to 'received'" % (str(len(remove_index)), status)
        return df

    def adjust_sent_number(self, df, target, amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        actual_index = df.index
        n_actual = len(actual_index)
        to_remove = n_actual - target
        if to_remove > 0:
            status_percentage = shake_dict(self.status_percentage, amplitude)
            for status, percentage in status_percentage.iteritems():
                status_target = int(percentage * target / 100)
                status_index = df[df['status'] == status].index
                status_actual = len(status_index)
                status_to_remove = status_actual - status_target
                if status_to_remove > 0:
                    if status_to_remove > to_remove: status_to_remove = to_remove
                    remove_index = random.sample(status_index, status_to_remove)
                    self.stats_df.drop(remove_index, axis = 0, inplace = True)
                    if debug: print "## removed %s records with '%s' status" % (str(len(remove_index)), status)
                    to_remove = to_remove - status_to_remove
                    if to_remove <= 0:
                        break
        if to_remove > 0:
            status_targeted_index = df[df['status'] == 'received'].index
            status_targeted_actual = len(status_targeted_index)
            if to_remove <= status_targeted_actual:
                remove_index = random.sample(status_targeted_index, to_remove)
                self.stats_df.drop(remove_index, axis = 0, inplace = True)
                if debug: print "## removed %s records with '%s' status" % (str(len(remove_index)), 'received')
        if to_remove < 0:
            self.add_records(df, - to_remove, debug = debug)
        return df

    def adjust_received_number(self, df, target, amplitude = None, debug = True):
        if not amplitude:
            amplitude = self.amplitude
        actual_index = df[df['status'] == 'received'].index
        n_actual = len(actual_index)
        to_remove = n_actual - target
        if to_remove > 0:
            status_percentage = shake_dict(self.status_percentage, amplitude)
            for status, percentage in status_percentage.iteritems():
                status_target = int(percentage * n_actual / 100)
                status_index = df[df['status'] == status].index
                status_actual = len(status_index)
                status_to_remove = status_actual - status_target
                if status_to_remove > 0:
                    if status_to_remove > to_remove: status_to_remove = to_remove
                    remove_index = random.sample(status_index, status_to_remove)
                    self.stats_df.drop(remove_index, axis = 0, inplace = True)
                    if debug: print "## removed %s records with '%s' status" % (str(len(remove_index)), status)
                    to_remove = to_remove - status_to_remove
                    if to_remove <= 0:
                        break
        if to_remove > 0:
            status_targeted_index = df[df['status'] == 'received'].index
            status_targeted_actual = len(status_targeted_index)
            if to_remove <= status_targeted_actual:
                remove_index = random.sample(status_targeted_index, to_remove)
                self.stats_df.drop(remove_index, axis = 0, inplace = True)
                if debug: print "## removed %s records with '%s' status" % (str(len(remove_index)), 'received')
        if to_remove < 0:
            self.add_records(df, - to_remove, debug = debug)

    def add_records(self, df, n, value_dict = None, debug = True):
        add_df = pd.DataFrame()
        add_df['sms'] = ['+336' + str(random.randint(11111111, 99999999)) for x in xrange(n)]
        add_df['status'] = 'received'
        add_df['send_date'] = sample_list(df.send_date.tolist(), n)
        add_df['receive_date'] = sample_list(df.receive_date.tolist(), n)
        add_df['stop'] = '0'
        add_df['unknown'] = 'NaN'
        short_list = list(df)[:]
        for col in ['sms', 'status', 'send_date', 'receive_date', 'stop', 'unknown']:
            short_list.remove(col)
        if value_dict:
            for col, value in value_dict.iteritems():
                if col in short_list:
                    add_df[col] = value
        else:
            for col in short_list:
                add_df[col] = sample_list(df[col].tolist(), n)
        if debug:
            print "## adding %s records to stats_df (see add_df below)" % str(n)
            #show_df(add_df)
        old_len = len(self.stats_df)
        self.stats_df = self.stats_df.append(add_df)
        new_len = len(self.stats_df)
        if debug:
            print "   --> stats_df from %s records, to %s records" % (str(old_len), str(new_len))

    def add_ville(self, df, ville_list = ['Paris', 'Lyon', 'Marseille', 'Nice', 'Lille', 'Toulouse', 'Bordeaux']):
        ville_df = pd.DataFrame(data = [[x, ville_list[x]] for x in range(len(ville_list))],
                                columns=['ville_num', 'ville'])
        show_df(ville_df)
        df['ville_num'] = np.random.randint(0, len(ville_list), size=len(df))
        show_df(df)
        df = pd.merge(df, ville_df, on='ville_num')
        show_df(df)
        df.drop('ville_num', axis = 1, inplace = True)
        show_df(df)
        return df


class PrimoTextoAPI(object):
    headers = {'content-type': "application/json",
               'X-Primotexto-ApiKey': 'a3a5d60a2900fa425f4edb2d03ea157d'}
    url = {'list' : 'https://api.primotexto.com/v2/lists/',
           'upload' : 'https://api.primotexto.com/v2/lists/%s/contacts',
           'import' : 'https://api.primotexto.com/v2/lists/%s/import',
           'campaign' : 'https://api.primotexto.com/v2/marketing/campaigns',
           'bat' : 'https://api.primotexto.com/v2/marketing/campaigns/%s/test',
           'send' : 'https://api.primotexto.com/v2/marketing/campaigns/%s/send',
           'status' : 'https://api.primotexto.com/v2/campaigns/%s/status'}

    def __init__(self):
        self.list_id = None
        self.campaign_id = None

    def create_list(self, name):
        data = {"name": name}
        res = requests.post(self.url['list'], data = json.dumps(data), headers = self.headers)
        print "## New list created in PrimoTexto: '%s' -- %s ##" % (name, str(res.text))
        try:
            self.list_id = res.json()['id']
            return self.list_id
        except:
            print "Warning! API Call to PrimoTexto failed!!"
            print str(res.text)

    def upload_contact(self, sms, list_id = None):
        if list_id:
            self.list_id = list_id
        data = {"identifier": sms}
        res = requests.post(self.url['upload'] % str(self.list_id), data = json.dumps(data), headers = self.headers)
        try:
            contact_id = res.json()['id']
        except:
            print "Warning! API Call to PrimoTexto failed!!"
            print str(res.text)

    def upload_list(self, contact_df = None, list_id = None):
        if list_id:
            self.list_id = list_id
        if 'sms' in list(contact_df):
            contact_df.rename(columns = {'sms' : 'tel'}, inplace = True)
            contact_df = contact_df.replace(np.nan, 'None')
            data = contact_df.values.tolist()
            data.insert(0, list(contact_df))
        else:
            print "Warning! No SMS numbers passed to upload list '%s'" % str(self.list_id)
            return False
        for row in data[:5]:
            print str(row)
        res = requests.post(self.url['import'] % str(self.list_id), data = json.dumps(data), headers = self.headers)
        print str(res.text)
        if not res.text:
            print "## %s contacts successfully uploaded in PrimoText, in list : %s ##" % (str(len(contact_df.index)), str(self.list_id))
            return True

    def create_campaign(self, name, message, sender, send_date = None, url = None, list_id = '5a61d9f07076b97ff6360ae3'):
        if list_id:
            self.list_id = list_id
        data = {"name": name,
	            "message": message,
	            "sendList": {"id": self.list_id},
                "sourceAddress" : sender}
        if send_date:
            try:
                parsed_date = dparser.parse(send_date, fuzzy = True, dayfirst = True)
                date_in_s = parsed_date.strftime('%s')
                date_in_ms = int(date_in_s) * 1000
                print str(date_in_ms)
                data['date'] = date_in_ms
            except:
                print "** Warning !! No send_date **"
        if url:
            data["externalUrl"] = url
            data["landingPageType"] = "EXTERNAL"
        res = requests.post(self.url['campaign'], data = json.dumps(data), headers = self.headers)
        print res.text
        try:
            self.campaign_id = res.json()['campaignId']
            return self.campaign_id
        except:
            return False

    def send_bat(self, sms_list, campaign_id = None):
        if campaign_id:
            self.campaign_id = campaign_id
        for sms in sms_list:
            data = {"identifier": sms}
            res = requests.post(self.url['bat'] % str(self.campaign_id), data = json.dumps(data), headers = self.headers)
            print res.text
            #try:
            #    res.json()['creditsUsed']
            #    return True
            #except:
            #    return False

    def send_bat_item(self, sms, campaign_id = None):
        if campaign_id:
            self.campaign_id = campaign_id
        data = {"identifier": sms}
        res = requests.post(self.url['bat'] % str(self.campaign_id), data = json.dumps(data), headers = self.headers)
        print res.text
        try:
            res.json()['creditsUsed']
            return sms
        except:
            return False

    def send(self, campaign_id = None):
        if campaign_id:
            self.campaign_id = campaign_id
        res = requests.post(self.url['send'] % str(self.campaign_id), headers = self.headers)
        print res.text

    def status(self, campaign_id = None):
        if campaign_id:
            self.campaign_id = campaign_id
        res = requests.get(self.url['status'] % str(self.campaign_id), headers = self.headers)
        print res.text

class Formatter(object):
    def __init__(self):
        self.types = {}
        self.htchar = '\t'
        self.lfchar = '\n'
        self.indent = 0
        self.set_formater(object, self.__class__.format_object)
        self.set_formater(dict, self.__class__.format_dict)
        self.set_formater(list, self.__class__.format_list)
        self.set_formater(tuple, self.__class__.format_tuple)

    def set_formater(self, obj, callback):
        self.types[obj] = callback

    def __call__(self, value, **args):
        for key in args:
            setattr(self, key, args[key])
        formater = self.types[type(value) if type(value) in self.types else object]
        return formater(self, value, self.indent)

    def format_object(self, value, indent):
        return repr(value)

    def format_dict(self, value, indent):
        items = [
            self.lfchar + self.htchar * (indent + 1) + repr(key) + ': ' +
            (self.types[type(value[key]) if type(value[key]) in self.types else object])(self, value[key], indent + 1)
            for key in value
        ]
        return '{%s}' % (','.join(items) + self.lfchar + self.htchar * indent)

    def format_list(self, value, indent):
        items = [
            self.lfchar + self.htchar * (indent + 1) + (self.types[type(item) if type(item) in self.types else object])(self, item, indent + 1)
            for item in value
        ]
        return '[%s]' % (','.join(items) + self.lfchar + self.htchar * indent)

    def format_tuple(self, value, indent):
        items = [
            self.lfchar + self.htchar * (indent + 1) + (self.types[type(item) if type(item) in self.types else object])(self, item, indent + 1)
            for item in value
        ]
        return '(%s)' % (','.join(items) + self.lfchar + self.htchar * indent)

    def format_ordereddict(self, value, indent):
        items = [
            self.lfchar + self.htchar * (indent + 1) +
            "(" + repr(key) + ', ' + (self.types[
                type(value[key]) if type(value[key]) in self.types else object
            ])(self, value[key], indent + 1) + ")"
            for key in value
        ]
        return 'OrderedDict([%s])' % (','.join(items) +
               self.lfchar + self.htchar * indent)

class SignNow(object):
    key = {'client_id': '0fccdbc73581ca0f9bf8c379e6a96813',
           'secret': '3719a124bcfc03c534d4f5c05b5a196b',
            'ENCODED_CLIENT_CREDENTIALS': 'MGZjY2RiYzczNTgxY2EwZjliZjhjMzc5ZTZhOTY4MTM6MzcxOWExMjRiY2ZjMDNjNTM0ZDRmNWMwNWI1YTE5NmI='}

class Intuit(object):
    key = {'client_id' : 'Q0r92u7K7Ju7pTgwYeLVTw37Dce3xW5kMsCUP7RZdvkqQSckKG',
           'secret' : 'wD4dmmt1eXIDGPIxeSThOqYnN90evWD2nl4XErQj',
           'redirect_uri' : 'https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'}

api_key = 'AIzaSyA2AWv2WRxuBmowrMwWgS24c-aC_BgrapM'

