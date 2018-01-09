# -*- coding: utf-8 -*-
from sms_object_toolbox import *
import json
import pprint

# campagne Alexandra 5-jan-18
""" xxx
folder = "/home/david/comptage_sms/alexandra/amplitude_05-01-18"
campagne = "Alexandra_Amplitude_05-01-18"
message = u"Ce weekend, pendant les portes ouvertes, la gamme SUV est à partir de 149€ / mois chez Opel. Cliquez pour prendre RDV http://bit.ly/jevaist’envoyerlelienquandilseradispo"
message = message.encode("utf-8")
exp = "OPEL AMPLITUDE"
bat_david = ['+33680835196']
bat_alexandra = ['+33679312656']
cp_list = ['10000', '52000', '89000', '89100', '77240', '77170', '77210', '77130']
s = SMSQuery(client = 'Alexandra')
s.where(cp_list = cp_list, cp_precision = 3, age_min = 30, age_max = 65)
s.select_sample(limit = int(800 / 0.14))
"""

# campagne Yakare 5-jan-18
""" xxx """
folder = "/home/david/comptage_sms/yakare/renault_5regions_05-01-18"
campagne = "Yakare_Renault_5-regions_05-01-18"
message = u"Renault Gueudet révèle ses PEPITES: voitures de -10km, disponibles, à prix d’or! Attention stock limité, jusqu’au 31/01/18. http://bit.ly/pepiteS"
message = message.encode("utf-8")
exp = "RENAULT"
list_id = "5a4f5bb6ab234161a12c1483"
camp_id = "5a4f5bb7ab234161a12c1484"
bat_david = ['+33680835196']
bat_yakare = ['+33638660499', '+33622138212']

pt = PrimoTextoAPI()
pt.create_list(campagne)
pt.upload_contact('+33680835196', '5a37da867076b97ff6320662')
pt.upload_list(['+33638660499', '+33622138212'])
pt.create_campaign(campagne, message, exp, "05-01-18 14:00:00")
pt.send_bat(bat_yakare, campaign_id = camp_id)
pt.send(campaign_id = "5a380f397076b97ff6320db3")
pt.status(campaign_id = "5a380f397076b97ff6320db3")

for region in ['idf', "picardie", 'haute-normandie', 'nord', 'rhone-alpes']:
    s = SMSQuery(client = "yakare")
    s.where(region_list = region, age_min = 25, age_max = 60)
    s.select_sample(limit = 2500)
    s.hlr_cleanup("Yakare_05-01-18_" + str(region), True, folder, str(region) + "_hlr.csv", False)
    s.upload_to_router(list_id = list_id)


# comptage Alexandra IDF
""" xxx
#cp_list = ['75', '77', '78', '91', '92', '93', '94', '95']
s = SMSQuery(client = 'Pauline')
#s.where(age_min = 25, age_max = 45, civi = 'F')
#s.count_sms()
#s.select(limit = 11000)
#s.hlr_cleanup('pauline_4-jan-18', True, '/home/david/comptage_sms/pauline', '9523_4-jan-18_hlr.csv', False)

s.valid_df = HLR().process_file("/home/david/comptage_sms/pauline", "pauline_4-jan-18.csv")

s.upload_to_router('pauline_4-jan-18')
"""

# stats
""" xxx
city_cap = {'Saint Quentin' : 1000,
            'Lisieux' : 500,
            'Evreux' : 1500,
            'Vernon' : 500,
            'Beauvais' : 1000,
            'Compiegne' : 750,
            'Senlis' : 250,
            'Boulogne' : 750,
            'Rouen' : 2050,
            'Dieppe' : 250,
            'Le Havre' : 2550,
            'Amiens' : 2050,
            'Abbeville' : 239}

folder = "/home/david/comptage_sms/alexandra/nissan_speed_dating/stats"
for ds in ["28-11", "30-11", "5-12", "7-12"]:
    file = "stats_%s.csv" % str(ds)

    s = SMSRouterStats(folder = folder, file = file, debug = False,
                       write_to_csv = True, csv_folder = folder, csv_file = "alex2" + file)
    s.remove_weird_status()
    s.adjust_sent_number_groupby(city_cap)
    s.adjust_sent_number_groupby(city_cap)
    #s.adjust_status_distribution_groupby()
    s.get_default_ref_df(debug = False)
    s.append_ref_df()
    s.adjust_age()
    s.add_age_range()

    #s.get_stats_groupby('ville', debug = False)
    #s.get_stats_groupby('age_range', debug = False)
    #s.get_stats_groupby('gender', debug = False)
    s.get_stats_alexandra()

    #for k,v in sorted(s.stats.iteritems()):
    #    print k,v
    #s.stats = json.dumps(s.stats)
    #pretty(s.stats)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(s.stats)

#age_range_df = create_age_range_df()
#show_df(age_range_df)
"""

# stats yakare
""" xxx
folder = "/home/david/comptage_sms/yakare/opel"
file = "stats2.csv"
s = SMSRouterStats(folder, file)
#s.adjust_sent_number(s.stats_df, 14000)
#s.adjust_status_distribution(s.stats_df)
s.isolate_stops()
get_primary_stats(s.stats_df)
"""

# yakare lyon +20km
""" xxx
cp_list = ['690', '698', '382', '693']
s = SMSQuery()
s.where(cp_list=cp_list, city_cp_strict=True)
s.count_sms(True)
"""

# yakare paris 12
""" xxx
cp_cap = {'75012' : 3000 , '75004' : 2000, '75011' : 2000, '75013' : 2000, '75020' : 1000}
s = SMSQuery()
s.where(cp_list = cp_cap.keys())
#s.select(['sms','cp'])
#cap_df = cap_multiple_size_group_by_col(s.df, 'cp', enlarge_sample(cp_cap))
folder = "/home/david/comptage_sms/yakare/paris_12"
#cap_df.sms.to_csv(folder + "/paris12.csv", header = False, index = False)
HLR().process_file(folder, "paris12_hlr.csv", write_in_db = False)
"""

# test HLR()
""" xxx
from hlr_batch import *
#print getBatchStatus('13812')
#print downloadBatch('13812')
print createRequest(['+33680835196', '+33689451274'], 'test')
"""

# yakare 78240 95310 60000 60740
""" xxx
folder = "/home/david/comptage_sms/yakare/4cp_plus_20km"
c_name = 'test'
#s = SMSQuery()
#s.where(cp_list='60740', cp_precision = 2, age_min = 30, age_max = 65)
#s.count_sms()
#df = s.select(limit = 6500)
#HLR(df.sms.tolist()).batch(c_name, True, folder, c_name + 'hlr_ok.csv')
HLR(['+33680835196', '+33689451274']).batch(c_name, True, folder, c_name + '_hlr_ok.csv')
#HLR().process_file(folder, c_name + "_hlr.csv")
"""

# test HLR
""" xxx
#h = HLR(['+33680835196', '+33603018181'])
#h.batch()
#h.check_batch_status(13812)
#print h.batch_status
#h.download_batch(13686)

#h = HLR(['+33680835196', '+33689451274'])
#h.batch()
"""

# test SMSQuery()
""" xxx
s = SMSQuery()
s.where(city_list=['libourne','biarritz'], cp_precision = 3)
s.count_sms(groupby_count = True)
#s.select(['sms', 'cp', 'age'], limit = 50)
#s.hlr_df()
"""

# alexandra amplitude 15-12
""" xxx
cp_list = [10000, 89000, 52000, 89100, 21000, 21200, 39100, 45500, 45200]
s = SMSQuery(client = 'Alexandra')
#s.where(cp_list = cp_list, age_min = 30, age_max = 65)
#s.count_sms(groupby_count=False)
#s.select(limit = 7142)
#s.hlr_cleanup('alexandra')

#HLR().process_file("/home/david/comptage_sms/alexandra/amplitude_15-12", "alexandra_hlr.csv")
"""

# Parameters from client campaigns
"""
city_dict = {'Paris' : 6000,
'Boulogne' : 1200,
'Neuilly' : 1150,
'Levallois' : 1150,
'Rueil' : 1000,
'Velizy' : 1000,
'Villiers' : 1000}

#cp_dict = {'75001':6000,'92100':1200, '92300':1150, '92200':1150, '92500':1000,'78140':1000, '95400':1000}

cp_dict = {'02100' : 1000,
'14100' : 500,
'27000' : 1500,
'27200' : 500,
'27500' : 500,
'60000' : 1000,
'60200' : 750,
'60300' : 250,
'62200' : 750,
'76000' : 2050,
'76200' : 250,
'76600' : 2050,
'80000' : 2050,
'80100' : 239}

city_cp_df = pd.DataFrame([['Saint Quentin','02100'],
['Lisieux','14100'],
['Evreux','27000'],
['Vernon','27200'],
['Vernon','27950'],
['Le Havre','27500'],
['Beauvais','60000'],
['Compiegne','60200'],
['Compiegne','60280'],
['Senlis','60300'],
['Boulogne','62200'],
['Rouen','76000'],
['Dieppe','76200'],
['Le Havre','76600'],
['Amiens','80000'],
['Abbeville','80100']], columns = ['ville', 'cp'])

como_dict_ville = {'Paris':8000,'Pontoise':3000,'Saint Denis':3000}
como_dict_cp = {'75':8000,'95300':3000,'93200':3000}


ville_list = ['Gradignan','Merignac','Pessac','Talence',"Villenave",'Begles']
# sms_query_by_city(ville_list, 2, count = False, select_field = ['sms', 'age'])
"""

# before HLR
"""
db_conn = pg.get_connection()
cp_list, city_cp_df = get_cp_from_city(ville_list, db_conn, strict = True)
#city_list, city_cp_df = get_city_from_cp(cp_dict.keys(), db_conn)
show_df(city_cp_df)

folder = "/home/david/comptage_sms/yakare/como_vp_2-dec"
file = "como.csv"
#create_path_if_does_not_exist(folder)


print "--- Initial Selection ---"
cp_list = {'10600':200, '52000':200, '89000':200, '89100':200, '77240':200, '77130':200, '77170':200}
#cp_list = {'10600':1250}
q = sms_query_builder(cp_list.keys(), None, cp_nb_char_precision = 5,
                      age_min = None, age_max = None, civi = ['M','F'],
                      count = False, groupby_count = True, groupby_count_field = ['cp'],
                      select_field = ['sms', 'cp'])

db_conn = pg.get_connection()
df = pd.read_sql(q, db_conn)
#df['cp4'] = df.cp.apply(lambda x : str(x)[:3] + '00')
#print df.cp4.unique()
#df = df.groupby('cp4').sum()
#show_df(df, 8)
#show_group_by_df(df, 'cp')
df = cap_multiple_size_group_by_col(df, 'cp', enlarge_sample(cp_list))
show_group_by_df(df, 'cp')

folder = "/home/david/comptage_sms/yakare/opel"
file = "opel2.csv"
df.to_csv(folder + "/" + file, index = False, header = False)
df.sms.to_csv(folder + "/" + file.replace(".csv", "_hlr.csv"), index = False, header = False)
"""

# clean file coming from HLR
""" xxx
folder = "/home/david/comptage_sms/yakare/opel"
#file = "opel.csv"
#h = APIHLRLookup(df.sms.tolist())
#h.batch()
h = HLR()
h.process_file(folder, "opel2_hlr_checked.csv")
"""


# another before HLR
"""

#df = df.sample(n=6000)
#cp3_city_df = pd.DataFrame([['750','Paris'], ['953','Pontoise'], ['932','Saint Denis']], columns = ['cp3', 'ville'])
df = pd.merge(df, city_cp_df)
#df = df[['sms', 'ville']]
show_df(df)
show_group_by_df(df, 'ville')
#df = cap_multiple_size_group_by_col(df, 'ville', enlarge_sample(como_dict_ville))
#df.sms.to_csv(folder + "/" + file, index = False, header = False)


#large_extract = pd.read_csv(folder + "/" + file, dtype = basestring)
res_df = filter_good_sms_hlr_file(folder, file.replace(".csv", "_hlr.csv"), debug = True)
#res_df = pd.merge(large_extract, res_df.to_frame('sms'), on = 'sms')
show_df(res_df)
#res_df[['sms','year_old','gender','ville']].to_csv(folder + "/" + file_name.replace(".csv", "_go."), index = False)
res_df.to_csv(folder + "/" + file.replace('.csv', '_go.csv'), index = False, header = False)
print "--- CSV with HLR-clean SMS printed OK to '%s' ---" % str(folder + "/" + file.replace(".csv", "_go."))
print "--- %s contacts ready to be imported in PrimoTexto ---" % str(len(res_df.index))
"""

# nissan -- before HLR
"""
sent_df = pd.read_csv(folder + "/nissan_hlr-ok_2017-11-27.csv", dtype = basestring)
#sent_df.sms = sent_df.sms.apply(lambda x : "+" + str(x))
show_df(sent_df)
sent_df.drop(['cp','ville'], axis = 1, inplace=True)
show_df(sent_df)

df = tag_records_in_df_with_other_df(df, sent_df, '2017-11-28')
show_df(df)

generate_sending_lists_case_nissan(df, 'cp', enlarge_sample(cp_dict), 'send_date',
                                   ['2017-11-29','2017-11-30','2017-12-01'],
                                    folder = folder, debug = True)


#df.to_csv(folder + "/full_extract_nissan_age_gender.csv", header = True, index = False)

#show_group_by_df(df, 'cp')

print "--- Modified DF for HLR ---"
df = cap_multiple_size_group_by_col(df, 'cp', enlarge_sample(cp_dict))

print "--- Modified DF to file ---"
#df = df['sms']
show_df(df)


df['sms'].to_csv(folder + "/" + file, index = False)
print "--- CSV printed OK in '%s' ---" % str(folder + "/" + file)
"""

""" xxx
large_extract = pd.read_csv(folder + "/full_extract_nissan_age_gender_send-dates.csv", dtype = basestring)
show_df(large_extract)
supp = large_extract.loc[pd.isnull(large_extract['send_date']), ['sms','cp','ville','age','gender']]
show_df(supp)
supp = cap_multiple_size_group_by_col(supp, 'cp', enlarge_sample(cp_dict, 0.75, 1))
show_df(supp)
supp.to_csv(folder + "/nissan_supp.csv", index = False)
supp.sms.to_csv(folder + "/nissan_supp_for_hlr.csv", index = False)
"""

"""
for send_case in ['2017-11-29','2017-11-30','2017-12-01']:
    file_name = "nissan_capped_for_%s_hlr.csv" % str(send_case)
    #df = pd.read_csv(folder + "/nissan_capped_for_%s_hlr.csv" % str(send_case), dtype = basestring)
    res_df = filter_good_sms_hlr_file(folder, file_name, debug = True)




    res_df = pd.merge(large_extract, res_df.to_frame('sms'), on = 'sms')
    show_df(res_df)
    res_df[['sms','year_old','gender','ville']].to_csv(folder + "/" + file_name.replace(".csv", "_go."), index = False)
    print "--- CSV with HLR-clean SMS printed OK to '%s' ---" % str(folder + "/" + file_name.replace(".csv", "_go."))
    print "--- %s contacts ready to be imported in PrimoTexto ---" % str(len(res_df.index))



df = pd.read_csv(folder + "/" + file.replace(".", "_hlr."))
    show_df(df)
    df = df[df['Error Code'] == 0]
    df.sms = df.MSISDN.apply(lambda num: "+" + str(num))
    res_df = df.sms
    show_df(res_df)
"""
""" Nissan specific (gets data in large extract to create a supp file
large_extract = pd.read_csv(folder + "/full_extract_nissan_age_gender_send-dates.csv", dtype = basestring)
show_df(large_extract)
file_name = "nissan_supp_for_hlr_res.csv"
res_df = filter_good_sms_hlr_file(folder, file_name, debug = True)
res_df = pd.merge(large_extract, res_df.to_frame('sms'), on = 'sms')
show_df(res_df)
res_df[['sms','year_old','gender','ville']].to_csv(folder + "/" + file_name.replace(".csv", "_go.csv"), index = False)
print "--- CSV with HLR-clean SMS printed OK to '%s' ---" % str(folder + "/" + file_name.replace(".csv", "_go.csv"))
print "--- %s contacts ready to be imported in PrimoTexto ---" % str(len(res_df.index))
"""
"""
age_range = create_age_range_df([2,4,6,10,20])
print age_range

folder = "/home/david/comptage_sms/alexandra/amplitude"
file = "amplitude easy VO.csv"
stats_df = pd.read_csv(folder+"/"+file, sep = str(";"), error_bad_lines = False)

stats_df = clean_router_df(stats_df)
show_df(stats_df)
sms_list = [str(sms) for sms in list(stats_df.sms.unique())]
print len(sms_list)
q = sms_query_builder(count = False, select_field = ['sms','age','gender'],
                      sms_list = sms_list)
ref_df = pd.read_sql(q, pg.get_connection())
show_df(ref_df)
stats_df = pd.merge(stats_df, ref_df, 'left', 'sms')
show_df(stats_df)

keep_col = ['1', '3', '4', '5', '7', '8']
for col in list(stats_df):
    if "Champ" in col:
        for check in keep_col:
            if check in col:
                stats_df.drop(col, axis = 1, inplace = True)
                break
stats_df['Champ2'] = stats_df['Champ2'].apply(lambda x : 'F' if len(str(x)) > 1 else 'M')
stats_df.rename(columns={ stats_df.columns[0]: "sms" }, inplace = True)
show_df(stats_df)
print stats_df['Statut'].unique()
print stats_df['Champ2'].unique()
print stats_df['D?sinscrit'].unique()
"""