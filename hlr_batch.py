#!/usr/bin/env python
import os, time, smtplib, csv, httplib, urllib, json, re, operator, collections, shutil, sys, string
from pprint import pprint
import inotify.adapters
from collections import defaultdict
from shutil import copyfile
import uuid

debug = True
debuglevel = 10

key = "JlqVVuRPYTi6vTsBQwDom8KDKkteKVHU"
secret = "david9690"

personalcache = '30'
sharedcache = '30'
path_to_watch = '/home/david/PycharmProjects/test/hlr_batch_in'
path_to_write = '/home/david/PycharmProjects/test/hlr_batch_out'
url = "www.hlrlookup.com"

def getBatchStatus(batchid, debug = False):
    conn = httplib.HTTPSConnection(url)
    conn.request("GET", "https://"+url+"/api/status/"+batchid+"?apikey="+key+"&password="+secret)
    response = conn.getresponse()
    data = response.read().decode('utf-8')
    data = re.sub('\?.*?"', '"', data)
    if debug: print("Batch status response: "+data)
    conn.close()
    jdata = json.loads(data)
    if debug: print jdata
    return jdata

def downloadBatch(batchid):
    conn = httplib.HTTPSConnection(url)
    conn.request("GET", "https://"+url+"/api/download/"+batchid+"?apikey="+key+"&password="+secret)
    response = conn.getresponse()
    data = response.read()#.decode('utf-8');
    conn.close()
    return data

def uploadRequest(msisdns, batchname = 'upload_with_Python', debug = True):
    if debug:
        print "--- Sending bulk HLR request on %s SMS ---" % str(len(msisdns))
    data = ''
    count = 0
    for msisdn in msisdns:
        #print "Adding ", msisdn
        data += msisdn
        data += ','
        count += 1

    params = 'data='+data+'&start=yes&batchname='+batchname

    headers = {"Content-type": "application/x-www-form-urlencoded"}
    conn = httplib.HTTPSConnection(url)
    conn.set_debuglevel(debuglevel)
    conn.request("POST", "https://"+url+"/api/bulk/?apikey="+key+"&personalcache="+personalcache+"&sharedcache="+sharedcache+"&password="+secret, params, headers)
    #print("POST URL is https://"+url+"/api/bulk/?apikey="+key+"&personalcache="+personalcache+"&sharedcache="+sharedcache+"&password="+secret)
    #print("Data:\n"+data)
    response = conn.getresponse()
    #print(response.status, response.reason)
    data = response.read().decode('utf-8')
    #print(data)
    if data == 'Bad Request missing msisdn':
        ## Code here what to do if HLRLookup doesn't receive any msisdn. Maybe send an email warning?
        return ''
    try:
        jdata = json.loads(data)
    except Exception as e:
        ## Code here what to do if upload to HLRLookup doesn't work. Maybe send an email warning?
        return ''
    pprint(jdata)
    batchid = str(jdata["batchid"])
    return batchid, jdata

def createRequest(msisdns, batchname, debug = True):

    batchid, jdata = uploadRequest(msisdns, batchname, debug)
    #if debug:
    #    print("BatchID: "+batchid)
    #    print("Status: "+jdata["status"])
    #    print("Batch Status: "+getBatchStatus(batchid)['status'])
    while 1:
        status = getBatchStatus(batchid)
        if status['status'] == "complete":
            break
        print("Batch status is %s (%s SMS remaining -- batch is %s pourcent completed" %
              (status['status'], status['records_remaining'], status['percentage_complete']))
        time.sleep(10)

    if debug:
        print("Batch Complete!")
        print("--- Downloading batch ---")

    while 1:
        batchdata = downloadBatch(batchid)
        if batchdata != 'CSV generation pending':
            break
        print("Waiting for batch to generate...")
        time.sleep(10)

    print("--- Batch downloaded OK ---")
    #conn.close()
    return batchdata


def resendHLR(msisdn, row):
    print("Resending MSISDN: "+msisdn)

    try:
        conn = httplib.HTTPSConnection(url)
        conn.set_debuglevel(debuglevel)
        conn.request("GET", "https://"+url+"/api/hlr/?apikey="+key+"&password="+secret+"&msisdn="+msisdn)
        response = conn.getresponse()
        data = response.read().decode('utf-8')
        jdata = json.loads(data)
        print(jdata['data'][msisdn])
        return jdata['data'][msisdn], 'pass'
    except:
        print("Either no response or timeout - using previous inconclusive result.")
        return row, 'fail'

def mySort(row):
    return int(row[0])

def sendHLR(file):
    failed = 0
    print("Reading file: "+file)
    with open(path_to_watch+file,'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        originalrows=defaultdict(dict)
        msisdns = set()
        cells = collections.OrderedDict()
        count = 0
        print "Original File:"
        for row in reader:
            originalrows[count]["original_data"]=row
            if debug: print(row,len(row[0]))
            msisdn = re.sub("[^0-9]", "",row[0])
            #reference = row[0]
            #print( ', '.join(row))
            #originalrows[count]["reference"]=reference
            originalrows[count]["msisdn"]=msisdn

            if len(msisdn) > 0:
                if msisdn not in msisdns:
                    msisdns.add(msisdn)

            else:
                if debug: print("Length of msisdn was 0 apparently")
            print "Row number"+str(count)+ "Data: "+str(originalrows[count]["original_data"])
            count += 1
        csvfile.close()
        print("Finished reading file")

        #if debug: sys.exit(0)
        batchdata = createRequest(msisdns,file)
        if batchdata == '':
            return 1
        randomuuid = uuid.uuid4().hex
        print("Outputting to: "+path_to_write+file+"-"+randomuuid+".csv")
        print("BATCHDATA: "+batchdata)

        f = open(path_to_write+file+"-"+randomuuid+".csv",'w')#,newline='')

        resultsreader = csv.reader(batchdata.splitlines(1), delimiter=',', lineterminator='\n')

        f.write(",MSISDN,Status,Error Code, Error Text, Original Network, Current Network, Current Country, Roaming Country, Type, Date Checked\r\n")
        count = 0
        first_inconclusives = 0
        resultrows = {}
        for row in resultsreader:
            try:
                errorCode = row[3]
                if errorCode == '5':
                    first_inconclusives = first_inconclusives + 1
                    resend, resend_status = resendHLR(row[0], row)
                    if resend_status == 'pass':
                        if resend['error_code'] != '5':
                            row[1] = resend['msisdn']
                            row[2] = resend['status']
                            row[3] = resend['error_code']
                            row[4] = resend['error_text']
                            row[5] = resend['original_network_name']
                            row[6] = resend['ported_network_name']
                            row[7] = resend['mccmnc']
                            row[8] = resend['original_country_name']
                            row[9] = resend['original_country_prefix']
                            row[10] = resend['ported_country_name']
                            row[11] = resend['ported_country_prefix']
                            row[12] = ''
                            row[13] = resend['roaming_country_prefix']
                            row[14] = resend['roaming_network_prefix']
                            row[15] = resend['is_roaming']
                            row[16] = resend['is_mobile']
                        else:
                            failed = failed+1
                    else:
                        failed = failed+1

                if row[3] != '5':
                    ## Here you can add some custom code if you want to translate the output for your client
                    pass
                resultrows[row[0]] = row
            except Exception as e:
                print("Row "+str(count)+" had an error: ",str(e),". Row contents:",row)
            count += 1
        outputrowcount=0
        for row in originalrows:
            f.write(originalrows[outputrowcount]["msisdn"]+",")
            msisdn = re.sub("[^0-9]", "",originalrows[outputrowcount]["msisdn"])
            if msisdn in resultrows.keys():
                matchrow = resultrows[msisdn]
                line = ','.join(([unicode(i) for i in matchrow]))
                line = line.split(",",1)[1]
                f.write(line+'\n')
                print("Row number: "+str(outputrowcount)+", msisdn: " +str(msisdn)+ ", result: "+line)
            else:
                f.write('\n')
                print("Row number: "+str(outputrowcount)+", msisdn: " +str(msisdn)+ ", result: ")
            outputrowcount += 1
        f.close()
    return first_inconclusives, failed, count, outputrowcount

"""
i = inotify.adapters.Inotify()
i.add_watch(path_to_watch)
print("watching: " + path_to_watch)




for event in i.event_gen():
    if event is not None:
        (header, type_names, watch_path, filename) = event
        if debug: print(header.wd, header.mask, header.cookie, header.len, type_names,watch_path, filename)
        if 'IN_CLOSE_WRITE' in type_names:
            print(filename," finished uploading!")
            add = filename
            if filename[-4:] == '.csv':
                print("Processing CSV: "+add)
                first_inconclusives, failed, count, outputrowcount = sendHLR(add)
                if failed == 0:
                    print("Finished processing the file and no numbers failed")
                else:
                    print("Finished processing the file, but there are "+str(failed)+" inconclusive numbers out of a total of "+str(count)+" records.")
            else:
                print("Non CSV detected, will not process this file: "+add)
"""