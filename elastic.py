from lxml import etree
import sys
import glob
from elasticsearch import Elasticsearch
import requests
from pylab import np, plt

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'projet'
TYPE_NAME = 'flow'
ID_FIELD = 'key'

bulk_size = 1000
folder = "C:/Users/quent/OneDrive/Documents/Cours/IA/ISCX_train/"
    
PROTOCOL_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
DIRECTION_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
TAG_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
FLAG_DICT = { "EMPTY" : 0, "UNKNOWN" : 1 , "F" : 2, "S" : 3, "R" : 4, "P" : 5, "A" : 6, "U" : 7 }

def BuildProtocolDict():
    
    agg = {
    "aggs" : {
        "group_by_protocolName" : {
            "terms" : { "field" : "protocolName.keyword" }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_protocolName']['buckets']
    i=1
    for h in A:
        PROTOCOL_DICT[ h['key'] ] = i
        i = i+1

def BuildDirectionDict():  
    agg = {
    "aggs" : {
        "group_by_direction" : {
            "terms" : { "field" : "direction.keyword" }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_direction']['buckets']
    i=1
    for h in A:
        DIRECTION_DICT[ h['key'] ] = i
        i = i+1
        
def BuildTagDict():
    agg = {
    "aggs" : {
        "group_by_Tag" : {
            "terms" : { "field" : "Tag.keyword" }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_Tag']['buckets']
    i=1
    for h in A:
        TAG_DICT[ h['key'] ] = i
        i = i+1
        
def payloadToHistogram(payload):
    res = [0]*256
    for chr in payload:
        res[ord(chr)] += 1
    return(res)
    
def protocolToID(protocol):
    res = -1
    if protocol == '':
        res = 0
    else:    
        if (protocol in PROTOCOL_DICT):
            res = PROTOCOL_DICT[protocol]
    return(res)
    
def directionToID(direction):
    res = -1
    if direction == '':
        res = 0
    else:
        if (direction in DIRECTION_DICT):
            res = DIRECTION_DICT[direction]
    return(res)
    
def tagToID(Tag):
    res = -1
    if Tag == '':
        res = 0
    else:
        if (Tag in TAG_DICT):
            res = TAG_DICT[Tag]
    return(res)
    
def stringToNumerical(string):
    res = ""
    for chr in string:
        if chr>='0' and chr <='9':
            res = res + chr
    return(res)
    
def ip4ToVector(ip):
    res = [-1,-1,-1,-1]
    if ip == "" :
        res = [0,0,0,0]
    else:
        addr = ip.split('.')
        if len(addr) == 4:
            res[0] = int(addr[0]+addr[1]+addr[2]+addr[3])
            res[1] = int(addr[0]+addr[1]+addr[2])
            res[2] = int(addr[0]+addr[1])
            res[3] = int(addr[0])
        
    return(res)        
    
def flagsToVector(flags):
    res=[0]*len(FLAG_DICT)
    if flags == "" :
        res[FLAG_DICT['EMPTY']] = 1
    else :
        ftab = flags.split(",")
        for flag in ftab:
            if flag in FLAG_DICT:
                res[FLAG_DICT[flag]] = 1
            else:
                res[FLAG_DICT['UNKNOWN']] = 1
    return res;
    



def datagramToVector(datagram):
    res = []
    res += [int(datagram["totalSourceBytes"])]
    res += [int(datagram["totalDestinationBytes"])]
    res += [int(datagram["totalDestinationPackets"])]
    res += [int(datagram["totalSourcePackets"])]
    res += (payloadToHistogram(datagram["sourcePayloadAsBase64"]))
    res += (payloadToHistogram(datagram["destinationPayloadAsBase64"]))
    res += [(directionToID(datagram["direction"]))]
    res += (flagsToVector(datagram['sourceTCPFlagsDescription']))
    res += (flagsToVector(datagram['destinationTCPFlagsDescription']))
    res += (ip4ToVector(datagram['source']))
    res += (ip4ToVector(datagram['destination']))
    res += [int(datagram['sourcePort'])]
    res += [int(datagram['destinationPort'])]
    res += [protocolToID(datagram['protocolName'])]
    res += [int(stringToNumerical(datagram['startDateTime']))]
    res += [int(stringToNumerical(datagram['stopDateTime']))]
    return(res)
    
    




def testServer():
    response = requests.get("http://localhost:9200/")
    return (response.status_code == 200)
            

def initIndex():
    # create ES client, create index (delete it first if it exists)
    es = Elasticsearch(hosts = [ES_HOST])
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        #res = es.indices.delete(index = INDEX_NAME, ignore=[400,404])
        print(" response: '%s'" % (res))
    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    print("creating '%s' index..." % (INDEX_NAME))
    #res = es.indices.create(index = INDEX_NAME, body = request_body)
    #print(" response: '%s'" % (res))
    return es
    

def indexing(folder):
    
    i = 0
    j = 0
    
    bulk_data = []            
                    
    es = initIndex()
    
    files = glob.glob(folder+"*.xml")
    
    for file in files:
        print(file + " is being processed")    
        tree = etree.parse(file) 
        root = tree.getroot()
        print(file + " successfully parsed") 
        
        for flow in root:
            
            op_dict = {
            "index" : 
                    {
                    "_index": INDEX_NAME,
                    "_type": TYPE_NAME,
                    "_id": i
                    }
            }
            
            dic = {}
            
            for element in flow:
                dic[element.tag]=element.text
            
            bulk_data.append(op_dict)
            bulk_data.append(dic)
            i = i + 1
            
            if (i == (j+1)*bulk_size ):
                es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
                j += 1
                print("BULK #"+ str(j)+" INDEXED")
                bulk_data.clear()
                
    if (i != (j+1)*bulk_size ):
        es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
        j += 1
        print("BULK #"+ str(j)+" INDEXED")
        bulk_data.clear()
                                
def search(bdy, s):
    es = Elasticsearch(hosts = [ES_HOST])
    try:
        hits=es.search(index=INDEX_NAME, body=bdy, size=s)                      
    except:
        print("error:", sys.exc_info()[0])
        hits=[]
    return hits

def searchBody(body, size):
    hits = search(body, size)
    H=hits['hits']['hits']
    print ("# hits: ", len(H))
    for h in H:
        print(h)
        
        
    

def groupByAppName(n):
    
    agg = {
    "aggs" : {
        "group_by_appName" : {
            "terms" : { "field" : "appName.keyword", "size":n }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_appName']['buckets']
    print("# aggs: ", len(A))
    for h in A:
        print('>> ',h['key'], " ", h['doc_count'])
        
def groupByProtocolName(n):
    
    agg = {
    "aggs" : {
        "group_by_protocolName" : {
            "terms" : { "field" : "protocolName.keyword", "size":n }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_protocolName']['buckets']
    print("# aggs: ", len(A))
    for h in A:
        print('>> ',h['key'], " ", h['doc_count'])
        
def groupByTCP(n):
    
    agg = {
    "aggs" : {
        "group_by_TCP" : {
            "terms" : { "field" : "destinationTCPFlagsDescription.keyword", "size":n }
            }
        },
        "size" : 0
    } 
        
    hits = search(agg, 0)
    H=hits['aggregations']
    A=H['group_by_TCP']['buckets']
    print("# aggs: ", len(A))
    for h in A:
        print('>> ',h['key'], " ", h['doc_count'])

def getListOfAppName(appName, size):
    
    body_must={
        "query":{
                "bool":{
                        "must":{
                                "match":{
                                        "appName": appName}
                                }
                        }
                }
        }
    return searchBody(body_must, size)

def getListOfFlowByProtocol(protocol, size):
    
    body_must={
        "query":{
                "bool":{
                        "must":{
                                "match":{
                                        "protocolName": protocol}
                                }
                        }
                }
        }
    return searchBody(body_must, size)

def getSourcePayloadSize(hit):
    print(">> #", hit['_id'], " : ", hit['_source']['sourcePayloadAsBase64'], "Bytes")
    return hit['_source']['sourcePayloadAsBase64']
    
def getDestinationPayloadSize(hit):
    print(">> #", hit['_id'], " : ", hit['_source']['destinationPayloadAsBase64'], "Bytes")
    return hit['_source']['destinationPayloadAsBase64']

def getSourceBytesSize(hit):
    print(">> #", hit['_id'], " : ", hit['_source']['totalSourceBytes'], "Bytes")
    return int(hit['_source']['totalSourceBytes'])

def getDestinationBytesSize(hit):
    print(">> #", hit['_id'], " : ", hit['_source']['totalDestinationBytes'], "Bytes")
    return int(hit['_source']['totalDestinationBytes'])

def getSourcePacketsNumber(hit):
    #print(">> #", hit['_id'], " : ", hit['_source']['totalSourcePackets'], "Packets")
    return int(hit['_source']['totalSourcePackets'])

def getDestinationPacketsNumber(hit):
    #print(">> #", hit['_id'], " : ", hit['_source']['totalDestinationPackets'], "Packets")
    return int(hit['_source']['totalDestinationPackets'])

def getPacketsNumber(hit):
    return getDestinationPacketsNumber(hit) + getSourcePacketsNumber(hit)

def getGraph(body, size):
    hits = search(body, size)
    H=hits['hits']['hits']
    tab = 1000000*[0]
    for h in H:
        tab[int(getPacketsNumber(h))] = tab[int(getPacketsNumber(h))] + 1
        #☻print(str(getPacketsNumber(h)))
    X = np.linspace(0,1000000,1000000,endpoint=True)
    print (tab[2])
    fig=plt.figure()
    ax = fig.add_subplot()
    ax.plot(X,tab)
    ax.set_yscale('log')
    ax.set_xscale('log')
    
    
def main():
    if (testServer()):
        print("The server is running, all is good !") 
        indexing(folder)
    else:
        print("The server doesn't seems to be running, please call for help...")
                                
body_all={
        "query":
            {
                "match_all":{}
            }
        }
    
def main2():
    print("Le pr0gramm3 de r0b1n")
    BuildProtocolDict()
    BuildDirectionDict()
    BuildTagDict()
    print(DIRECTION_DICT)
    print(PROTOCOL_DICT)
    print(TAG_DICT)
    print (datagramToVector({'appName': 'HTTPWeb', 'totalSourceBytes': '378', 'totalDestinationBytes': '513', 'totalDestinationPackets': '5', 'totalSourcePackets': '5', 'sourcePayloadAsBase64': 'R0VUIC9pbWcvYnRuX2J1eS5wbmcgSFRUUC8xLjENCkhvc3Q6IHRyYWNrLmFmZmlsaWF0ZS1iLmNvbQ0KDQo=', 'sourcePayloadAsUTF': 'GET /img/btn_buy.png HTTP/1.1Host: track.affiliate-b.com', 'destinationPayloadAsBase64': 'SFRUUC8xLjEgNDA0IE5vdCBGb3VuZA0KRGF0ZTogU2F0LCAxMiBKdW4gMjAxMCAxNjowNTo1NyBHTVQNClNlcg==', 'destinationPayloadAsUTF': 'HTTP/1.1 404 Not FoundDate: Sat, 12 Jun 2010 16:05:57 GMTSer', 'direction': 'L2R', 'sourceTCPFlagsDescription': 'F,S,P,A', 'destinationTCPFlagsDescription': 'F,S,P,A', 'source': '192.168.2.106', 'protocolName': 'tcp_ip', 'sourcePort': '4625', 'destination': '202.210.143.140', 'destinationPort': '80', 'startDateTime': '2010-06-12T13:05:15', 'stopDateTime': '2010-06-12T13:05:15', 'Tag': 'Normal'}))
    
    #groupByAppName(50)
    #groupByTCP(50)
    #groupByProtocolName(50)
    #searchBody(body_all, 1)
    #getListOfFlowByProtocol('udp_ip', 1)
    
    #getListOfAppName('Unknown_UDP', 1000)
    #getGraph(body_all, 10000)
        
main2()