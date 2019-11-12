from typing import Any

from lxml import etree
import sys
import glob
from elasticsearch import Elasticsearch
import requests
import pickle
from pylab import np, plt
from pathlib import Path
#from sklearn.neural_network import MLPClassifier

# ----------------------------------------------
# CONSTANTS
# ----------------------------------------------

# The ElasticSearch host coordinates. In our case, it's the local machine.
ES_HOST = {"host" : "localhost", "port" : 9200}

# The index name for the raw xml data. Beware, changing it will rewrite all data on the disk!
DATA_INDEX = 'projet'

# The index name for the processed vectorized data. Beware, changing it will rewrite all data on the disk!
VECTORS_INDEX = 'vectors'

# ???
TYPE_NAME = 'flow'

# ???
ID_FIELD = 'key'

# The maximum Bulk Size. Bulks are chunks of data that allows faster searches. No need to change it.
BULK_SIZE = 1000

# The maximum results displayed for a single research.
MAX_DISPLAY_SIZE = 10000

# The timeout in minutes after which the search sends out an exception.
TIMEOUT = '2m'

# The working directory of the program. The directory containing the XML files must be names 'XMLFiles'.
XMLdirectory = Path(Path().absolute(),Path("XMLFiles"))
print("Directory Path:", Path().absolute())
print("XML Files Path:", XMLdirectory)



# DICTIONNARIES: Generated when launching the program. Used for vectorisation.
PROTOCOL_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
DIRECTION_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
TAG_DICT = { "EMPTY" : 0, "UNKNOWN" : -1 }
FLAG_DICT = { "EMPTY" : 0, "UNKNOWN" : 1 , "F" : 2, "S" : 3, "R" : 4, "P" : 5, "A" : 6, "U" : 7 }


# ----------------------------------------------
# METHODS
# ----------------------------------------------

def testServer():
    """Tests if the server is running"""
    response = requests.get("http://localhost:9200/")
    return (response.status_code == 200)

# ----------------------------------------------
# Initialize Indexes
##

def initNewIndex(index_name):
    """Instantiate a new Elasticsearch database. It will delete it if it already exists."""

    # create ES client, create index (delete it first if it exists)
    es = Elasticsearch(hosts=[ES_HOST])
    if es.indices.exists(index_name):
        print("deleting '%s' index..." % (index_name))
        res = es.indices.delete(index=index_name, ignore=[400, 404])
        print(" response: '%s'" % (res))

    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    print("creating '%s' index..." % (index_name))
    res = es.indices.create(index=index_name, body=request_body)
    print(" response: '%s'" % (res))
    return es

def initDataIndex():
    """Instantiate a new Elasticsearch standard database for the raw XML data."""
    es = initNewIndex(DATA_INDEX)
    return es


# ----------------------------------------------
# Indexing data
##

def indexingXMLData(folder = XMLdirectory):
    """This method takes the raw XML data from the files in /XMLFiles and puts it in an elasticSearch index"""

    next_id = 0     # incrementing ID for datagrams
    bulk_incr = 0   # bulk incrementor
    bulk_data = []  # bulk memory buffer

    es = initDataIndex()

    files = glob.glob(folder + "*.xml")
    print("These files will be parsed and added to the index :" , files)

    for file in files:
        print(file + " is being processed")
        tree = etree.parse(file)
        root = tree.getroot()
        print(file + " successfully parsed")

        for flow in root:

            # Each flow is indexed in two parts: a header (ID, Database name, and Type) and the body of the flow
            header = {
                "index":
                    {
                        "_index": DATA_INDEX,
                        "_type": TYPE_NAME,
                        "_id": next_id
                    }
            }
            # The body is a transcript of the XML data, stored in a dictionary
            body = {}
            for element in flow:
                body[element.tag] = element.text

            # header and body are added to a bulk buffer
            bulk_data.append(header)
            bulk_data.append(body)
            next_id += 1

            # When the bulk buffer is full, it is written on disk and cleared
            if (next_id == (bulk_incr + 1) * bulk_size):
                es.bulk(index=DATA_INDEX, body=bulk_data, refresh=True)
                bulk_incr += 1
                print("BULK #" + str(bulk_incr) + " INDEXED")
                bulk_data.clear()

    # When we reach the EOF of the last file, the last bulk is written on disk if it's not empty
    if (next_id != (bulk_incr + 1) * bulk_size):
        es.bulk(index=DATA_INDEX, body=bulk_data, refresh=True)
        bulk_incr += 1
        print("BULK #" + str(bulk_incr) + " INDEXED")
        bulk_data.clear()


# ----------------------------------------------
# Initialize Dictionaries
##

def buildDict(dictionary, field):
    """Builds a dictionary by listing all different fields of the flows in the database"""

    # ElasticSearch request for grouping results by the field.
    agg = {
        "aggs": {
            "group_by_" + field : {
                "terms": {"field": field + ".keyword"}
                }
            },
        "size": 0
    }

    # Getting the aggregations
    hits = search(agg)
    print(hits)
    aggregation = hits['aggregations']['group_by_'+field]['buckets']

    # indexing each field value with a new id
    next_id = 1
    for hit in aggregation:
        dictionary[hit['key']] = next_id
        next_id += 1


def buildProtocolDict():
    """ Builds the protocols dictionary"""
    buildDict(PROTOCOL_DICT, 'protocolName')


def buildDirectionDict():
    """ Builds the directions dictionary"""
    buildDict(DIRECTION_DICT, 'direction')
     
        
def BuildTagDict():
    """ Builds the tags dictionary"""
    buildDict(TAG_DICT, 'Tag')


# ----------------------------------------------
# Searches
##

def search(request = {"query":{"match_all": {}}}, index = DATA_INDEX):
    """This method searches for a request in a given index. By default, returns all flows of the data index."""
    es = Elasticsearch(hosts=[ES_HOST])
    try:
        results = es.search(index=index, body=request, size=MAX_DISPLAY_SIZE, scroll=TIMEOUT )
    except:
        print("error:", sys.exc_info()[0])
        results = []
    return [es, results]


def searchFlows(request, index = DATA_INDEX):
    """This method gives only the list of raw flows resulting from a research, without the index fields"""
    [es, hits] = search(request, index)
    res = hits['hits']['hits']
    return [es, res]


def groupByField(field, topnumber):
    """Gives the number of flows by FIELD value, for the TOPNUMBER biggest"""
    agg = {
        "aggs": {
            "group_by_"+field: {
                "terms": {"field": field+".keyword", "size": topnumber}
            }
        },
        "size": 0
    }

    # Get the result of the research and scroll through all results to get all the results
    [es, data] = search(agg)
    hits = data['aggregations']

    # Take the grouped appnames
    aggregation = hits['group_by_appName']['buckets']
    print("# aggs: ", len(aggregation))

    # Print the results
    for hit in aggregation:
        print('>> ', hit['key'], " ", hit['doc_count'])

    return aggregation


def groupByAppName(topnumber):
    """Gives the number of flows by appName, for the TOPNUMBER biggest"""
    return groupByField('appName', topnumber)


def groupByProtocolName(topnumber):
    """Gives the number of flows by protocol Name, for the TOPNUMBER biggest"""
    return groupByField('protocolName', topnumber)


def groupByTCP(topnumber):
    """Gives the number of flows by TCPFlags Name, for the TOPNUMBER biggest"""
    return groupByField('destinationTCPFlagsDescription', topnumber)


def getFlowsWithFieldValue(field, value):
    """Returns all flows with a corresponding value for a certain field"""
    request = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        field : value}
                }
            }
        }
    }
    # Taking the results
    [es, data] = search(request)
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    results = []

    # Scrolling through the results and adding them to a list
    while(scroll_size > 0):
        results += data['hits']['hits']
        data = es.scroll(scroll_id=sid, scroll='2m')
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

    # Returning the list of results
    return results


def getFlowsOfAppName(appName):
    """Returns all flows with a corresponding appName"""
    return getFlowsWithFieldValue('appName', appName)


def getListOfFlowByProtocol(protocol):
    """Returns all flows with a corresponding protocolName"""
    return getFlowsWithFieldValue('protocolName', protocol)

# ----------------------------------------------
# Get Information (utilitaries)
##

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
    # print(">> #", hit['_id'], " : ", hit['_source']['totalSourcePackets'], "Packets")
    return int(hit['_source']['totalSourcePackets'])


def getDestinationPacketsNumber(hit):
    # print(">> #", hit['_id'], " : ", hit['_source']['totalDestinationPackets'], "Packets")
    return int(hit['_source']['totalDestinationPackets'])


def getPacketsNumber(hit):
    return getDestinationPacketsNumber(hit) + getSourcePacketsNumber(hit)


def getAppnames(topresults):
    hits = groupByAppName(topresults)
    res = []
    for hit in hits:
        res += [hit['key']]
    return res


# ----------------------------------------------
# Draw Graph
##
def getGraph(body):
    [es, results] = search(body)
    H=hits['hits']['hits']
    tab = 1000000*[0]
    for h in H:
        tab[int(getPacketsNumber(h))] = tab[int(getPacketsNumber(h))] + 1
        #☻print(str(getPacketsNumber(h)))
    X = np.linspace(0,1000000,1000000,endpoint=True)
    print(tab[2])
    fig=plt.figure()
    ax = fig.add_subplot()
    ax.plot(X,tab)
    ax.set_yscale('log')
    ax.set_xscale('log')


# ----------------------------------------------
# Vectorisation
##

def fieldToID(dictionary, value):
    """Takes a field value and returns the ID value from a dictionary"""
    # UNKNOWN value at first
    res = dictionary['UNKNOWN']

    # Test if protocol field is empty
    if value == '':
        res = dictionary['EMPTY']

    # Find the corresponding ID in the dictionary
    else:
        if (value in dictionary):
            res = dictionary[value]
    # Returns the final ID
    return (res)


def protocolToID(protocol):
    """Takes a protocol name and returns the ID value from the dictionary"""
    return fieldToID(PROTOCOL_DICT, protocol)


def directionToID(direction):
    """Takes a direction and returns the ID value from the dictionary"""
    return fieldToID(DIRECTION_DICT, direction)


def tagToID(Tag):
    """Takes a tag and returns the ID value from the dictionary"""
    return fieldToID(TAG_DICT, Tag)


def payloadToHistogram(payload):
    """Takes an ascii string and makes an histogram out of it"""
    res = [0]*256
    if payload != None :
        for chr in payload:
            # The array field corresponding to the character is incremented
            res[ord(chr)] += 1
    return(res)
    

def stringToNumerical(string):
    """Takes a string and delete all characters except numericals"""
    res = ""
    for chr in string:
        if chr>='0' and chr <='9':
            res = res + chr
    return(res)
    
    
def ip4ToVector(ip):
    """Takes an IP from string format and builds 4 numbers from it"""
    res = [-1,-1,-1,-1]
    if ip == "" :
        res = [0,0,0,0]
    else:
        addr = ip.split('.')
        if len(addr) == 4:
            res[0] = int(addr[0]+addr[1]+addr[2]+addr[3])   # Concatenation of all 4 bytes
            res[1] = int(addr[0]+addr[1]+addr[2])           # Concatenation of first 3 bytes
            res[2] = int(addr[0]+addr[1])                   # Concatenation of first 2 bytes
            res[3] = int(addr[0])                           # first byte
    return(res)        


def flagsToVector(flags):
    """Takes an string of all flags and makes an histogram from the dictionary"""
    res=[0]*len(FLAG_DICT)
    # Check if the flags fields are empty
    if flags == "" or flags == None:
        res[FLAG_DICT['EMPTY']] = 1
    else :
        ftab = flags.split(",")
        for flag in ftab:
            if flag in FLAG_DICT:
                # Put a 1 in the corresponding field
                res[FLAG_DICT[flag]] = 1
            else:
                # Put a 1 in the unknown flag field
                res[FLAG_DICT['UNKNOWN']] = 1
    return res;


def datagramToVector(datagram):
    """Takes a datagram and uses utilitaries on each field to transform it into an integer vector"""
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





def saveVectorsByAppname(n):
    apps = getAppnames(n)
    for app in apps:
        print(app)
        filename = "C:/Users/quent/OneDrive/Documents/Cours/IA/vectors/"+app
        file = open(filename,'wb+')
        hits = getFlowsOfAppName(app, n)
        vector = []
        for hit in hits:
            vector += [datagramToVector(hit["_source"])]
        pickle.dump(vector, file)
        file.close()
     
        
def saveVectorsByAppnameWithScroll(n):
    es = Elasticsearch(hosts = [ES_HOST])
    apps = getAppnames(n)
    for app in apps:
        print(app)
        filename = "C:/Users/quent/OneDrive/Documents/Cours/IA/vectors/"+app
        file = open(filename,'wb+')
        data = getFlowsOfAppNameWithScroll(app, n)
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        vector = []
        i=0
        while scroll_size > 0:
            for hit in data['hits']['hits']:
                vector += [datagramToVector(hit["_source"])]
            i+= scroll_size
            print(i)
            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])
        pickle.dump(vector, file)
        file.close()
        
        
def saveVectorsByAppnameWithScrollAndElasticSearchThisMagnificientTool():
    es = Elasticsearch(hosts = [ES_HOST])
    apps = getAppnames(3)
    for app in apps:
        print(app)
        appindexname=app.lower()
        es_vector = initNewIndex(appindexname)
        
        data = getFlowsOfAppNameWithScroll(app)
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        bulk_data = []
        i=0
        j=0
        while scroll_size > 0:
            for hit in data['hits']['hits']:
                
                op_dict = {
                "index" : 
                        {
                        "_index": appindexname,
                        "_type": TYPE_NAME,
                        "_id": hit["_id"]
                        }
                }
                
                dic = { "vector" : datagramToVector(hit["_source"]), "tag" : findTagOfDatagram(hit["_source"])}
                
                bulk_data.append(op_dict)
                bulk_data.append(dic)
                i = i + 1
            
                if (i == (j+1)*bulk_size ):
                    es_vector.bulk(index = DATA_INDEX, body = bulk_data, refresh = True)
                    j += 1
                    print("BULK #"+ str(j)+" INDEXED")
                    bulk_data.clear()
                    
            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])
                
            if (i != (j+1)*bulk_size and len(bulk_data) != 0):
                es_vector.bulk(index = DATA_INDEX, body = bulk_data, refresh = True)
                j += 1
                print("BULK #"+ str(j)+" INDEXED")
                bulk_data.clear()
                
        if (i != (j+1)*bulk_size and len(bulk_data) != 0):
            es_vector.bulk(index = DATA_INDEX, body = bulk_data, refresh = True)
            j += 1
            print("BULK #"+ str(j)+" INDEXED")
            bulk_data.clear()
            
def doTraining():
    apps = getAppnames(2)
    for app in apps:
        appindexname=app.lower()
        print(appindexname)
        data = searchAll(appindexname)
        print (len(data['hits']['hits']))
        #for hit in data['hits']['hits']:
            #print("zizi")
        
    
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
    buildDict(PROTOCOL_DICT, 'protocolName')

    #groupByAppName(50)
    #saveVectorsByAppnameWithScrollAndElasticSearchThisMagnificientTool()
    
    doTraining()

    #groupByTCP(50)
    #groupByProtocolName(50)
    #searchBody(body_all, 1)
    #getListOfFlowByProtocol('udp_ip', 1)
    
    #getFlowsOfAppNameWithScroll('Unknown_UDP', 500)
    #getGraph(body_all, 10000)
        
main2()