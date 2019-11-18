
import constants
import search
import util
import indexing

from elasticsearch import Elasticsearch
import pickle

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
    return fieldToID(constants.PROTOCOL_DICT, protocol)


def directionToID(direction):
    """Takes a direction and returns the ID value from the dictionary"""
    return fieldToID(constants.DIRECTION_DICT, direction)


def tagToID(Tag):
    """Takes a tag and returns the ID value from the dictionary"""
    return fieldToID(constants.TAG_DICT, Tag)


def payloadToHistogram(payload):
    """Takes an ascii string and makes an histogram out of it"""
    res = [0] * 256
    if payload != None:
        for chr in payload:
            # The array field corresponding to the character is incremented
            res[ord(chr)] += 1
    return (res)


def stringToNumerical(string):
    """Takes a string and delete all characters except numericals"""
    res = ""
    for chr in string:
        if chr >= '0' and chr <= '9':
            res = res + chr
    return (res)


def ip4ToVector(ip):
    """Takes an IP from string format and builds 4 numbers from it"""
    res = [-1, -1, -1, -1]
    if ip == "":
        res = [0, 0, 0, 0]
    else:
        addr = ip.split('.')
        if len(addr) == 4:
            res[0] = int(addr[0] + addr[1] + addr[2] + addr[3])  # Concatenation of all 4 bytes
            res[1] = int(addr[0] + addr[1] + addr[2])  # Concatenation of first 3 bytes
            res[2] = int(addr[0] + addr[1])  # Concatenation of first 2 bytes
            res[3] = int(addr[0])  # first byte
    return (res)


def flagsToVector(flags):
    """Takes an string of all flags and makes an histogram from the dictionary"""
    res = [0] * len(constants.FLAG_DICT)
    # Check if the flags fields are empty
    if flags == "" or flags == None:
        res[constants.FLAG_DICT['EMPTY']] = 1
    else:
        ftab = flags.split(",")
        for flag in ftab:
            if flag in constants.FLAG_DICT:
                # Put a 1 in the corresponding field
                res[constants.FLAG_DICT[flag]] = 1
            else:
                # Put a 1 in the unknown flag field
                res[constants.FLAG_DICT['UNKNOWN']] = 1
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
    return (res)


# ----------------------------------------------
# Building vectors index
##

def saveVectorsByAppname(topAppNames):
    """Writes on disk the vectors of all flaws from the biggest N appNames in files named by the appNames.
    Prefer using the saveVectorsByAppnameES instead."""
    apps = util.getAppnames(topAppNames)
    for app in apps:
        print(app)
        filename = constants.SavesDirectory + app
        file = open(filename, 'wb+')
        [es, data] = search.getFlowsOfAppName(app)
        vector = []

        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        vector = []

        # Scrolling through the results and adding them to the file
        while scroll_size > 0:
            for hit in data['hits']['hits']:
                vector += [datagramToVector(hit["_source"])]

            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

        # Returning the list of results

        pickle.dump(vector, file)
        file.close()


def saveVectorsByAppnameES(topAppNames):
    """Writes on ElasticSearch the vectors of all flaws from the biggest N appNames in files named by the appNames"""
    es = Elasticsearch(hosts=[constants.ES_HOST])
    apps = util.getAppnames(topAppNames)
    for app in apps:

        print(">> Indexing " + app + " vectors.")
        appIndexName = app.lower()

        # Building a new index for the appName
        es_vector = indexing.initNewIndex(appIndexName)

        [scrollES, data] = util.getFlowsOfAppName(app)

        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        next_id = 0  # incrementing ID for datagrams
        bulk_incr = 0  # bulk incrementor
        bulk_data = []  # bulk memory buffer

        while scroll_size > 0:
            for hit in data['hits']['hits']:

                op_dict = {
                    "index":
                        {
                            "_index": appIndexName,
                            "_type": constants.VECTOR_TYPE_NAME,
                            "_id": hit["_id"]  # We keep the same ID thant the flow to be able to find it later
                        }
                }

                dic = {"vector": datagramToVector(hit["_source"]), "tag": (tagToID(hit["_source"]['Tag']))}

                bulk_data.append(op_dict)
                bulk_data.append(dic)
                next_id = next_id + 1

                if next_id == (bulk_incr + 1) * constants.BULK_SIZE:
                    es_vector.bulk(index=constants.DATA_INDEX, body=bulk_data, refresh=True)
                    bulk_incr += 1
                    print("BULK #" + str(bulk_incr) + " INDEXED")
                    bulk_data.clear()

            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

        if next_id != (bulk_incr + 1) * constants.BULK_SIZE and len(bulk_data) != 0:
            es_vector.bulk(index=constants.DATA_INDEX, body=bulk_data, refresh=True)
            bulk_incr += 1
            print("BULK #" + str(bulk_incr) + " INDEXED")
            bulk_data.clear()

