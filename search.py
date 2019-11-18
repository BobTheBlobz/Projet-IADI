import constants
from elasticsearch import Elasticsearch
import sys

# ----------------------------------------------
# METHODS
# ----------------------------------------------

# ----------------------------------------------
# Searches
##

def search(request = {"query":{"match_all": {}}}, index = constants.DATA_INDEX):
    """This method searches for a request in a given index. By default, returns all flows of the data index."""
    es = Elasticsearch(hosts=[constants.ES_HOST])
    try:
        results = es.search(index=index, body=request, size=constants.MAX_DISPLAY_SIZE, scroll=constants.TIMEOUT )
    except:
        print("error:", sys.exc_info()[0])
        results = []
    return [es, results]


def searchFlows(request, index = constants.DATA_INDEX):
    """This method gives only the list of raw flows resulting from a research, without the index fields"""
    [es, hits] = search(request, index)
    res = hits['hits']['hits']
    return [es, res]

def searchPrint(request = {"query":{"match_all": {}}}, index = constants.DATA_INDEX, N = 10):
    """This method searches and print first N resultsfor a request in a given index. By default, returns all flows of the data index. """
    es = Elasticsearch(hosts=[constants.ES_HOST])
    try:
        results = es.search(index=index, body=request, size=N)
    except:
        print("error:", sys.exc_info()[0])
        results = []
    print (results)
    return results

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
    results = search(request)

    return results


def getFlowsOfAppName(appName):
    """Returns all flows with a corresponding appName"""
    return getFlowsWithFieldValue('appName', appName)


def getListOfFlowByProtocol(protocol):
    """Returns all flows with a corresponding protocolName"""
    return getFlowsWithFieldValue('protocolName', protocol)
