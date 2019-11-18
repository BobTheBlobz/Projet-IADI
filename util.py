import constants
import search

# ----------------------------------------------
# METHODS
# ----------------------------------------------


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
    hits = search.groupByAppName(topresults)
    res = []
    for hit in hits:
        res += [hit['key']]
    return res

