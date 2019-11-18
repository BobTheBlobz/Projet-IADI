
import search
import constants

# ----------------------------------------------
# METHODS
# ----------------------------------------------


# ----------------------------------------------
# Initialize Dictionaries
##

def buildDict(dictionary, field):
    """Builds a dictionary by listing all different fields of the flows in the database"""

    # ElasticSearch request for grouping results by the field.
    agg = {
        "aggs": {
            "group_by_" + field: {
                "terms": {"field": field + ".keyword"}
            }
        },
        "size": 0
    }

    # Getting the aggregations
    [es, hits] = search.search(agg)
    aggregation = hits["aggregations"]["group_by_" + field]["buckets"]

    # indexing each field value with a new id
    next_id = 1
    for hit in aggregation:
        dictionary[hit['key']] = next_id
        next_id += 1


def buildProtocolDict():
    """ Builds the protocols dictionary"""
    buildDict(constants.PROTOCOL_DICT, 'protocolName')
    print("Protocol Dictionary built")


def buildDirectionDict():
    """ Builds the directions dictionary"""
    buildDict(constants.DIRECTION_DICT, 'direction')
    print("Direction Dictionary built")


def buildTagDict():
    """ Builds the tags dictionary"""
    buildDict(constants.TAG_DICT, 'Tag')
    print("Tag Dictionary built")


def buildDicts():
    """ Builds the protocols, directions and tag dictionaries"""
    buildProtocolDict()
    buildDirectionDict()
    buildTagDict()
