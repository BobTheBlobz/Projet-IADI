import constants

from elasticsearch import Elasticsearch
from lxml import etree
import glob
import requests
from pathlib import Path


# ----------------------------------------------
# Initialize Indexes
##

def testServer():
    """Tests if the server is running"""
    response = requests.get("http://localhost:9200/")
    return (response.status_code == 200)


def initNewIndex(index_name):
    """Instantiate a new Elasticsearch database. It will delete it if it already exists."""

    # create ES client, create index (delete it first if it exists)
    es = Elasticsearch(hosts=[constants.ES_HOST])
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
    es = initNewIndex(constants.DATA_INDEX)
    return es


# ----------------------------------------------
# Indexing data
##

def indexingXMLData(folder = constants.XMLdirectory, index = constants.DATA_INDEX):
    """This method takes the raw XML data from the files in /XMLFiles and puts it in an elasticSearch index"""

    next_id = 0     # incrementing ID for datagrams
    bulk_incr = 0   # bulk incrementor
    bulk_data = []  # bulk memory buffer

    es = initNewIndex(index)

    files = glob.glob(Path(folder,Path("*.xml")).as_posix())
    print (Path(folder,Path("*.xml")).as_posix())

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
                        "_index": constants.DATA_INDEX,
                        "_type": constants.FLOW_TYPE_NAME,
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
            if (next_id == (bulk_incr + 1) * constants.BULK_SIZE):
                es.bulk(index=constants.DATA_INDEX, body=bulk_data, refresh=True)
                bulk_incr += 1
                print("BULK #" + str(bulk_incr) + " INDEXED")
                bulk_data.clear()

    # When we reach the EOF of the last file, the last bulk is written on disk if it's not empty
    if (next_id != (bulk_incr + 1) * constants.BULK_SIZE):
        es.bulk(index=constants.DATA_INDEX, body=bulk_data, refresh=True)
        bulk_incr += 1
        print("BULK #" + str(bulk_incr) + " INDEXED")
        bulk_data.clear()
