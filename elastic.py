from lxml import etree
import shelve
import glob
import os
from elasticsearch import Elasticsearch
import requests

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'projet'

TYPE_NAME = 'name'
ID_FIELD = 'key'

bulk_size = 1000
folder = "C:/Users/quent/OneDrive/Documents/Cours/IA/ISCX_train/"
    
def testServer():
    response = requests.get("http://localhost:9200/")
    return (response.status_code == 200)
            

def initIndex():
    # create ES client, create index (delete it first if it exists)
    es = Elasticsearch(hosts = [ES_HOST])
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index = INDEX_NAME, ignore=[400,404])
        print(" response: '%s'" % (res))
    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index = INDEX_NAME, body = request_body)
    print(" response: '%s'" % (res))
    return es
    

def indexing(folder):
    
    i = 0
    es = initIndex()
    
    files = glob.glob(folder+"*.xml")
    for file in files:
        print(file + " is being processed")    
        tree = etree.parse(file) 
        root = tree.getroot()
        tab = []
        
        for flow in root:
            dic = {"key" : i}
            for element in flow:
                dic[element.tag]=element.text
            tab.append(dic)
            i = i + 1
            
        print(str(i) + " flows treated")
        print(tab[0])
    

def main():
    if (testServer()):
        print("The server is running, all is good !") 
        indexing(folder)
    else:
        print("The server doesn't seems to be running, please call for help...")
        
main()