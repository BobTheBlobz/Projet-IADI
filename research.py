
ES_HOST = {"host" : "localhost", "port" : 9200}

ALL_FLOWS = {
    "query":
        {
            "match_all": {}
        }
}



def search(body = ALL_FLOWS, function = None, args = []):

    es = Elasticsearch(hosts = [ES_HOST])
    try:
        data = es.search(index=DATA_INDEX, body=body, size=size, scroll='2m')
    except:
        print("error:", sys.exc_info()[0])
        data=[]

    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    data_processed = []

    while scroll_size > 0:
        for hit in data['hits']['hits']:
            if function != None:
                data_processed += function(hit,args)
            else:
                data_processed += hit

        data = es.scroll(scroll_id=sid, scroll='2m')
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])


def getFlowsOfAppName(appName, size):
    body_must = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "appName": appName}
                }
            }
        }
    }
    return search(body_must, size)



def groupByAppName(n):
    agg = {
        "aggs": {
            "group_by_appName": {
                "terms": {"field": "appName.keyword", "size": n}
            }
        },
        "size": 0
    }

    hits = search(agg)
    H = hits['aggregations']
    A = H['group_by_appName']['buckets']
    print("# aggs: ", len(A))
    for h in A:
        print('>> ', h['key'], " ", h['doc_count'])
    return A



def getAppnames(n):
    hits = groupByAppName(n)
    res = []
    for hit in hits:
        res += [hit['key']]
    return res



def saveVectorsByAppnameWithScrollAndElasticSearchThisMagnificientTool():
    es = Elasticsearch(hosts=[ES_HOST])

    apps = getAppnames(3)

    for app in apps:
        print(app)
        appindexname = app.lower()
        es_vector = initNewIndex(appindexname)

        data = getFlowsOfAppNameWithScroll(app)
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        bulk_data = []
        i = 0
        j = 0
        while scroll_size > 0:
            for hit in data['hits']['hits']:

                op_dict = {
                    "index":
                        {
                            "_index": appindexname,
                            "_type": TYPE_NAME,
                            "_id": hit["_id"]
                        }
                }

                dic = {"vector": datagramToVector(hit["_source"]), "tag": findTagOfDatagram(hit["_source"])}

                bulk_data.append(op_dict)
                bulk_data.append(dic)
                i = i + 1

                if (i == (j + 1) * bulk_size):
                    es_vector.bulk(index=DATA_INDEX, body=bulk_data, refresh=True)
                    j += 1
                    print("BULK #" + str(j) + " INDEXED")
                    bulk_data.clear()

            data = es.scroll(scroll_id=sid, scroll='2m')
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

            #if (i != (j + 1) * bulk_size and len(bulk_data) != 0):
            #    es_vector.bulk(index=DATA_INDEX, body=bulk_data, refresh=True)
            #    j += 1
            #    print("BULK #" + str(j) + " INDEXED")
            #    bulk_data.clear()

        if (i != (j + 1) * bulk_size and len(bulk_data) != 0):
            es_vector.bulk(index=DATA_INDEX, body=bulk_data, refresh=True)
            j += 1
            print("BULK #" + str(j) + " INDEXED")
            bulk_data.clear()
