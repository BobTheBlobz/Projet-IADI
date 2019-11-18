import util
import search

# ----------------------------------------------
# IA Training
##

def doTraining():
    apps = util.getAppnames(2)
    for app in apps:
        appindexname = app.lower()
        print(appindexname)
        data = search.search(appindexname)
        print(len(data['hits']['hits']))
        # for hit in data['hits']['hits']:
        # print("zizi")


def doPrediction(clf):
    print("")
