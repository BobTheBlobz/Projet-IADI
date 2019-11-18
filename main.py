from pylab import np, plt

import search
import constants
import util
import dictionaries
import indexing
import vectoring


# ----------------------------------------------
# METHODS
# ----------------------------------------------


# ----------------------------------------------
# Draw Graph
##
def getGraph(body):
    [es, data] = search.search(body)
    H = data['hits']['hits']
    tab = 1000000 * [0]
    for h in H:
        tab[int(search.getPacketsNumber(h))] = tab[int(search.getPacketsNumber(h))] + 1
        # ☻print(str(getPacketsNumber(h)))
    X = np.linspace(0, 1000000, 1000000, endpoint=True)
    print(tab[2])
    fig = plt.figure()
    ax = fig.add_subplot()
    ax.plot(X, tab)
    ax.set_yscale('log')
    ax.set_xscale('log')


def main():
    print("############################################################")
    print("# Starting AI & ID Program                                 #")
    print("# By R.ASPE, Q.FLEGEAU, under the direction of F.MARTEAU   #")
    print("############################################################")
    print()
    if indexing.testServer():
        print("The server is running, all is good !")
        # indexingXMLData()
        dictionaries.buildDicts()
        vectoring.saveVectorsByAppnameES(1)

    else:
        print("The server doesn't seems to be running correctly")


def testing():
    print("Le pr0gramm3 de r0b1n")
    dictionaries.buildDicts()
    print("direction:" ,constants.DIRECTION_DICT)
    print(util.getAppnames(1))
    search.searchPrint({"query": {"match_all": {}}}, 'httpweb', 1)


testing()