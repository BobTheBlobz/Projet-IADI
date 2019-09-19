from lxml import etree
import shelve
import glob
import os

folder = "C:/Users/quent/OneDrive/Documents/Cours/IA/ISCX_train/"
files = glob.glob(folder+"*.xml")

for file in files:

    print(file + " is being processed")
    
    tree = etree.parse(file) 
    root = tree.getroot()
    tab = []
    
    for flow in root:
        dic = {}
        for element in flow:
            dic[element.tag]=element.text
        tab.append(dic)
        
    d = shelve.open("data")
    d[os.path.basename(file)]=tab
    d.close()

