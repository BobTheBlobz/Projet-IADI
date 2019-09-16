from lxml import etree

tree = etree.parse("C:/Users/quent/OneDrive/Documents/Cours/IA/ISCX_train/TestbedSatJun12Flows.xml") 
root = tree.getroot()
tab = []

for flow in root:
    dic = {}
    for element in flow:
        dic[element.tag]=element.text
    tab.append(dic)
    
print(tab[0])
    
