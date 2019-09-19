from qtpy.QtWidgets import QApplication, QWidget, QPushButton, QLabel, QVBoxLayout, QFileDialog
from qtpy.QtCore import Qt
from lxml import etree
import sys

  
class Accueil(QWidget):
    def __init__(self, parent=None):
        super(Accueil, self).__init__(parent)
        
        self.setWindowTitle("Ouvrir XML")
        self.resize(200,100)
        
        self.buttonOpenFile = QPushButton("Ouvrir un XML")
        self.buttonQuit = QPushButton("Quitter")
        self.text=QLabel("Que voulez vous faire ?")
        self.text.setAlignment(Qt.AlignCenter)
        
        layout = QVBoxLayout()
        layout.addWidget(self.text)
        layout.addWidget(self.buttonOpenFile)
        layout.addWidget(self.buttonQuit)
        self.setLayout(layout)
        
        self.buttonQuit.clicked.connect(self.close)
        self.buttonOpenFile.clicked.connect(self.openFile)
        
        self.nextWidget=None
        
    def openFile(self):
        fichier=QFileDialog.getOpenFileName(self, "Sélectionnez le fichier", "", "Fichier XML (*.xml)")
        if(fichier[0]!=""):
            self.nextWidget=ModifyView(fichier[0])
            self.hide()
        else:
            print("notok")
            
class ModifyView():
    def __init__(self, fichier):
        self.tree = etree.parse(fichier) 
        print("go")
        for step in self.tree.xpath("/TestbedSatJun12"):
            tag = step.find("Tag").text
            print("go2")
            if(tag == "Normal"):
                print(tag)
      

def main(argv):
    app = QApplication.instance()
    if not app:
        app = QApplication(sys.argv)    
    widget = Accueil()
    widget.show()
    app.exec_()

if __name__ == "__main__":
    main(sys.argv)