import glob
import os

a = glob.glob("C:/Users/quent/OneDrive/Documents/Cours/IA/ISCX_train/*.xml")
print(os.path.basename(a[0]))