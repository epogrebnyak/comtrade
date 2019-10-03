import numpy as np
import matplotlib.pyplot as plt

doc = """1480	145
6640	221
7561	388
9441	688
11073	915
11365	1084
12193	2124
13554	2484
14197	3205"""
xs = [line.split() for line in doc.split("\n")]

pos = np.array([0] + [int(x[0]) for x in xs]) / 1000
hs = [int(x[1]) for x in xs]

#pos = np.array([0,5,10,30])
#hs = [1,2,3]

ws = pos[1:] - pos[:-1]
plt.bar(pos[:-1], hs, width=ws, align='edge')

plt.show()

 