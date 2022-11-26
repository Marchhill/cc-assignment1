#!/usr/bin/python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

dirpath = "."
figureCount = 1

print(dirpath)
df = pd.read_csv(dirpath+"/dynamic.csv").iloc[7:20]
df.index = np.arange(1, len(df)+1)
df_time = df['Execution Time']
df_time.index = np.arange(1, len(df_time)+1)

plt.figure(figureCount)
figureCount += 1
df_time.plot(kind='line')
plt.ylabel("Execution Time (s)")
plt.xlabel("Iteration number")
plt.ylim(ymin=0)
plt.title("Dynamic Scheduler Iterations")
plt.savefig("dynamic.png")

for param in list(df)[0:3]:
    plt.figure(figureCount)
    figureCount += 1
    df_param = pd.DataFrame()
    df_param = df[param]
    print(df_param)
    df_param.plot(kind='line')
    plt.xlabel("Iteration number")
    plt.title(param)
    plt.savefig("dynamic_param_"+param+".png")
