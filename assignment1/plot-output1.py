#!/usr/bin/python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import os

def main():
    argv = ["20221103T232402"]
    # if len(argv) < 1:
    #     print("ERROR: expecting timestamp")
    #     sys.exit(1)

    dirpath = "./experiments/"+argv[0]

    dfmean = pd.DataFrame()
    dfstd = pd.DataFrame()

    # Number of Executors [2, timefor100, timefor200, timefor500]

    for name in ['data-100MB', 'data-200MB', 'data-500MB']:
        df1 = pd.read_csv(dirpath+"/measurements/"+name+".csv")
        print(df1)
        df1.set_index('Workers', inplace=True)
        dfmean[name] = df1.mean(axis=1)
        dfstd[name] = df1.std(axis=1)
    
    print(dfstd)

    dfmean.plot(kind='bar', stacked=False, yerr = dfstd)
    plt.ylabel("Time (s)")
    plt.title("WordLetterCount Performance Summary")
    plt.show()
    plt.savefig("output1.png")

    

if __name__ == "__main__":
    main()
    # main(sys.argv[1:])