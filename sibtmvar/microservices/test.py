import time
import pandas as pd
from multiprocessing import Pool
import numpy as np
import multiprocessing

def worker(i):
    t = time.time()
    time.sleep(0.5)  # simulate processing something
    d = {
        "name": "Player {}".format(i + 1),
        "points": (i + 1) ** 2.0
    }
    print("Iteration {} took {} seconds".format(i + 1, time.time() - t))
    return d

def highlight_part(d):

    # Check if variant in the query
    #
    for index, row in d.iterrows():
        # Check if highlight is present
        time.sleep(0.5)
        row['test'] = "abcde"
        print(row)


    return d

def main():  # absolutely all execution code must be protected when messing with multiprocessing
    # each subprocess WILL execute everything again if not contained in 'main'

    documents_df = pd.DataFrame(np.random.randint(0, 100, size=(10, 5)), columns=list('ABCDE'))
    print(documents_df)

    # parallel way
    print("The old way...")
    t0 = time.time()

    for index, row in documents_df.iterrows():
        # Check if highlight is present
        time.sleep(0.5)

        row['test'] = "abcde"


    print(len(documents_df))
    print("Total execution took {} seconds".format(time.time() - t0))

    # parallel way
    print("The parallel way...")
    t0 = time.time()

    # create as many processes as there are CPUs on your machine
    num_processes = multiprocessing.cpu_count()

    data_split = np.array_split(documents_df, num_processes)

    process_pool = Pool(processes=num_processes)
    dfs = process_pool.map(highlight_part, data_split)
    # Concat dataframes to one dataframe
    data = pd.concat(dfs)


    print(data)
    print("Total execution took {} seconds".format(time.time() - t0))


if __name__ == "__main__":
    main()