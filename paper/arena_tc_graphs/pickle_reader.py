import pandas as pd


def read_pickle_file(file):
    pickle_data = pd.read_pickle(file)
    return pickle_data
