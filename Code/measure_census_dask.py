from pickletools import read_uint1
from random import sample
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
from clear_cache_util import clear_caches

import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "0"
csv_handler = CSVHandler(f"dask_census_run{run_id}_v2025.1.0_itr(30).csv")

import time
# import pandas as pd
import dask.dataframe as pd
import pandas as pds

def sleep():
    time.sleep(30)

# I/O functions - READ
@measure_energy(handler=csv_handler)
def load_csv(path):
    return pd.read_csv(path)

# @measure_energy(handler=csv_handler)
# def load_hdf(path, key):
#     return pd.read_hdf(path, key=key)

@measure_energy(handler=csv_handler)
def load_json(path):
    return pd.read_json(path, orient=str)

# I/O functions - WRITE
@measure_energy(handler=csv_handler)
def save_csv(df, path):
    return df.to_csv(path)

# @measure_energy(handler=csv_handler)
# def save_hdf(df, path, key):
#     return df.to_hdf(path, key=key)

@measure_energy(handler=csv_handler)
def save_json(df, path):
    return df.to_json(path)

###------------------------------------------###

# Handling missing data 
@measure_energy(handler=csv_handler)
def isna(df, cname):
    return df[cname].isna()

@measure_energy(handler=csv_handler)
def dropna(df):
    return df.dropna()

@measure_energy(handler=csv_handler)
def fillna(df, val):
    return df.fillna(val)

@measure_energy(handler=csv_handler)
def replace(df, cname, src, dest):
    return df[cname].replace(src, dest)

###------------------------------------------###

# Table operations
# drop column
# groupby
# merge 
# transpose
# sort
# concat
@measure_energy(handler=csv_handler)
def drop(df, cnameArray):
    return df.drop(columns=cnameArray)

@measure_energy(handler=csv_handler)
def groupby(df, cname):
    return df.groupby(cname)

@measure_energy(handler=csv_handler)
def merge(df1, df2, on=None):
    if(on):
        return pd.merge(df1, df2, on=on)
    else:
        return pd.merge(df1, df2)

@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort_values(by=[cname])

# def transpose(df):
#     return df.transpose()

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return pd.concat([df1, df2])

###--------------------------------------------###
# Statistical Operations
# min, max, mean, count, unique, correlation

# count 
@measure_energy(handler=csv_handler)
def count(df):
    return df.count().compute()

# sum
@measure_energy(handler=csv_handler)
def sum(df, cname):
    return df[cname].sum().compute()

# mean
@measure_energy(handler=csv_handler)
def mean(df):
    return df.mean().compute()

# min
@measure_energy(handler=csv_handler)
def min(df):
    return df.min().compute()
# max
@measure_energy(handler=csv_handler)
def max(df):
    return df.max().compute()

# unique
@measure_energy(handler=csv_handler)
def unique(df):
    return df.unique().compute()

# @measure_energy(handler=csv_handler)
# def drop_column(df, col_names=[]):
#     df.drop(columns=col_names)

# @measure_energy(handler=csv_handler)
# def remove_duplicates(df):
#     return df.drop_duplicates()

# @measure_energy(handler=csv_handler)
# def merge(df1, df2, on=None):
#     if(on):
#         return pd.merge(df1, df2, on=on)
#     else:
#         return pd.merge(df1, df2)

# @measure_energy(handler=csv_handler)
# def group_by(df, groupby):
#     pass

# # subset 
# @measure_energy(handler=csv_handler)
# def subset(df, subset):
#     return df[subset]

# # sample
# @measure_energy(handler=csv_handler)
# def sample(df, cnt=1000):
#     return df.sample(cnt)

# count, mean, min, max, value_counts, unique, sort values, groupby

print("Starting USCensus1990 Dask Process...")
for i in range(30):
    # Input output functions 
    clear_caches()
    df = load_csv(path='../datasets/USCensus1990.csv')
    sleep()
    clear_caches()
    df = load_json(path='../datasets/USCensus1990.json')
    sleep()
    clear_caches()
    # df = load_hdf(path='../datasets/USCensus1990_dask.h5', key='/data')
    # sleep()
    
    save_csv(df, f'df_USCensus1990_dask{i}.csv')
    sleep()
    save_json(df, f'df_USCensus1990_dask{i}.json')
    sleep()
    # df = df.compute()
    # for col in df.columns:
    #     if pds.api.types.is_string_dtype(df[col]):
    #         df[col] = df[col].astype(object)
    # save_hdf(df, f'df_USCensus1990_dask{i}.hdf', key='a')
    # sleep()
    clear_caches()
    # --------------------------------------------------

    # Handling missing data
    df = pd.read_csv('../datasets/USCensus1990.csv')
    sleep()
    isna(df, cname='iCitizen')
    sleep()
    dropna(df)
    sleep()
    fillna(df, val='0')
    sleep()
    replace(df, cname='iCitizen', src='0', dest='X')
    clear_caches()
    # --------------------------------------------------
    # Table operations
    df = pd.read_csv('../datasets/USCensus1990.csv')
    sleep()
    df_samp = pd.read_csv('../datasets/USCensus1990.csv')
    sleep()
    drop(df, cnameArray=['iKorean', 'iMarital'])
    sleep()
    groupby(df, cname='iCitizen')
    sleep()
    
    concat_dataframes(df, df_samp)
    sleep()
    
    sort(df, 'dAge')
    sleep()
    merge(df, df_samp)
    sleep()
    clear_caches()

    # ------------------------------------------
    # Statistical operations
    df = pd.read_csv('../datasets/USCensus1990.csv')
    sleep()
    count(df)
    sleep()
    sum(df, 'dIncome1')
    sleep()
    mean(df['dAge'])
    sleep()
    min(df['dIncome1'])
    sleep()
    max(df['dIncome1'])
    sleep()
    unique(df['dAge'])
    sleep()
    

    print(f"Finished iteration {i+1}")

print("Process complete")
csv_handler.save_data()

# df = load_csv(path='../datasets/adult.csv')
# drop_column(df, col_names=['age', 'education', 'occupation'])


# # time.sleep(2)


# remove_duplicates(df_with_dup)
# # time.sleep(2)

# # time.sleep(2)

# SUBSET = ['age', 'workclass', 'education', 'sex', 'race']
# SUBSET_A = ['occupation', 'relationship']
# subset(df, SUBSET)
# # time.sleep(2)

# sample(df, 1000)
# # time.sleep(2)
# sample(df, 10000)
# # time.sleep(2)
# sample(df, 20000)
# # time.sleep(2)

# col = ['capital-gain', 'capital.loss', 'hours.per.week']
# # col = ['capital-gain', 'capital.loss']


