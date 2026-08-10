from pickletools import read_uint1
from random import sample
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
from clear_cache_util import clear_caches

csv_handler = CSVHandler('Vaex_drug_v4.13.0_itr(30).csv')
import time
import vaex as ve

def sleep():
    time.sleep(30)

# I/O functions - READ
@measure_energy(handler=csv_handler)
def load_csv(path):
    return ve.read_csv(path)

# @measure_energy(handler=csv_handler)
# def load_hdf(path):
#     return ve.open(path)

@measure_energy(handler=csv_handler)
def load_json(path):
    return ve.from_json(path)

# I/O functions - WRITE
@measure_energy(handler=csv_handler)
def save_csv(df, path):
    return df.export_csv(path)

# @measure_energy(handler=csv_handler)
# def save_hdf(df, path, key='a'):
#     return df.export(path)

@measure_energy(handler=csv_handler)
def save_json(df, path):
    return df.to_pandas_df().to_json(path, orient='records', lines=True)

###------------------------------------------###

# Missing data handling 
@measure_energy(handler=csv_handler)
def dropna(df):
    return df.dropna()

@measure_energy(handler=csv_handler)
def fillna(df, val):
    return df.fillna(val)

@measure_energy(handler=csv_handler)
def isna(df, cname):
    return df[cname].isna()

@measure_energy(handler=csv_handler)
def replace(df, cname, src, dest):
    return df[cname].str.replace(src, dest)

###------------------------------------------###

# Table operations
# drop column
# groupby
# merge 
# transpose
# sort


@measure_energy(handler=csv_handler)
def drop(df, col_names=[]):
    df.drop(columns=col_names)

@measure_energy(handler=csv_handler)
def groupby(df, cname):
    return df.groupby(cname)

@measure_energy(handler=csv_handler)
def merge(df1, df2, on=None, lsuffix="_l"):
    if(on == None):
        return df1.join(df2, lsuffix="_l")
    else:
        return df1.join(df2, on=on, how='inner', lsuffix=lsuffix)

@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort(cname)

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return ve.concat([df1, df2])

###--------------------------------------------###
# Statistical Operations
# min, max, mean, count, unique, correlation

# count 
@measure_energy(handler=csv_handler)
def count(df):
    return df.count()

# sum
@measure_energy(handler=csv_handler)
def sum(df, cname):
    return df.sum(cname)

# mean
@measure_energy(handler=csv_handler)
def mean(df):
    return df.mean()

# min
@measure_energy(handler=csv_handler)
def min(df):
    return df.min()

@measure_energy(handler=csv_handler)
def max(df):
    return df.max()

@measure_energy(handler=csv_handler)
def unique(df):
    return df.unique()

# # subset 
# @measure_energy(handler=csv_handler)
# def subset(df, subset):
#     return df[subset]

# @measure_energy(handler=csv_handler)
# def sample(df, cnt=1000):
#     return df.sample(cnt)


# df = load_csv('../datasets/adult.csv')
# drop_column(df, col_names=['age', 'education', 'occupation'])

# dfa = ve.from_csv('../datasets/adult.csv')
# dfb = ve.from_csv('../datasets/adult.csv')
# concat_dataframes(dfa, dfb)


# SUBSET = ['age', 'workclass', 'education', 'sex', 'race']
# SUBSET_A = ['occupation', 'relationship']
# subset(df, SUBSET)

# sample(df, 1000)
# sample(df, 10000)
# sample(df, 20000)


# col = ['capital-gain', 'capital.loss', 'hours.per.week']
# # col = ['capital-gain', 'capital.loss']
# sum(df[col])
# mean(df, 'age')
# min(df['capital-gain'])
# max(df['capital-gain'])
# unique(df['age'])
print("Starting Drug Vaex Process ...")
for i in range(30):
    # Input Output functions
    clear_caches()
    df = load_csv(path='../datasets/drugs.csv')
    sleep()
    clear_caches()
    df = load_json(path='../datasets/drugs.json')
    sleep()
    # df = load_hdf(path='../datasets/drugs_vaex.hdf5')
    # sleep()

    save_csv(df, f'df_drug_vaex_{i}.csv')
    sleep()
    save_json(df, f'df_drug_vaex_{i}.json')
    sleep()
    # save_hdf(df, f'df_drug_vaex_{i}.hdf5')
    # sleep()

    # --------------------------------------------------

    # Handling missing data
    clear_caches()
    df = ve.read_csv('../datasets/drugs.csv')
    sleep()
    isna(df, cname='review')
    sleep()
    dropna(df)
    sleep()
    fillna(df, val='0')
    sleep()
    replace(df, cname='review', src='?', dest='X')
    sleep()
    #
    # --------------------------------------------------
    # Table operations
    clear_caches()
    df = ve.read_csv('../datasets/drugs.csv')
    sleep()
    df_samp = ve.read_csv('../datasets/drugs.csv')
    sleep()
    drop(df, col_names=['drugName'])
    sleep()
    groupby(df, cname='rating')
    sleep()

    concat_dataframes(df, df_samp)
    sleep()

    sort(df, 'rating')
    sleep()
    merge(df, df_samp)
    sleep()

    # ------------------------------------------
    # Statistical operations
    clear_caches()
    df = ve.read_csv('../datasets/drugs.csv')
    sleep()
    count(df)
    sleep()
    sum(df, 'usefulCount')
    sleep()
    mean(df['rating'])
    sleep()
    min(df['usefulCount'])
    sleep()
    max(df['usefulCount'])
    sleep()
    unique(df['condition'])
    sleep()

    print(f"finished iteration {i+1}")
    clear_caches()

csv_handler.save_data()
