from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler

import time
import vaex as ve
from clear_cache_util import clear_caches

csv_handler = CSVHandler('Vaex_water_v4.13.0_itr(30).csv')

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
    return ve.from_json(path, lines=True)

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

# Handling missing data 
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

# Table operations
@measure_energy(handler=csv_handler)
def drop(df, col_names=[]):
    df.drop(columns=col_names)

@measure_energy(handler=csv_handler)
def groupby(df, cname):
    return df.groupby(cname)

@measure_energy(handler=csv_handler)
def merge(df1, df2, on=None, lsuffix="_l"):
    if(on == None):
        return df1.join(df2, lsuffix=lsuffix)
    else:
        return df1.join(df2, on=on, how='inner', lsuffix=lsuffix)

@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort(cname)

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return ve.concat([df1, df2])

# Statistical operations
@measure_energy(handler=csv_handler)
def count(df):
    return df.count()

@measure_energy(handler=csv_handler)
def sum(df, cname):
    return df.sum(cname)

@measure_energy(handler=csv_handler)
def mean(df):
    return df.mean()

@measure_energy(handler=csv_handler)
def min(df):
    return df.min()

@measure_energy(handler=csv_handler)
def max(df):
    return df.max()

@measure_energy(handler=csv_handler)
def unique(df):
    return df.unique()

print("Starting Water Potability Vaex Process...")
for i in range(30):
    # I/O
    clear_caches()
    df = load_csv(path='../datasets/water_potability.csv')
    sleep()
    clear_caches()
    df = load_json(path='../datasets/water_potability.json')
    sleep()
    # df = load_hdf(path='../datasets/water_potability_vaex.hdf5')
    # sleep()

    save_csv(df, f'df_water_vaex_{i}.csv')
    sleep()
    save_json(df, f'df_water_vaex_{i}.json')
    sleep()
    # save_hdf(df, f'df_water_vaex_{i}.hdf5')
    # sleep()

    # Handling missing data
    clear_caches()
    df = ve.read_csv('../datasets/water_potability.csv')
    sleep()
    isna(df, cname='ph')
    sleep()
    dropna(df)
    sleep()
    fillna(df, val=0)
    sleep()
    replace(df, cname='ph', src='NaN', dest='7.0')  # Use string if needed
    sleep()

    # Table operations
    clear_caches()
    df = ve.read_csv('../datasets/water_potability.csv')
    sleep()
    df_samp = ve.read_csv('../datasets/water_potability.csv')
    sleep()
    drop(df, col_names=['ph', 'Solids'])
    sleep()
    groupby(df, cname='Potability')
    sleep()
    concat_dataframes(df, df_samp)
    sleep()
    sort(df, 'Turbidity')
    sleep()
    merge(df, df_samp)
    sleep()

    # Statistical operations
    clear_caches()
    df = ve.read_csv('../datasets/water_potability.csv')
    sleep()
    count(df)
    sleep()
    sum(df, 'Hardness')
    sleep()
    mean(df['Conductivity'])
    sleep()
    min(df['Turbidity'])
    sleep()
    max(df['Turbidity'])
    sleep()
    unique(df['Potability'])
    sleep()

    print(f"Finished iteration {i+1}")
    clear_caches()

print("Process Complete")
csv_handler.save_data()
