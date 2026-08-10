import time
import polars as pl
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
from clear_cache_util import clear_caches

import sys

# run_id = sys.argv[1] if len(sys.argv) > 1 else "0"
# csv_handler = CSVHandler(f"polaris_census_run{run_id}_v0.20.0_itr(30).csv")
csv_handler = CSVHandler(f"polaris_census_v0.20.0_itr(30).csv")


def sleep():
    time.sleep(30)

# ----------------- I/O FUNCTIONS ------------------

@measure_energy(handler=csv_handler)
def load_csv(path):
    return pl.read_csv(path)

@measure_energy(handler=csv_handler)
def load_json(path):
    return pl.read_json(path)

@measure_energy(handler=csv_handler)
def save_csv(df, path):
    df.write_csv(path)

@measure_energy(handler=csv_handler)
def save_json(df, path):
    df.write_json(path)

# ----------------- MISSING DATA ------------------

@measure_energy(handler=csv_handler)
def isna(df, cname):
    return df.select(pl.col(cname).is_null())

@measure_energy(handler=csv_handler)
def dropna(df):
    return df.drop_nulls()

@measure_energy(handler=csv_handler)
def fillna(df, val):
    return df.fill_null(val)

@measure_energy(handler=csv_handler)
def replace(df):
    return df.with_columns(
        pl.col("iCitizen").cast(str).replace("0", "X").alias("iCitizen")
    )

# ----------------- TABLE OPS ------------------

@measure_energy(handler=csv_handler)
def drop(df, cnameArray):
    return df.drop(cnameArray)

@measure_energy(handler=csv_handler)
def groupby(df, cname):
    return df.group_by(cname).count()

@measure_energy(handler=csv_handler)
def merge(df1, df2, on=None):
    if on:
        return df1.join(df2, on=on, how="inner")
    elif df1.height == df2.height:
        return df1.hstack(df2)
    else:
        print("Warning: Can't hstack due to mismatched row counts.")
        return df1

@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort(cname)

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return pl.concat([df1, df2])

# ----------------- STATS OPS ------------------

@measure_energy(handler=csv_handler)
def count(df):
    return df.select([pl.count()])

@measure_energy(handler=csv_handler)
def sum(df, cname):
    return df.select(pl.col(cname).sum())

@measure_energy(handler=csv_handler)
def mean(df):
    return df.select([pl.mean(col) for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]])

@measure_energy(handler=csv_handler)
def min_vals(df):
    return df.select([pl.min(col) for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]])

@measure_energy(handler=csv_handler)
def max(df):
    return df.select([pl.max(col) for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]])

@measure_energy(handler=csv_handler)
def unique(df, cname):
    return df.select(pl.col(cname).unique())

# ----------------- MAIN EXEC ------------------

print(f"Starting USCensus1990 Polars Process...")

for i in range(30):
    clear_caches()
    df = load_csv(f'../datasets/USCensus1990.csv')
    sleep()
    df = load_json(f'../datasets/USCensus1990.json')
    sleep()

    save_csv(df, f'df_USCensus1990_polars_{i}.csv')
    sleep()
    save_json(df, f'df_USCensus1990_polars_{i}.json')
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/USCensus1990.csv')
    sleep()
    isna(df, 'iCitizen')
    sleep()
    dropna(df)
    sleep()
    fillna(df, val='0')
    sleep()
    replace(df)
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/USCensus1990.csv')
    df_samp = df.sample(n=min(20000, df.height))
    sleep()
    drop(df, ['iKorean', 'iMarital'])
    sleep()
    groupby(df, 'iCitizen')
    sleep()
    concat_dataframes(df, df_samp)
    sleep()
    sort(df, 'dAge')
    sleep()
    merge(df, df_samp)
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/USCensus1990.csv')
    sleep()
    count(df)
    sleep()
    sum(df, 'dIncome1')
    sleep()
    mean(df)
    sleep()
    min_vals(df)
    sleep()
    max(df)
    sleep()
    unique(df, 'dAge')
    sleep()
    sleep()

    print(f"finished {i+1} iterations.")

csv_handler.save_data()
print("Process ended...")
