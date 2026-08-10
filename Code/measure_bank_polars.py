import time
import sys
import polars as pl
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
from clear_cache_util import clear_caches

csv_handler = CSVHandler("polars_bank_v1.31.0_itr(30).csv")

def sleep():
    time.sleep(30)

# ----------------- I/O FUNCTIONS ------------------

@measure_energy(handler=csv_handler)
def load_csv(path):
    return pl.read_csv(path, separator=';', quote_char='"')

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
def replace(df, cname, src, dest):
    return df.with_columns(
        pl.col(cname).replace(src, dest).alias(cname)
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
        print("Warning: Cannot hstack due to mismatched row counts.")
        return df1

@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort(cname)

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return pl.concat([df1, df2])

# ----------------- STATISTICAL OPS ------------------

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
def min_val(df, cname):
    return df.select(pl.col(cname).min())

@measure_energy(handler=csv_handler)
def max_val(df, cname):
    return df.select(pl.col(cname).max())

@measure_energy(handler=csv_handler)
def unique(df, cname):
    return df.select(pl.col(cname).unique())

# ----------------- MAIN EXEC ------------------

print("Starting Bank Polars Process...")

for i in range(30):
    df = load_csv(f'../datasets/bank.csv')
    sleep()
    df = load_json(f'../datasets/bank.json')
    sleep()

    save_csv(df, f'df_bank_polars_{i}.csv')
    sleep()
    save_json(df, f'df_bank_polars_{i}.json')
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/bank.csv', separator=';', quote_char='"')
    sleep()
    isna(df, 'job')
    sleep()
    dropna(df)
    sleep()
    fillna(df, val='N/A')
    sleep()
    replace(df, 'job', 'unknown', 'X')
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/bank.csv', separator=';', quote_char='"')
    df_samp = df.sample(n=min(20000, df.height))
    sleep()
    drop(df, ['job', 'education'])
    sleep()
    groupby(df, 'job')
    sleep()
    concat_dataframes(df, df_samp)
    sleep()
    sort(df, 'age')
    sleep()
    merge(df, df_samp, on='age')
    sleep()

    clear_caches()
    df = pl.read_csv(f'../datasets/bank.csv', separator=';', quote_char='"')
    sleep()
    count(df)
    sleep()
    sum(df, 'balance')
    sleep()
    mean(df)
    sleep()
    min_val(df, 'balance')
    sleep()
    max_val(df, 'balance')
    sleep()
    unique(df, 'job')
    sleep()

    print(f"Finished iteration {i+1}")

csv_handler.save_data()
print("Process complete.")
