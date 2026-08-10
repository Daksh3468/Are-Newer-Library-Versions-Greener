import time
import sys
import json
import polars as pl
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
from clear_cache_util import clear_caches

csv_handler = CSVHandler('polars_drug_v1.31.0_itr(30).csv')

def sleep():
    time.sleep(30)

# ----------------- I/O FUNCTIONS ------------------

@measure_energy(handler=csv_handler)
def load_csv(path):
    return pl.read_csv(path)

@measure_energy(handler=csv_handler)
def load_json(path):
    with open(path, 'r') as f:
        data = json.load(f, strict=False)
    return pl.DataFrame(data)

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
    if df1.height == df2.height:
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
def min_val(df, cname):
    return df.select(pl.col(cname).min())

@measure_energy(handler=csv_handler)
def max_val(df, cname):
    return df.select(pl.col(cname).max())

@measure_energy(handler=csv_handler)
def unique(df, cname):
    return df.select(pl.col(cname).unique())

# ----------------- MAIN EXEC ------------------

print("Starting Drug Polars Process...")

for i in range(30):
    # I/O Operations
    clear_caches()
    df = load_csv(path='../datasets/drugs.csv')
    sleep()
    df = load_json(path='../datasets/drugs.json')
    print("load done")
    sleep()

    save_csv(df, f'df_drug_polars{i}.csv')
    sleep()
    print("save csv done")
    save_json(df, f'df_drug_polars{i}.json')
    sleep()
    print("save json done")

    # ----------------- MISSING DATA ------------------
    clear_caches()
    df = pl.read_csv('../datasets/drugs.csv')
    sleep()
    isna(df, 'review')
    print("isna done")
    sleep()
    dropna(df)
    print("dropna done")
    sleep()
    fillna(df, val='0')
    print("fillna done")
    sleep()
    replace(df, 'review', '?', 'X')
    print("replace done")
    sleep()

    # ----------------- TABLE OPS ------------------
    clear_caches()
    df = pl.read_csv('../datasets/drugs.csv')
    df_samp = df.sample(n=min(20000, df.height))
    sleep()
    drop(df, ['drugName'])
    print("drop done")
    sleep()
    groupby(df, 'rating')
    print("groupby done")
    sleep()
    concat_dataframes(df, df_samp)
    print("concat_dataframes done")
    sleep()
    sort(df, 'rating')
    print("sort done")
    sleep()
    merge(df, df_samp)
    print("merge done")
    sleep()

    # ----------------- STATISTICAL OPS ------------------
    clear_caches()
    df = pl.read_csv('../datasets/drugs.csv')
    sleep()
    count(df)
    print("count done")
    sleep()
    sum(df, 'usefulCount')
    print("sum done")
    sleep()
    mean(df)
    print("mean done")
    sleep()
    min_val(df, 'usefulCount')
    print("min_val done")
    sleep()
    max_val(df, 'usefulCount')
    print("max_val done")
    sleep()
    unique(df, 'condition')
    print("unique done")
    sleep()

    print(f"Finished iteration {i+1}")

csv_handler.save_data()
print("Process ended.")
