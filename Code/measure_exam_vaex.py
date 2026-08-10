from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler
import time
import vaex as ve
from clear_cache_util import clear_caches

csv_handler = CSVHandler('Vaex_exam_v4.13.0_itr(30).csv')

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

print("Starting Exam Score Dataset Vaex Process...")
for i in range(30):
    clear_caches()
    df = load_csv('../datasets/exam_score.csv')
    sleep()
    clear_caches()
    df = load_json('../datasets/exam_score.json')
    sleep()
    # df = load_hdf('../datasets/exam_score_vaex.hdf5')
    # sleep()

    save_csv(df, f'df_exam_vaex_{i}.csv')
    sleep()
    save_json(df, f'df_exam_vaex_{i}.json')
    sleep()
    # save_hdf(df, f'df_exam_vaex_{i}.hdf5')
    # sleep()

    clear_caches()
    df = ve.read_csv('../datasets/exam_score.csv')
    sleep()
    isna(df, 'ReadingScore')
    sleep()
    dropna(df)
    sleep()
    fillna(df, 0)
    sleep()
    replace(df, 'Gender', 'female', 'F')
    sleep()

    clear_caches()
    df = ve.read_csv('../datasets/exam_score.csv')
    df_samp = ve.read_csv('../datasets/exam_score.csv')
    sleep()
    drop(df, ['ReadingScore'])
    sleep()
    groupby(df, 'Gender')
    sleep()
    concat_dataframes(df, df_samp)
    sleep()
    sort(df, 'ReadingScore')
    sleep()
    merge(df, df_samp)
    sleep()

    clear_caches()
    df = ve.read_csv('../datasets/exam_score.csv')
    count(df)
    sleep()
    sum(df, 'ReadingScore')
    sleep()
    mean(df['ReadingScore'])
    sleep()
    min(df['ReadingScore'])
    sleep()
    max(df['ReadingScore'])
    sleep()
    unique(df['Gender'])
    sleep()

    print(f"Finished iteration {i+1}")
    clear_caches()

print("Exam Score Dataset Process Complete.")
csv_handler.save_data()
