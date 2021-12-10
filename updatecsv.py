import dask.dataframe as dd
import csv
import sys

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

df = dd.read_csv('steam_reviews.csv', usecols=['app_name', 'recommended'], quoting=csv.QUOTE_MINIMAL, on_bad_lines="skip", engine="python")
df.to_csv('reviews.csv', index=False, single_file=True)
