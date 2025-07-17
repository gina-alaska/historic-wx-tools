import dask.dataframe as dd
import zipfile

year = 2020

df = dd.read_csv(f'./{year}/*.csv', delimiter=' ')

df.to_csv(f'akurma_{year}.csv', index=False, single_file=True)

with zipfile.ZipFile(f'akurma_{year}.csv.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(f'akurma_{year}.csv')