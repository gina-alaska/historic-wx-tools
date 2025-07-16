import os
import sys
from pathlib import Path
import csv
from itertools import islice

import s3fs
from osgeo import gdal
import matplotlib.pyplot as plt
from pyproj import CRS, Transformer

os.environ["AWS_NO_SIGN_REQUEST"] = "YES"
gdal.UseExceptions()

var_codes = {
    'Geopotential height [gpm]': 'gpm',
    'Pressure [Pa]': 'pressure',
    'Temperature [C]': 'temp',
    'Dew point temperature [C]': 'dpt',
    'u-component of wind [m/s]': 'uw',
    'v-component of wind [m/s]': 'vw',
    'Specific humidity [kg/kg]': 'sh',
    'Wind direction (from which blowing) [deg true]': 'wd',
    'Wind speed [m/s]': 'ws',
    'Wind speed (gust) [m/s]': 'wgs',
    'Visibility [m]': 'vis',
    'Ceiling [m]': 'ceil',
    'Significant height of combined wind waves and swell [m]': 'sigheight',
    'Total cloud cover [%]': 'tcc'
}

airport_dict = {}

with open('./ak_airport_lat_lon.csv') as airport_csv:
    reader = csv.reader(airport_csv, delimiter=',', quotechar='|')
    next(reader)
    for row in reader:
        airport, lat, lon = row
        if airport and lat and lon:
            airport_dict[airport] = (lat, lon)
            
year=2019

def make_transformer(ds):
    print("Creating transformer")
    proj_wkt = ds.GetProjection()
    gt = ds.GetGeoTransform()
    crs_ds = CRS.from_wkt(proj_wkt)

    crs_wgs84 = CRS.from_epsg(4326)

    transformer = Transformer.from_crs(crs_wgs84, crs_ds, always_xy=True)
    return (gt, transformer)

def transform_to_ds(gt, transformer, x,y):
    x_geo, y_geo = transformer.transform(y, x)

    px = int((x_geo - gt[0]) / gt[1])
    py = int((y_geo - gt[3]) / gt[5])
    
    return px, py

def extract_point_value(band, lat, lon, transform=True):
    if transform:
        px, py = transform_to_ds(gt, transformer, lat, lon)
    else:
        px, py = lat, lon
    #band = ds.GetRasterBand(3)
    value = band.ReadAsArray(px, py, 1, 1)[0, 0]
    
    return value

fs = s3fs.S3FileSystem(anon=True)

s3_path = 'noaa-urma-pds'

dates = sorted(fs.glob(s3_path + f"/akurma.{year}*"))

template_ds = gdal.Open('/vsis3/' + fs.ls(dates[0])[0])

gt, transformer = make_transformer(template_ds)

for key in airport_dict:
    lat, lon = airport_dict[key]
    airport_dict[key] = (lat, lon, *transform_to_ds(gt, transformer, lat, lon))

for date in dates[:1]:
    date_str = date.split('.')[1]
    with open(f'./akurma_{date_str}.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=' ',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Date', 'Hour', 'Airport', 'Latitude', 'Longitude', 'Paramter', 'Value'])
        
        for hour in fs.glob(date + "/akurma*2dvaranl*"):
            hour_str = hour.split(".")[2][1:3]
            print(f"Processing {date_str} at {hour}...")

            ds = gdal.Open('/vsis3/' + hour)
            
            if not transformer:
                gt, transformer = make_transformer(ds)
                
            for i in range(1, ds.RasterCount + 1):
                band = ds.GetRasterBand(i)
                print(f"Getting values from Band {i}: {band.GetMetadata()['GRIB_COMMENT']}")
                for key, value in islice(airport_dict.items(), 30):
                    print(key, value)
                    pixel_value = extract_point_value(band, value[2], value[3], transform=False)
                    writer.writerow([date_str, hour_str, key, value[0], value[1], var_codes[band.GetMetadata()['GRIB_COMMENT']], pixel_value])

