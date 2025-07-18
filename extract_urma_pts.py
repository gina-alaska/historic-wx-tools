import os
import sys
from pathlib import Path
import csv
from itertools import islice
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Dict
import logging
from datetime import datetime, timedelta

import s3fs
from osgeo import gdal
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

def generate_dates(year_str):
    
    start_date = datetime.strptime(year_str + "0101", "%Y%m%d")
    end_date = datetime.strptime(str(int(year_str) + 1) + "0101", "%Y%m%d")
    
    return [(start_date + timedelta(days=i)).strftime("%Y%m%d")
            for i in range((end_date - start_date).days)]


def create_airport_dict(csv_path='./ak_airport_lat_lon.csv'):
    airport_dict = {}

    with open(csv_path) as airport_csv:
        reader = csv.reader(airport_csv, delimiter=',', quotechar='|')
        next(reader)
        for row in reader:
            airport, lat, lon = row
            if airport and lat and lon:
                airport_dict[airport] = (lat, lon)
    return airport_dict

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
    value = band.ReadAsArray(px, py, 1, 1)[0, 0]
    
    return value

def transform_airport_dict(airport_dict):
    for key in airport_dict:
        lat, lon = airport_dict[key]
        airport_dict[key] = (lat, lon, *transform_to_ds(gt, transformer, lat, lon))
    return airport_dict

@dataclass
class ProcessDateArgs:
    date_s3_path: str
    transformer: Transformer
    transform_airport_dict: Dict

def process_date(args: ProcessDateArgs):
    try:
        date_s3_path = args.date_s3_path
        transformer = args.transformer
        transformed_airport_dict = args.transform_airport_dict
        
        date_str = date_s3_path.split('.')[1]
        print(f"Processing {date_str}...")
        fs = s3fs.S3FileSystem(anon=True)
        with open(f'./{year}/akurma_{date_str}.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Date', 'Hour', 'Airport', 'Latitude', 'Longitude', 'Paramter', 'Value'])
            
            hourly_files = fs.glob(date_s3_path + "/akurma*2dvaranl*")
            
            if len(hourly_files) != 24:
                expected_hours = {f"{h:02d}" for h in range(24)}
                actual_hours = set([hour.split(".")[2][1:3] for hour in hourly_files])
                logging.info(f"Hour(s) missing from {date_s3_path}: {expected_hours - actual_hours}")
            
            for hour in fs.glob(date_s3_path + "/akurma*2dvaranl*"):
                hour_str = hour.split(".")[2][1:3]

                with gdal.Open('/vsis3/' + hour) as ds:
                
                    if not transformer:
                        gt, transformer = make_transformer(ds)
                    
                    if ds.RasterCount < expected_band_count:
                        logging.info(f"Band count ({ds.RasterCount}) is less than expected count ({expected_band_count}) for {hour}")
                    
                    for i in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(i)
                        try:
                            if var_codes[band.GetMetadata()['GRIB_COMMENT']]:
                                for key, value in transformed_airport_dict.items():
                                    pixel_value = extract_point_value(band, value[2], value[3], transform=False)
                                    writer.writerow([date_str, hour_str, key, value[0], value[1], var_codes[band.GetMetadata()['GRIB_COMMENT']], pixel_value])
                        except KeyError:
                            pass
    except Exception as e:
        import traceback
        print(f"Error processing {args.date_s3_path}:\n{traceback.format_exc()}", flush=True)
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <year>")
        sys.exit(1)
        
    year = sys.argv[1]
    
    log_file_path = os.path.join("./extract_urma_pts.log")
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file_path,
        level=logging.INFO,
    )
    
    logging.info(f"Beginning processing for {year}")
    
    os.makedirs(str(year), exist_ok=True)
    
    fs = s3fs.S3FileSystem(anon=True)
    s3_path = 'noaa-urma-pds'
    
    date_s3_paths = sorted(fs.glob(s3_path + f"/akurma.{year}*"))
    expected_dates = set(generate_dates(year))
    actual_dates = set([date.split('.')[1] for date in date_s3_paths])
    missing_dates = expected_dates - actual_dates
    if missing_dates:
        logging.info(f"Missing the following dates: {sorted(missing_dates)}")
    
    with gdal.Open('/vsis3/' + fs.ls(date_s3_paths[0])[0]) as template_ds:
        expected_band_count = template_ds.RasterCount
        gt, transformer = make_transformer(template_ds)
    airport_dict = create_airport_dict()
    transformed_airport_dict = transform_airport_dict(airport_dict)
    
    args_list = [ProcessDateArgs(date_s3_path, transformer, transformed_airport_dict) for date_s3_path in date_s3_paths]
    with ProcessPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process_date, args) for args in args_list]
        for future in futures:
            future.result()
