Tools for downloading historic forecasts over Alaska. Currently has methods developed for RTMA/URMA and LAMP forecasts.

For URMA, an example notebook [methods_testing.ipynb](methods_testing.ipynb) loads a file from https://noaa-urma-pds.s3.amazonaws.com/index.html and grabs a single value (at the Fairbanks lat and lon) from the temperature band of the dataset. RTMA could easily be accessed instead by pointing to the correct aws bucket and changing a few strings in the code to match rtma file names.

For LAMP, processing 
