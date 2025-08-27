Tools for downloading historic forecasts over Alaska. Currently has methods developed for RTMA/URMA and LAMP forecasts.

For URMA, an example notebook [method_testing.ipynb](method_testing.ipynb) loads a file from https://noaa-urma-pds.s3.amazonaws.com/index.html and grabs a single value (at the Fairbanks lat and lon) from the temperature band of the dataset. The scripted processing essentially does this for each file in a year with a list of lat/lon points at all AK airports. RTMA could easily be accessed instead by pointing to the correct aws bucket and changing a few strings in the code to match rtma file names.

For LAMP, processing parses the fixed-format archival messages into csvs. It uses the hourly updates (files ending in ...30z) and brings in a csv of Alaska stations to limit the output when postproccessing.

Both sets of scripts will produce individual csvs (daily for RTMA/URMA, by month:hour for LAMP), then stich then together and compress into zip files by year.
