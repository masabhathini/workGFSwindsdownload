#!/usr/bin/env python
import os, sys
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
xr.set_options(keep_attrs=True)
#startdatestamp = '20241018'
startdatestamp = sys.argv[1]
enddatestamp = sys.argv[2]
outputpath = sys.argv[3]
cycle = '00'
# Define the GFS OpenDAP URL
opendap_url = 'https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs' + startdatestamp + '/gfs_0p25_' + cycle + 'z'

# Open the dataset using xarray
ds = xr.open_dataset(opendap_url)

# Print dataset information
print(ds)
startdate = (datetime.strptime(startdatestamp,'%Y%m%d') + timedelta(days=0)).strftime('%Y%m%dT00')

# Selecting a variable (e.g., 'TMP' - Temperature) for a specific time step
ugrd10m = ds['ugrd10m'].sel(time=pd.date_range(start=startdate, end=enddatestamp, freq='3H'))
vgrd10m = ds['vgrd10m'].sel(time=pd.date_range(start=startdate, end=enddatestamp, freq='3H'))

# Mergeing the variables
result_global_ds = xr.merge([ugrd10m, vgrd10m])

# Rename the variables
result_global_ds = result_global_ds.rename({'ugrd10m':'U10', 'vgrd10m':'V10','lon':'longitude','lat':'latitude'})

outputfile = outputpath + 'gfsforecast/' + startdatestamp + cycle + '/gfs' + startdatestamp + cycle + '.nc'
os.makedirs(outputpath + 'gfsforecast/' + startdatestamp + cycle, exist_ok=True)
result_global_ds.to_netcdf(outputfile)
print('GFS downloaded file: ', outputfile)
