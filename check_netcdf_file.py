import xarray as xr
import sys

filepath = sys.argv[1]
data = xr.open_dataset(filepath)
print(data)