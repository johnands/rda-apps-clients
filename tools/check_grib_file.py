import pygrib
import sys
import numpy as np
import matplotlib.pyplot as plt

filepath = sys.argv[1]
grib = pygrib.open(filepath)
for message in grib: print(message)

grib.seek(0)
for message in grib:
    print('\nName:', message['parameterName'])
    print('Level type:', message['typeOfLevel'])
    print('Level:', message['level'])
    print('Grid type:', message['gridDefinitionDescription'])
    print('Step type:', message['stepType'])
    print('Step range:', message['startStep'], message['endStep'])
    print('Unit', message['parameterUnits'])
    print('Resolution lat/lon in degrees:', message['jDirectionIncrementInDegrees'], message['iDirectionIncrementInDegrees'])
    print('Lat min/max:', message['latitudeOfFirstGridPointInDegrees'], message['latitudeOfLastGridPointInDegrees'])
    print('Lon min/max:', message['longitudeOfFirstGridPointInDegrees'], message['longitudeOfLastGridPointInDegrees'])
    data, lats, lons = message.data()
    print('Data shape/size:', data.shape, data.size)
    print('Values min/max/mean/median:', data.min(), data.max(), data.mean(), np.median(data))
    
    # plt.hist(data.flatten())
    # plt.show()
    # exit(1)

grib.close()