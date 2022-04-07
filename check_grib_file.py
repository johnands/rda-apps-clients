import pygrib
import sys

filepath = sys.argv[1]
grib = pygrib.open(filepath)
for message in grib: print(message)

grib.seek(0)
for message in grib:
    print('\nName:', message['parameterName'])
    print('Level type:', message['typeOfLevel'])
    print('Level:', message['level'])
    print('Grid type:', message['gridDefinitionDescription'])
    print('Resolution lat/lon in degrees:', message['jDirectionIncrementInDegrees'], message['iDirectionIncrementInDegrees'])
    print('Lat min/max:', message['latitudeOfFirstGridPointInDegrees'], message['latitudeOfLastGridPointInDegrees'])
    print('Lon min/max:', message['longitudeOfFirstGridPointInDegrees'], message['longitudeOfLastGridPointInDegrees'])
    data, lats, lons = message.data()
    print('Data shape/size:', data.shape, data.size)

grib.close()