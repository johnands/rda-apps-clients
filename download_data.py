import time
import src.python.rdams_client as rc


def download_when_ready(request_id, target_dir='./data/', wait_interval=10):
    while True:
        response = rc.get_status(request_id)
        request_status = response['result']['status']
        if request_status == 'Completed':
            rc.download(request_id, target_dir=target_dir)
            break
        
        print(request_status)
        print('Not yet available. Waiting ' + str(wait_interval) + ' seconds.' )
        time.sleep(wait_interval)

dataset_id = 'ds084.1'
response = rc.get_control_file_template(dataset_id)
template = response['result']['template']
template_dict = rc.read_control_file(template)

# template_dict['date'] = '202203150000/to/202203150000'
# template_dict['datetype'] = 'init'
# template_dict['param'] = 'TMP/U GRD/V GRD/R H/DSWRF/A PCP/PRMSL'
# template_dict['level'] = 'HTGL:2/10/100;SFC:0;MSL:0'
# template_dict['product'] = '24-hour Forecast/3-hour Average (initial+24 to initial+27)/3-hour Accumulation (initial+24 to initial+27)'
# template_dict['slat'] = -90
# template_dict['nlat'] = 90
# template_dict['wlon'] = -180
# template_dict['elon'] = 180
# template_dict['targetdir'] = './data/'

def filter_solar_product(product):
    if product.startswith('3-hour Average'):
        return True
    elif product.startswith():
        return 

# get selected products for all parameters
metadata_response = rc.query(['-get_metadata', dataset_id])
metadata = metadata_response['result']['data']
all_products = []

# # wind/temperature/humidity/pressure - exclude analysis and averages
# param_vars = list(filter(lambda x: x['param'] == 'TMP', metadata))
# products = set([item['product'] for item in param_vars])
# _, products = zip(*sorted([(int(product.split('-')[0]), product) for product in products if 'Forecast' in product], key=lambda x: x[0]))
# all_products.extend(products)

# # precipitation - only total accumulated
# param_vars = list(filter(lambda x: x['param'] == 'A PCP', metadata))
# products = set([item['product'] for item in param_vars])
# _, products = zip(*sorted([(int(product.split('-')[0]), product) for product in products if '(initial+0 ' in product], key=lambda x: x[0]))
# all_products.extend(products)

# solar - only 6 hour averages
# param_vars = list(filter(lambda x: x['param'] == 'DSWRF', metadata))
# products = set([item['product'] for item in param_vars])
# _, products = zip(*sorted([(int(product.split('+')[-1][:-1]), product) for product in products if product.startswith('12-hour Average')], key=lambda x: x[0]))
# all_products.extend(products)

# precipitation rate
param_vars = list(filter(lambda x: x['param'] == 'PRATE', metadata))
products = set([item['product'] for item in param_vars])
_, products = zip(*sorted([(int(product.split('-')[0]), product) for product in products if 'Forecast' in product], key=lambda x: x[0]))
all_products.extend(products)

print(products)

template_dict['date'] = '202203150000/to/202203150000'
template_dict['datetype'] = 'init'
template_dict['param'] = 'PRATE'
template_dict['level'] = 'SFC:0'
template_dict['product'] = '/'.join(all_products)
template_dict['slat'] = -90
template_dict['nlat'] = 90
template_dict['wlon'] = -180
template_dict['elon'] = 180
template_dict['targetdir'] = './data/precip_rate/'

response = rc.submit_json(template_dict)
print(response)
assert response['code'] == 200

request_id = response['result']['request_id']
download_when_ready(request_id)
rc.purge_request(request_id)
