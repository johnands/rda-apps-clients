import time
import pendulum as pm
import asyncio
import src.python.rdams_client as rc


def download_when_ready(request_id, target_dir='./data', wait_interval=10):
    while True:
        response = rc.get_status(request_id)
        request_status = response['result']['status']
        if request_status == 'Completed':
            start = time.time()
            rc.download(request_id, target_dir=target_dir)
            end = time.time()
            print('Time elapsed: {} s'.format(end - start))
            break
        
        print(request_status)
        print('Not yet available. Waiting ' + str(wait_interval) + ' seconds.' )
        time.sleep(wait_interval)

async def download_files():
    pass

def get_instant_products(metadata):
    # wind/temperature/humidity/pressure - exclude analysis and averages
    param_vars = list(filter(lambda x: x['param'] == 'TMP', metadata))
    products = list(set([item['product'] for item in param_vars if 'Forecast' in item['product']]))
    return products

def get_precip_products(metadata):
    # precipitation - only total accumulated
    param_vars = list(filter(lambda x: x['param'] == 'A PCP', metadata))
    products = list(set([item['product'] for item in param_vars if '(initial+0 ' in item['product']]))
    return products

def get_solar_products(metadata):
    # solar - 3 and 6 hour averages only, not 12 hour averages (not available after 06-12-2019)
    param_vars = list(filter(lambda x: x['param'] == 'DSWRF', metadata))
    products = list(set([item['product'] for item in param_vars if not item['product'].startswith('12-hour Average')]))
    return products

def get_parameter_set(set_name, metadata):
    if set_name == 'all':
        params = 'TMP/U GRD/V GRD/R H/DSWRF/A PCP/PRMSL'
        levels = 'HTGL:2/10/100;SFC:0;MSL:0'
        products = '/'.join(get_instant_products(metadata) + get_precip_products(metadata) + get_solar_products(metadata))
        return params, levels, products
    elif set_name == 'all_except_temp':
        params = 'U GRD/V GRD/R H/DSWRF/A PCP/PRMSL'
        levels = 'HTGL:2/10/100;SFC:0;MSL:0'
        products = '/'.join(get_instant_products(metadata) + get_precip_products(metadata) + get_solar_products(metadata))
        return params, levels, products
    elif set_name == 'temp':
        params = 'TMP'
        levels = 'HTGL:2'
        products = '/'.join(get_instant_products(metadata))
        return params, levels, products
    elif set_name == 'solar':
        params = 'DSWRF'
        levels = 'SFC:0'
        products = '/'.join(get_solar_products(metadata))
        return params, levels, products
    else:
        raise ValueError('Parameter set {} not implemented'.format(set_name))

def split_time_interval(from_dt, to_dt):
    #intervals = list((to_dt - from_dt).range('months', 3))
    intervals = list((to_dt - from_dt).range('days'))
    if len(intervals) == 1:
        intervals = [(from_dt, to_dt)]
    else:
        intervals = [(intervals[idx], intervals[idx+1]) for idx in range(len(intervals) - 1)]
        if intervals[-1][1] < to_dt: intervals.append((intervals[-1][1], to_dt))
    return intervals

def request_data(args):
    from_dt = pm.parse(args.from_to[0], tz='UTC')
    to_dt = pm.parse(args.from_to[1], tz='UTC')
    time_intervals = split_time_interval(from_dt, to_dt)

    dataset_id = 'ds084.1'
    response = rc.get_control_file_template(dataset_id)
    template = response['result']['template']
    template_dict = rc.read_control_file(template)

    # get selected products for all parameters
    metadata_response = rc.query(['-get_metadata', dataset_id])
    metadata = metadata_response['result']['data']
    params, levels, products = get_parameter_set(args.param_set, metadata)

    print('Requesting following data:')
    print('Time range: {} to {} in {} batches'.format(from_dt, to_dt, len(time_intervals)))
    print('Parameters:', params)
    print('Levels:', levels)
    print('Products:\n{}'.format(products))

    template_dict['datetype'] = 'init'
    template_dict['param'] = params
    template_dict['level'] = levels
    #template_dict['product'] = products
    template_dict['product'] = '6-hour Forecast'
    template_dict['slat'] = -90
    template_dict['nlat'] = 90
    template_dict['wlon'] = -180
    template_dict['elon'] = 180
    template_dict['targetdir'] = args.target_dir
    
    answer = input('Is this okay (y/n)?')
    if answer == 'y':
        request_ids = []
        for start_date, end_date in time_intervals:
            print('Requesting data from {} to {}'.format(start_date, end_date))
            
            template_dict['date'] = '{}0000/to/{}0000'.format(start_date.format('YYYYMMDD'), end_date.format('YYYYMMDD'))

            response = rc.submit_json(template_dict)
            if response['code'] != 200:
                print('Request could not be made:\n{}'.format(response))
                continue

            request_ids.append(response['result']['request_id'])
        
        for request_id in request_ids:
            if args.download: download_when_ready(request_id, target_dir=args.target_dir)
            if args.purge: rc.purge_request(request_id)

def main():
    import argparse

    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title='command', dest='command')

    request_parser = subparser.add_parser('request', help='Make a new data request, and (optional) download.')
    request_parser.add_argument('--param_set', required=True)
    request_parser.add_argument('--from_to', required=True, nargs=2)
    request_parser.add_argument('--target_dir', required=True)
    request_parser.add_argument('--download', action='store_true')
    request_parser.add_argument('--purge', action='store_true')

    download_parser = subparser.add_parser('download', help='Download previously requested dataset.')
    download_parser.add_argument('--request_id', required=False, help='Download a specific request only, defaults to all active requests.')
    download_parser.add_argument('--target_dir', required=True)
    download_parser.add_argument('--purge', action='store_true')

    purge_parser = subparser.add_parser('purge', help='Purge a previously requested dataset.')
    purge_parser.add_argument('--request_id', required=True, help='If "all", purge all active requests.')

    args = parser.parse_args()
    if args.command == 'request':
        request_data(args)
    elif args.command == 'download':
        if args.request_id is not None:
            download_when_ready(args.request_id, target_dir=args.target_dir)
            if args.purge: rc.purge_request(args.request_id)
        else:
            status = rc.get_status()
            for data in status['result']:
                request_id = data['request_index']
                print('Downloading request with id {}'.format(request_id))
                
                download_when_ready(request_id, target_dir=args.target_dir)
                if args.purge: rc.purge_request(request_id)
    else:
        if args.request_id == 'all':
            status = rc.get_status()
            for data in status['result']:
                response = rc.purge_request(data['request_index'])
                print(response)
        else:
            response = rc.purge_request(args.request_id)
            print(response)



if __name__ == '__main__':
    main()

