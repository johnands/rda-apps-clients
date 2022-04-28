import time
import pendulum as pm
import os
import src.python.rdams_client as rc


WAIT_INTERVAL = int(os.getenv('WAIT_INTERVAL', 60*10))


def download_when_ready(request_id, target_dir='./data'):
    while True:
        try:
            response = rc.get_status(request_id)
            request_status = response['result']['status']
            print(request_status)

            if request_status == 'Completed':
                start = time.time()
                rc.download(request_id, target_dir=target_dir)
                end = time.time()
                print('Time elapsed: {} s'.format(end - start))
                break
                
            print('Not yet available. Waiting ' + str(WAIT_INTERVAL) + ' seconds.' )
            time.sleep(WAIT_INTERVAL)

        except Exception:
            import traceback
            traceback.print_exc()
            time.sleep(WAIT_INTERVAL)

def get_instant_products(metadata):
    # wind/temperature/humidity/pressure - exclude analysis and averages
    param_vars = list(filter(lambda x: x['param'] == 'TMP', metadata))
    products = list(set([item['product'] for item in param_vars if 'Forecast' in item['product']]))
    return products

def get_precip_products(metadata):
    # precipitation - total accumulated only available after 2019-06-12, before that only 3- and 6-hour accumulated are available
    param_vars = list(filter(lambda x: x['param'] == 'A PCP', metadata))
    #products = list(set([item['product'] for item in param_vars if '(initial+0 ' in item['product']]))
    #products = list(set([item['product'] for item in param_vars if item['product'].startswith('3-hour') or item['product'].startswith('6-hour')]))
    products = sorted(list(set([item['product'] for item in param_vars if item['product'].startswith('12-hour')])))[1:]
    return products

def get_solar_products(metadata):
    # solar - all products (for hours > 240, 12 hour averages are returned before 2019-06-12, and 6 hour averages after)
    param_vars = list(filter(lambda x: x['param'] == 'DSWRF', metadata))
    products = list(set([item['product'] for item in param_vars]))
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
    elif set_name == 'all_except_temp_solar':
        params = 'U GRD/V GRD/R H/A PCP/PRMSL'
        levels = 'HTGL:2/10/100;SFC:0;MSL:0'
        products = '/'.join(get_instant_products(metadata) + get_precip_products(metadata))
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
    elif set_name == 'precip':
        params = 'A PCP'
        levels = 'SFC:0'
        products = '/'.join(get_precip_products(metadata))
        return params, levels, products
    else:
        raise ValueError('Parameter set {} not implemented'.format(set_name))

def split_time_interval(from_dt, to_dt):
    intervals = list((to_dt - from_dt).range('months', 3))
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
    template_dict['product'] = products
    template_dict['slat'] = -90
    template_dict['nlat'] = 90
    template_dict['wlon'] = -180
    template_dict['elon'] = 180
    template_dict['targetdir'] = args.target_dir
    
    answer = input('Is this okay (y/n)?')
    if answer == 'y':
        time_intervals_not_requested = set(time_intervals)
        requests_to_download = set()
        while len(time_intervals_not_requested) > 0 or len(requests_to_download) > 0:
            try:
                print('\nNew iteration:')
                print('time intervals not requested:', time_intervals_not_requested)
                print('requests to download:', requests_to_download)
                for start_date, end_date in sorted(list(time_intervals_not_requested)):
                    print('Requesting data from {} to {}'.format(start_date, end_date))
                    
                    template_dict['date'] = '{}0000/to/{}0000'.format(start_date.format('YYYYMMDD'), end_date.format('YYYYMMDD'))

                    response = rc.submit_json(template_dict)
                    if response['code'] != 200:
                        print('Request could not be made:\n{}'.format(response))
                        continue
                    
                    time_intervals_not_requested.remove((start_date, end_date))
                    if args.download: requests_to_download.add(response['result']['request_id'])
                
                if args.download:
                    for request_id in sorted(list(requests_to_download)):
                        print('Starting downloading service for request {}'.format(request_id))
                        download_when_ready(request_id, target_dir=args.target_dir)
                        requests_to_download.remove(request_id)
                        if args.purge: rc.purge_request(request_id)

                print('Iteration ended, waiting {} seconds'.format(WAIT_INTERVAL))
                time.sleep(WAIT_INTERVAL)
                
            except Exception:
                import traceback
                traceback.print_exc()
                time.sleep(WAIT_INTERVAL)

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

