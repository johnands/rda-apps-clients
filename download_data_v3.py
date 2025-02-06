import time
import pendulum as pm
from pendulum.datetime import DateTime as PmDateTime
from typing import *
import yaml
import threading
import traceback
import requests
import os
from pathlib import Path
import argparse
import re
import src.python.rdams_client as rda_client
from src.settings import SLEEP_INTERVAL
from src.utils.logger import scope_logger
from src.utils.entities import *
from src.config import parse_config, parse_time_intervals


def request_wrapper(func: Callable, *args, **kwargs) -> Dict[str, Any] | None:
    response = None
    n_retries = 10
    for _ in range(n_retries):
        try:
            #scope_logger.info('Making RDA API request')
            response: requests.Response = func(*args, **kwargs)
            if response.status_code != 200: print('Request failed with status code', response.status_code)
            response = response.json()
            return response

        except Exception:
            scope_logger.error('There was an exception during the request, trying again in 10 seconds')
            traceback.print_exc()
            time.sleep(5)

    scope_logger.info('Request could not be made, giving up')
    return None

def split_time_interval(from_dt: PmDateTime, to_dt: PmDateTime) -> List[Tuple[PmDateTime, PmDateTime]]:
    # one month of data per request is recommended for hourly data
    intervals = list((to_dt - from_dt).range('months', 1))
    if len(intervals) == 1:
        intervals = [(from_dt, to_dt)]
    else:
        intervals = [(intervals[idx], intervals[idx+1]) for idx in range(len(intervals) - 1)]
        if intervals[-1][1] < to_dt: intervals.append((intervals[-1][1], to_dt))
   
    # make sure the intervals don't overlap
    last_interval = intervals[-1]
    intervals = [(interval[0], interval[1].subtract(hours=6)) for interval in intervals[:-1]]
    intervals.append(last_interval)
    return intervals

def write_request_error_to_log(log_path: str, from_dt: PmDateTime, to_dt: PmDateTime, message: str) -> None:
    try:
        with open(log_path, 'a') as file:
            file.write(f'{pm.now("Europe/Oslo")} Request failed {from_dt.format("YYYYMMDDHHmm")} {to_dt.format("YYYYMMDDHHmm")} {message}\n')

    except Exception:
        scope_logger.error('Could not write to log file')
        traceback.print_exc()

def write_data_error_to_log(log_path: str, response_data: Dict[str, str]) -> None:
    try:
        subset_info = response_data['subset_info']['note']
        start_dt = re.search(r'Start date:\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2})', subset_info).group(1)
        end_dt = re.search(r'End date:\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2})', subset_info).group(1)
        with open(log_path, 'a') as file:
            file.write(f'{pm.now("Europe/Oslo").format("YYYYMMDDTHHmm")} request failed {start_dt} {end_dt}\n')
    
    except Exception:
        scope_logger.error('Could not write to log file')
        traceback.print_exc()

def download_worker(request_id: int, target_dir: Path, log_path: str) -> None:
    try:
        scope_logger.info(f'Starting download for request {request_id}')
        
        start = time.time()
        response = request_wrapper(rda_client.download, request_id, target_dir)
        scope_logger.info(f'Time elapsed: {time.time() - start} s')
        
        # keep request if unsuccessful to debug later
        if response is not None: 
            scope_logger.info('Download completed successfully, purging request')
        else:
            scope_logger.info('Could not download files, purging request')
            write_data_error_to_log(log_path, response['data'][0])
            
    except Exception:
        scope_logger.error('Exception in download worker, writing to error log')
        traceback.print_exc()
        write_data_error_to_log(log_path, response['data'][0])

    finally:
        print(f'Purging request {request_id}')
        request_wrapper(rda_client.purge_request, str(request_id))

def setup_requests(config_item: str, area: str, from_dt: PmDateTime | None = None, to_dt: PmDateTime | None = None, 
                  time_intervals_file: str | None = None) -> tuple[Dict[str, str], List[Tuple[PmDateTime, PmDateTime]]]:
    
    with open('./config/request_configs.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    scope_logger.info(f'Request config: {config[config_item]}')
    config = parse_config(config[config_item])

    scope_logger.info('Requesting following data:')
    if time_intervals_file is not None:
        time_intervals = parse_time_intervals(time_intervals_file)
        scope_logger.info(f'{len(time_intervals)} time intervals from file')
    else:
        time_intervals = split_time_interval(from_dt, to_dt)
        scope_logger.info(f'Time range: {from_dt} to {to_dt} in {len(time_intervals)} batches')

    scope_logger.info(f'Parameters: {config.parameters}')
    scope_logger.info(f'Levels: {config.levels}')
    scope_logger.info(f'Products:\n{config.products}')
    answer = input('Is this okay (y/N)? ')
    if answer != 'y': return 

    dataset_id = 'd084001'
    response = request_wrapper(rda_client.get_control_file_template, dataset_id)
    assert response is not None, scope_logger.info('Could not get control file template, aborting')
    control_file_template = response['data']['template']
    request_dict = rda_client.read_control_file(control_file_template)

    if area == 'global':
        area = Areas.GLOBAL
    elif area == 'europe':
        area = Areas.EUROPE
    else:
        raise ValueError('Area {} not recognized'.format(area))

    request_dict['dataset'] = dataset_id
    request_dict['datetype'] = 'init'
    request_dict['griddef'] = '1440:721:90N:0E:90S:359.75E:0.25:0.25'
    request_dict['gridproj'] = 'latLon'
    request_dict['param'] = config.parameters
    request_dict['level'] = config.levels
    request_dict['product'] = config.products
    request_dict['slat'] = area.lat_min
    request_dict['nlat'] = area.lat_max
    request_dict['wlon'] = area.lon_min
    request_dict['elon'] = area.lon_max

    # delete optional parameters
    if 'oformat' in request_dict: del request_dict['oformat']
    if 'groupindex' in request_dict: del request_dict['groupindex']
    if 'compression' in request_dict: del request_dict['compression']

    return request_dict, time_intervals

def service(request_dict: Dict[str, str], time_intervals: List[Tuple[PmDateTime, PmDateTime]], target_dir: Path, filter_request_ids: List[int] | None = None) -> None:
    max_requests = 10
    log_path = f'./data_cache/logs/{pm.now("Europe/Oslo").format("YYYYMMDDTHHmm")}.log'
    with open(log_path, 'a') as file:
        file.write(f'{str(request_dict)}\n')

    requests_downloaded = set()
    requests_error = set()
    while True:
        try:
            scope_logger.info('Checking status of requests')
            response = request_wrapper(rda_client.get_status)
            if response is None:
                scope_logger.info('Could not get status of existing requests, trying later')
                time.sleep(SLEEP_INTERVAL)
                continue
            
            current_requests = response['data']
            scope_logger.info(f'n_current_requests={len(current_requests)}, n_requests_error={len(requests_error)}, n_time_intervals={len(time_intervals)}, n_requests_downloaded={len(requests_downloaded)}')
            if len(current_requests) == len(requests_error) and len(time_intervals) == 0:
                scope_logger.info('Nothing more to do, exiting')
                break

            # handle current requests
            for request in current_requests:
                request_id = request['request_index']
                if filter_request_ids is not None and request_id not in filter_request_ids: continue
                
                request_status = request['status']
                if request_status == 'Completed' and request_id not in requests_downloaded:
                    with scope_logger.create_loggerscope(f'request_id={request_id}'):
                        #download_worker(request_id, target_dir, log_path)
                        threading.Thread(target=download_worker, args=(request_id, target_dir, log_path)).start()

                    requests_downloaded.add(request_id)
                
                elif request_status == 'Error':
                    if request_id not in requests_error:
                        scope_logger.error(f'Request {request_id} is faulty/stuck, contact rdahelp@ucar.edu for removal!')
                        requests_error.add(request_id)
                        write_data_error_to_log(log_path, request)
                
                else:
                    scope_logger.info(f'Request {request_id} has status {request_status}, waiting')

            # make new requests
            n_request_slots = max_requests - len(current_requests)
            n_new_requests_to_make = min(n_request_slots, len(time_intervals))
            for _ in range(n_new_requests_to_make):
                from_dt, to_dt = time_intervals.pop()
                request_dict_copy = request_dict.copy()
                request_dict_copy['date'] = '{}00/to/{}00'.format(from_dt.format('YYYYMMDDHH'), to_dt.format('YYYYMMDDHH'))
                
                scope_logger.info(f'Requesting data from {from_dt} to {to_dt}')
                response = request_wrapper(rda_client.submit_json, request_dict_copy)
                scope_logger.info(f'Response: {response}')

                if response is not None:
                    scope_logger.info('Request submitted successfully')
                    response_status = response['status']
                    if response_status == 'error':
                        scope_logger.error('Could not fetch data for this time interval, writing to log and skipping')
                        message = f'{response["http_response"]} {response["error_messages"]}'
                        write_request_error_to_log(log_path, from_dt, to_dt, message)

                else:
                    scope_logger.info('Could not submit request, writing to log and skipping')
                    message = 'Could not submit request'
                    write_request_error_to_log(log_path, from_dt, to_dt, message)

            time.sleep(SLEEP_INTERVAL)

        except Exception:
            print('Exception in main loop:')
            traceback.print_exc()
            time.sleep(SLEEP_INTERVAL)
            

def main():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title='command', dest='command')

    request_parser = subparser.add_parser('request', help='Service to request and download data for specified time interval(s)')
    request_parser.add_argument('--config_item', required=True, help='Request configuration in config/request_configs.yaml')
    request_parser.add_argument('--from_to', nargs=2, help='Date interval to fetch data for, format: "YYYY-MM-DDTHH:MM')
    request_parser.add_argument('--area', required=True, choices=['global', 'europe'], help='Predefined geographical area to fetch')
    request_parser.add_argument('--target_dir', required=True, help='Directory to download the data to')
    request_parser.add_argument('--time_intervals_file', help='CSV file with set of time intervals to fetch data for (arg from/to will be ignored)')

    download_parser = subparser.add_parser('download', help='Download previously requested datasets.')
    download_parser.add_argument('--request_ids', nargs='*', required=False, help='Download a specific request only, defaults to all active requests.')
    download_parser.add_argument('--target_dir', required=True, help='Directory to download the data to')
    download_parser.add_argument('--purge', action='store_true', help='Purge all requests for which download was successful')

    purge_parser = subparser.add_parser('purge', help='Purge a previously requested dataset.')
    purge_parser.add_argument('--request_ids', nargs='*', required=True, help='If "all", purge all active requests.')

    args = parser.parse_args()
    
    if args.command == 'request':
        os.makedirs(args.target_dir, exist_ok=True)

        if args.from_to is not None:
            from_dt = pm.parse(args.from_to[0], tz='UTC')
            to_dt = pm.parse(args.from_to[1], tz='UTC')
        else:
            from_dt = to_dt = None
        
        request_dict, time_intervals = setup_requests(args.config_item, args.area, from_dt, to_dt, args.time_intervals_file)
        service(request_dict, time_intervals, Path(args.target_dir))
    
    elif args.command == 'download':
        os.makedirs(args.target_dir, exist_ok=True)
        service(dict(), list(), Path(args.target_dir), args.request_ids)
    
    elif args.command == 'purge':
        if args.request_ids == 'all':
            status = request_wrapper(rda_client.get_status)
            assert status.type == ResponseTypes.SUCCESS, scope_logger.info('Could not get status of existing requests, aborting')
            for data in status.response['data']:
                scope_logger.info(f"Purging request {data['request_index']}")
                response = request_wrapper(rda_client.purge_request, str(data['request_index']))
                scope_logger.info(response)
        else:
            for request_id in args.request_ids:
                response = request_wrapper(rda_client.purge_request, request_id)
                scope_logger.info(response)


if __name__ == '__main__':
    main()

