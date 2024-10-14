import time
import pendulum as pm
from typing import *
import yaml
from dataclasses import dataclass
import queue
import threading
import traceback
import requests
import os
from pathlib import Path
import src.python.rdams_client as rda_client
from src.settings import SLEEP_INTERVAL
from src.utils.logger import scope_logger
from src.utils.entities import *
from src.config import parse_config, parse_time_intervals


def request_wrapper(func: Callable, *args, **kwargs) -> Response:
    n_retries = 20
    for _ in range(n_retries):
        try:
            scope_logger.info('Making RDA API request')
            response = func(*args, **kwargs)

            if isinstance(response, requests.Response):
                status_code = response.status_code
                convert_to_json = True
            elif isinstance(response, dict):
                status_code = response['http_response']
                convert_to_json = False
            else:
                raise ValueError('Response type not recognized')
            
            if status_code == 200:
                scope_logger.info('Request made successfully')
                if convert_to_json: response = response.json()
                return Response(ResponseTypes.SUCCESS, response)
            else:
                scope_logger.info(f'Request was not successful: status_code={status_code}')
                if convert_to_json: response = response.json()
                scope_logger.info(f'JSON response: status={response["status"]}, http_response={response["http_response"]}, error_messages={response["error_messages"]}')
                return Response(ResponseTypes.ERROR, response)
        
        except Exception:
            scope_logger.info('There was an exception during the request, trying again in 2 seconds')
            traceback.print_exc()
            time.sleep(2)

    scope_logger.info('Request could not be made, giving up')
    return Response(ResponseTypes.EXCEPTION, None)


def split_time_interval(from_dt, to_dt):
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

def download_worker(request_id: int, target_dir: Path) -> bool:
    scope_logger.info(f'Starting downloading service for request {request_id}')
    start_time = time.time()
    while True:
        try:
            # prevent faulty requests from running forever
            if time.time() - start_time > 60*60*12:
                scope_logger.info(f'Request has been downloading for more than 12 hours, skipping')
                return False
            
            response = request_wrapper(rda_client.get_status, request_id)
            request_status = response.response['data']['status']
            scope_logger.info(f'Response: status={response.response["status"]}, error_messages={response.response["error_messages"]}, data request status={request_status}')

            if request_status == 'Completed':
                scope_logger.info('Request is ready for download')
                start = time.time()
                response = request_wrapper(rda_client.download, request_id, target_dir)
                scope_logger.info(f'Time elapsed: {time.time() - start} s')
                
                # keep request if unsuccessful to debug later
                if response.type == ResponseTypes.SUCCESS: 
                    scope_logger.info('Download completed successfully, purging request')
                    request_wrapper(rda_client.purge_request, str(request_id))
                    return True
                else:
                    return False
                
            elif request_status == 'Set for Purge':
                scope_logger.info(f'Request has been purged, skipping')
                return False
            
            elif request_status == 'Error':
                scope_logger.info(f'Request has status Error, skipping')
                return False
                
            scope_logger.info(f'Not yet available. Waiting {SLEEP_INTERVAL} seconds.' )
            time.sleep(SLEEP_INTERVAL)

        except Exception:
            traceback.print_exc()
            time.sleep(SLEEP_INTERVAL)

def request_and_download_worker(request: Dict[str, str], target_dir: Path) -> bool:
    scope_logger.info(f'Starting request service for date interval {request["date"]}')
    start_time = time.time()
    while True:
        # disable request if it has been running for more than 1 hour
        if time.time() - start_time > 60*60:
            scope_logger.info(f'Have tried to request data for more than 1 hour, skipping')
            return False
        
        with scope_logger.create_loggerscope(f'New data: {request["date"]}'):
            response = request_wrapper(rda_client.submit_json, request)
        
        if response.type == ResponseTypes.SUCCESS:
            if 'request_id' not in response.response["data"]:
                scope_logger.info('Could not obtain request_id, skipping')
                return False
            
            request_id = response.response["data"]["request_id"]
            with scope_logger.create_loggerscope(f'request_id={request_id}'):
                success = download_worker(request_id, target_dir)
                return success
            
        else:
            time.sleep(SLEEP_INTERVAL)

@dataclass
class ThreadRequests:
    tasks: queue.Queue[Task] = queue.Queue()

    def __init__(self, request_dict: Dict[str, str], date_intervals: List[str], existing_request_ids: List[int], target_dir: Path, n_threads: int = 11) -> None:
        self.target_dir = target_dir
        self.log_path = f'./data_cache/failed_tasks_{pm.now("Europe/Oslo").to_datetime_string()}.log'
        self.n_threads = min(len(date_intervals) + len(existing_request_ids), n_threads)
        
        # download previously requested data first
        for request_id in existing_request_ids:
            self.tasks.put(Task('download_existing_data', request_id))

        # download new data in a decreasing chronological order
        for from_dt, to_dt in reversed(date_intervals):
            # beware of python's weird scoping rules!!!
            request_dict_copy = request_dict.copy()
            request_dict_copy['date'] = '{}00/to/{}00'.format(from_dt.format('YYYYMMDDHH'), to_dt.format('YYYYMMDDHH'))
            self.tasks.put(Task('request_new_data', request_dict_copy))

    def run(self) -> None:
        for _ in range(self.n_threads):
            threading.Thread(target=self.worker).start()
        
        self.tasks.join()

    def worker(self) -> None:
        scope_logger.info(f'Starting worker thread {threading.get_ident()}')
        while not self.tasks.empty():
            try:
                task = self.tasks.get()
                if task.name == 'download_existing_data':
                    with scope_logger.create_loggerscope(f'request_id={task.argument}'):
                        success = download_worker(task.argument, self.target_dir)
                elif task.name == 'request_new_data':
                    success = request_and_download_worker(task.argument, self.target_dir)
                else:
                    scope_logger.info(f'Task type {task.name} not recognized')
                    success = False
                
                self.tasks.task_done()
                if not success: 
                    with open(self.log_path, 'a') as file:
                        file.write(f'{pm.now("Europe/Oslo").to_datetime_string()} {task.name}\n{task.argument}\n')
            
            except Exception:
                scope_logger.info('Exception in worker:')
                traceback.print_exc()
                time.sleep(SLEEP_INTERVAL)

def request_data(args):
    with open('./config/request_configs.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    dataset_id = 'd084001'
    response = request_wrapper(rda_client.get_metadata, dataset_id)
    assert response.type == ResponseTypes.SUCCESS, scope_logger.info('Could not get metadata, aborting')
    metadata = response.response['data']['data']

    # all parameters have the same grid definition
    grid_definition = metadata[0]['griddef']

    scope_logger.info(config[args.config_item])
    config = parse_config(config[args.config_item])

    scope_logger.info('Requesting following data:')
    if args.time_intervals_file is not None:
        time_intervals = parse_time_intervals(args.time_intervals_file)
        scope_logger.info(f'{len(time_intervals)} time intervals from file')
    else:
        from_dt = pm.parse(args.from_to[0], tz='UTC')
        to_dt = pm.parse(args.from_to[1], tz='UTC')
        time_intervals = split_time_interval(from_dt, to_dt)
        scope_logger.info(f'Time range: {from_dt} to {to_dt} in {len(time_intervals)} batches')

    scope_logger.info(f'Parameters: {config.parameters}')
    scope_logger.info(f'Levels: {config.levels}')
    scope_logger.info(f'Products:\n{config.products}')
    
    answer = input('Is this okay (y/N)? ')
    if answer != 'y': return 

    response = request_wrapper(rda_client.get_control_file_template, dataset_id)
    assert response.type == ResponseTypes.SUCCESS, scope_logger.info('Could not get control file template, aborting')
    control_file_template = response.response['data']['template']
    request_dict = rda_client.read_control_file(control_file_template)

    if args.area == 'global':
        area = Areas.GLOBAL
    elif args.area == 'europe':
        area = Areas.EUROPE
    else:
        raise ValueError('Area {} not recognized'.format(args.area))

    request_dict['dataset'] = dataset_id
    request_dict['datetype'] = 'init'
    request_dict['griddef'] = grid_definition
    request_dict['param'] = config.parameters
    request_dict['level'] = config.levels
    request_dict['product'] = config.products
    request_dict['slat'] = area.lat_min
    request_dict['nlat'] = area.lat_max
    request_dict['wlon'] = area.lon_min
    request_dict['elon'] = area.lon_max
    request_dict['targetdir'] = args.target_dir

    # delete optional parameters
    del request_dict['gridproj']
    del request_dict['oformat']
    del request_dict['groupindex']
    del request_dict['compression']

    existing_request_ids = []
    if args.include_existing_requests:
        response = request_wrapper(rda_client.get_status)
        assert response.type == ResponseTypes.SUCCESS, scope_logger.info('Could not get status of existing requests, aborting')
        for request in response.response['data']: existing_request_ids.append(request['request_index'])

    runner = ThreadRequests(request_dict, time_intervals, existing_request_ids, Path(args.target_dir))
    runner.run()

def main():
    import argparse

    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title='command', dest='command')

    request_parser = subparser.add_parser('request', help='Make a new data request, and (optional) download.')
    request_parser.add_argument('--config_item', required=True, help='Request configuration in config/request_configs.yaml')
    request_parser.add_argument('--from_to', required=False, nargs=2, help='Date interval to fetch data for')
    request_parser.add_argument('--area', required=True, choices=['global', 'europe'], help='Predefined geographical area to fetch')
    request_parser.add_argument('--target_dir', required=True, help='Directory to save the data to')
    request_parser.add_argument('--include_existing_requests', action='store_true', help='Include any requests made previously in the download process')
    request_parser.add_argument('--download', action='store_true', help='Download all requested files')
    request_parser.add_argument('--purge', action='store_true', help='Purge all requests after download is completed')
    request_parser.add_argument('--time_intervals_file', help='File with set of time intervals to fetch data for (arg from/to will be ignored)')

    download_parser = subparser.add_parser('download', help='Download previously requested dataset.')
    download_parser.add_argument('--request_ids', nargs='*', required=False, help='Download a specific request only, defaults to all active requests.')
    download_parser.add_argument('--target_dir', required=True, help='Directory to save the data to')
    download_parser.add_argument('--purge', action='store_true', help='Purge all requests after download is completed')

    purge_parser = subparser.add_parser('purge', help='Purge a previously requested dataset.')
    purge_parser.add_argument('--request_ids', nargs='*', required=True, help='If "all", purge all active requests.')

    args = parser.parse_args()
    os.makedirs(args.target_dir, exist_ok=True)

    if args.command == 'request':
        request_data(args)
    
    elif args.command == 'download':
        if args.request_ids is not None:
            existing_request_ids = list(args.request_ids)
        else:
            status = request_wrapper(rda_client.get_status)
            assert status.type == ResponseTypes.SUCCESS, scope_logger.info('Could not get status of existing requests, aborting')
            existing_request_ids = [request['request_index'] for request in status.response['data']]
            
        runner = ThreadRequests(dict(), list(), existing_request_ids, Path(args.target_dir))
        runner.run()
    
    else:
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

