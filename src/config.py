from typing import *
import pendulum as pm
import pandas as pd
import itertools
from src.utils.entities import RequestConfig

# 2022-04-06: archive changed from 12h to 6h frequency for forecast hours > 240
FORECAST_HOURS = list(range(3, 240, 3)) + list(range(240, 384 + 1, 6))

def get_instant_products():
    products = [f'{hour}-hour Forecast' for hour in FORECAST_HOURS]
    return products

def get_average_products():
    three_hour_averages = [f'3-hour Average (initial+{hour-3} to initial+{hour})' for hour in list(range(3, 240, 6))]
    six_hour_averages = [f'6-hour Average (initial+{hour-6} to initial+{hour})' for hour in list(range(6, 384+1, 6))]
    return three_hour_averages + six_hour_averages

def get_total_accumulated_products():
    products = [f'{hour}-hour Accumulation (initial+0 to initial+{hour}' for hour in FORECAST_HOURS]
    return products

def get_six_hour_accumulated_products():
    three_hour_accumulated = [f'3-hour Accumulation (initial+{hour-3} to initial+{hour})' for hour in list(range(3, 240, 6))]
    six_hour_accumulated = [f'6-hour Accumulation (initial+{hour-6} to initial+{hour})' for hour in list(range(6, 384+1, 6))]
    return three_hour_accumulated + six_hour_accumulated

def get_products_by_type(product_type: str) -> str:
    if product_type == 'instant':
        return get_instant_products()
    elif product_type == 'average':
        return get_average_products()
    elif product_type == 'total_accumulated':
        return get_total_accumulated_products()
    elif product_type == 'six_hour_accumulated':
        return get_six_hour_accumulated_products()
    else:
        raise ValueError('Product type {} not recognized'.format(product_type))

def parse_config(config: Dict[str, Any]) -> RequestConfig:
    """
    Parse the configuration file and return a dictionary with the parsed values.
    """

    parameters = '/'.join(config['parameters'])
    levels = ';'.join([f'{name}:{"/".join([str(value) for value in values])}' for name, values in config['levels'].items()])
    products ='/'.join(itertools.chain(*[get_products_by_type(product_type) for product_type in config['product_types']]))
    return RequestConfig(parameters, levels, products)

def parse_time_intervals(file_path: str) -> List[Tuple[pm.DateTime, pm.DateTime]]:
    time_intervals = pd.read_csv(file_path, header=None, names=['from_dt', 'to_dt'])
    time_intervals['from_dt'] = pd.to_datetime(time_intervals['from_dt'].str.strip(), format='%Y-%m-%d %H:%M:%S')
    time_intervals['to_dt'] = pd.to_datetime(time_intervals['to_dt'].str.strip(), format='%Y-%m-%d %H:%M:%S')
    time_intervals = [(pm.instance(from_dt), pm.instance(to_dt)) for from_dt, to_dt in zip(time_intervals['from_dt'], time_intervals['to_dt'])]
    return time_intervals
