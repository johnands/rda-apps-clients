import src.python.rdams_client as rc
import time

result = rc.get_summary('ds084.1')
print(result)

def check_ready(rqst_id, wait_interval=1):
    """Checks if a request is ready."""
    for i in range(100): # 100 is arbitrary. Would wait 200 minutes for request
        res = rc.get_status(rqst_id)
        request_status = res['result']['status']
        if request_status == 'Completed':
            return True
        print(request_status)
        print('Not yet available. Waiting ' + str(wait_interval) + ' seconds.' )
        time.sleep(wait_interval)
    return False

control = {
    'dataset' : 'ds084.1',
    'date':'201609200000/to/201609200000',
    'datetype':'init',
    'param':'V GRD',
    'level':'HTGL:100',
    'oformat':'csv',
    'nlat':-10,
    'slat':-10,
    'elon':45,
    'wlon':45,
    'product':'Analysis'
}

response = rc.submit_json(control)
assert response['status'] == 'ok'
rqst_id = response['result']['request_id']

check_ready(rqst_id)
rc.download(rqst_id)