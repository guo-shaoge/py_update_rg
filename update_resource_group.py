#!/usr/bin/env python3

import json
import sys
import requests
import fire

g_pd_url = 'http://127.0.0.1:2379/pd/api/v2/keyspaces/'
g_pd_rc_url = 'http://127.0.0.1:2379/resource-manager/api/v1/config/group/'

# return true if stop==False and got error
def _check_http_resp(resp, stop = True):
    if resp.status_code != 200:
        print('[ERROR] got http error. reason: {}, code: {}, text: {}, url: {}'.format(resp.reason, resp.status_code, resp.text, resp.url), file= sys.stderr, flush=True)
        if stop:
            exit()
        else:
            return True
    return False

def _beg_end_valid(beg, end):
    return end >= beg and beg >= 0 and end >= 0

def _fetch_n_keyspaces(beg, end):
    # curl http://127.0.0.1:2379/pd/api/v2/keyspaces?limit=10
    # {
    #     "keyspaces": [
    #         {
    #             "id": 0,
    #             "name": "DEFAULT",
    #             "state": "ENABLED",
    #             "created_at": 1673581142,
    #             "state_changed_at": 1673581142,
    #             "config": {
    #                 "tso_keyspace_group_id": "0"
    #             }
    #         }
    #     ],
    #     "next_page_token": "2"
    # }
    keyspaces = []
    page_token = ''
    while True:
        pd_params = {'limit': '100'}
        if page_token != '':
            pd_params['page_token'] = page_token
        # ref: https://docs.python-requests.org/en/latest/api/#requests.Response
        resp = requests.get(g_pd_url, params = pd_params, timeout = 2)
        _check_http_resp(resp)
        res_json = resp.json()
        keyspaces.extend(res_json['keyspaces'])
        if ('next_page_token' not in res_json) or (_beg_end_valid(beg, end) and len(keyspaces) > end) or (res_json['next_page_token'] == ''):
            break
        else:
            page_token = res_json['next_page_token']
        if len(keyspaces) % 5000 == 0:
            print('[INFO] fetch n keyspaces process: ' + str(len(keyspaces)), flush=True)

    n_keyspaces = []
    if not _beg_end_valid(beg, end):
        n_keyspaces = keyspaces[:]
    else:
        n_keyspaces = keyspaces[beg:end]
        
    alive_keyspaces = []
    for keyspace in n_keyspaces:
        if keyspace['state'] != 'TOMBSTONE':
            alive_keyspaces.append(keyspace)

    print('[INFO] fetch n keyspaces done: all: {}, slice: {}, alive/ret: {}'.format(str(len(keyspaces)), str(len(n_keyspaces)), str(len(alive_keyspaces))), flush=True)
    return alive_keyspaces

def _fetch_one_keyspace(cluster_id):
    # curl 127.0.0.1:2379/pd/api/v2/keyspaces/uRkenLDNeAmDKjC
    # {
    #     "id": 1,
    #     "name": "uRkenLDNeAmDKjC",
    #     "state": "TOMBSTONE",
    #     "created_at": 1673581261,
    #     "state_changed_at": 1673587815,
    #     "config": {
    #         "gc_life_time": "6000",
    #         "serverless_cluster_id": "e2e-test-1131-304268294",
    #         "serverless_project_id": "",
    #         "serverless_tenant_id": "e2e-tenant-id",
    #         "tso_keyspace_group_id": "0"
    #     }
    # }
    resp = requests.get(g_pd_url + cluster_id)
    _check_http_resp(resp)
    return resp.json()

def _get_resource_group_by_keyspace_id(keyspace_id, stop = True):
    # curl -s 127.0.0.1:2379/resource-manager/api/v1/config/group/640055
    # {
    #   "name": "64005",
    #   "mode": 1,
    #   "r_u_settings": {
    #     "r_u": {
    #       "settings": {
    #         "fill_rate": 100000,
    #         "burst_limit": 50000000000
    #       },
    #       "state": {
    #         "tokens": 709290706.2351022,
    #         "last_update": "2023-09-11T08:08:58.021441499Z",
    #         "initialized": true
    #       }
    #     }
    #   },
    #   "priority": 0
    # }
    resp = requests.get(g_pd_rc_url + str(keyspace_id))
    got_err = _check_http_resp(resp, stop)
    if got_err:
        return '', got_err
    return resp.json(), got_err

def _change_resource_group(rg_jsons, new_fillrate):
    for rg_json in rg_jsons:
        rg_json['r_u_settings']['r_u']['settings']['fill_rate'] = new_fillrate
    return rg_jsons

def _put_new_rg(new_rg_jsons):
    for new_rg_json in new_rg_jsons:
        resp = requests.put(g_pd_rc_url, json=new_rg_json)
        _check_http_resp(resp)

def _handle_by_arg(only_show, rg_jsons, new_fillrate):
    if only_show == 'new':
        rg_jsons = _change_resource_group(rg_jsons, new_fillrate)
        print(json.dumps(rg_jsons, indent=2))
    elif only_show == 'ori':
        print(json.dumps(rg_jsons, indent=2))
    elif only_show == '':
        rg_jsons = _change_resource_group(rg_jsons, new_fillrate)
        _put_new_rg(rg_jsons)
    else:
        print('unexpected only_show param, got {}'.format(only_show))
        exit()

def fetch_n_keyspaces(beg=0, end=-1):
    keyspaces = _fetch_n_keyspaces(beg, end)
    print(json.dumps(keyspaces, indent=2))

def by_cluster_id(clusterid, new_fillrate, only_show = ''):
    keyspace = _fetch_one_keyspace(clusterid)
    rg_json, go_err = _get_resource_group_by_keyspace_id(keyspace['id'])
    rg_jsons = [rg_json]
    _handle_by_arg(only_show, rg_jsons, new_fillrate)

def by_keyspace(keyspace_id, new_fillrate, only_show = ''):
    rg_json, got_err = _get_resource_group_by_keyspace_id(keyspace_id)
    rg_jsons = [rg_json]
    _handle_by_arg(only_show, rg_jsons, new_fillrate)

def by_n_keyspaces(new_fillrate, beg=0, end=-1, only_show = ''):
    keyspaces = _fetch_n_keyspaces(beg, end)
    rg_jsons = []
    for keyspace in keyspaces:
        rg_json, got_err = _get_resource_group_by_keyspace_id(keyspace['id'], False)
        if got_err:
            continue
        rg_jsons.append(rg_json)
        if len(rg_jsons) % 1000 == 0:
            print('[INFO] update rg json in mem process: ' + str(len(rg_jsons)), flush=True)

    print('[INFO] update rg json in mem done: ' + str(len(rg_jsons)), flush=True)
    _handle_by_arg(only_show, rg_jsons, new_fillrate)

# check https://github.com/google/python-fire/blob/master/examples/cipher/cipher.py
if __name__ == '__main__':
    fire.Fire(name = 'update_resource_group')
