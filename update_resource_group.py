#!/usr/bin/env python3

import json
import sys
import requests
import fire

g_pd_url = 'http://127.0.0.1:2379/pd/api/v2/keyspaces/'
g_pd_rc_url = 'http://127.0.0.1:2379/resource-manager/api/v1/config/group/'

def _check_http_resp(resp):
    if resp.status_code != 200:
        print('got http error. reason: {}, code: {}, text: {}, url: {}'.format(resp.reason, resp.status_code, resp.text, resp.url))
        exit()

def _fetch_all_keyspaces():
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
    pd_params = {'limit': '10'}
    resp = requests.get(g_pd_url, params = pd_params)
    _check_http_resp(resp)
    return resp.json()['keyspaces']

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

def _get_resource_group_by_keyspace_id(keyspace_id):
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
    _check_http_resp(resp)
    return resp.json()

def _change_resource_group(rg_json, new_fillrate):
    rg_json['r_u_settings']['r_u']['settings']['fill_rate'] = new_fillrate
    return rg_json

def _put_new_rg(new_rg_json):
    resp = requests.put(g_pd_rc_url, json=new_rg_json)
    _check_http_resp(resp)

def _handle_by_arg(only_show, ori, new):
    if only_show == 'show_new_rg':
        print(ori)
    elif only_show == 'show_new_rg':
        print(new)
    elif only_show == '':
        _put_new_rg(new)
    else:
        print('unexpected only_show param, got {}'.format(only_show))

def change_by_cluster_id(clusterid, new_fillrate, only_show = ''):
    keyspace = _fetch_one_keyspace(clusterid)
    rg_json = _get_resource_group_by_keyspace_id(keyspace['id'])
    new_rg_json = _change_resource_group(rg_json, new_fillrate)
    _handle_by_arg(only_show, rg_json, new_rg_json)

def change_by_keyspace(keyspace_id, new_fillrate, only_show = ''):
    rg_json = _get_resource_group_by_keyspace_id(keyspace_id)
    new_rg_json = _change_resource_group(rg_json, new_fillrate)
    _handle_by_arg(only_show, rg_json, new_rg_json)

def change_by_all_keyspaces(new_fillrate, only_show = ''):
    keyspaces = _fetch_all_keyspaces()
    new_rg_jsons = []
    rg_jsons = []
    for keyspace in keyspaces:
        rg_json = _get_resource_group_by_keyspace_id(keyspace['id'])
        new_rg_json = _change_resource_group(rg_json, new_fillrate)

        rg_jsons.append(rg_json)
        new_rg_jsons.append(new_rg_json)
    _handle_by_arg(only_show, rg_jsons, new_rg_jsons)

# check https://github.com/google/python-fire/blob/master/examples/cipher/cipher.py
if __name__ == '__main__':
    fire.Fire(name = 'update_resource_group')
