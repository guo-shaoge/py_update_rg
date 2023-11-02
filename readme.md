# Install third-party lib
apt-get install python3
apt-get install python3-pip
python3 -m pip install requests
pip install fire

# Usage
1. change fill_rate of resource group of all keyspaces to 100
```shell
./update_resource_group.py change_by_all_keyspaces 100
```
2. show resource group meta of all keyspaces
```shell
./update_resource_group.py change_by_keyspaces 100 'show_new_rg'
./update_resource_group.py change_by_keyspaces 100 'show_ori_rg'
```
3. show the first one resource group info
```shell
./update_resource_group.py by_n_keyspaces 1 1000 --only_show='both'
```
4. show keyspace info of keyspace_id of range [beg:end)
```shell
./update_resource_group.py fetch_n_keyspaces --beg 500 --end 700
```
5. get resource groups of keyspace_id of range [500, 1000)
```shell
./update_resource_group.py by_n_keyspaces 2000 --beg 50 --end 1000 --only_show='both'
```
