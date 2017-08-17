#!/usr/bin/env bash

dump_path=~

./mongodump -h 172.16.215.16:40042 -d app_data -c register_code_clean  -u read -p read -o ${dump_path}

zip app_data.zip -r ${dump_path}/app_data/