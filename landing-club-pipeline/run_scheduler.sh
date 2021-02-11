#!/bin/bash

sleep 20 && airflow scheduler &
airflow serve_logs  > /dev/null 2>&1