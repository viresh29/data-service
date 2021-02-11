#!/bin/bash

sleep 10 && airflow initdb && airflow webserver
