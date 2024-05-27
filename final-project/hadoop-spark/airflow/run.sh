#!/bin/bash

airflow connections add 'spark-connect' --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port '7077'
