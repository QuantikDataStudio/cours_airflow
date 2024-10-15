import time
from datetime import datetime

import duckdb
from airflow.decorators import task, task_group
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from requests.auth import HTTPBasicAuth
import json

CREDENTIALS = {"username": 'gluino', "password": 'jpz*xba@HVE5djh0xrk'}
DATA_FILE_NAME = 'dags/data/data.json'
liste_des_apis = [
    {
        "schedule": "30 9 * * *",
        "nom": "states",
        "url": "https://opensky-network.org/api/states/all?extended=true",
        "columns": [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
            "category",
        ],
        "target_table": "bdd_airflow.main.openskynetwork_brute",
        "timestamp_required": False
    },
    {
        "schedule": "30 8 * * *",
        "nom": "flights",
        "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "columns": ['icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen', 'estArrivalAirport', 'callsign',
                    'estDepartureAirportHorizDistance', 'estDepartureAirportVertDistance',
                    'estArrivalAirportHorizDistance', 'estArrivalAirportVertDistance',
                    'departureAirportCandidatesCount', 'arrivalAirportCandidatesCount'],
        "target_table": "bdd_airflow.main.flights_brut",
        "timestamp_required": True
    }
]

def states_to_dict(states_list, colonnes, timestamp):
    out = []
    for state in states_list:
        state_dict = dict(zip(colonnes, state))
        state_dict['timestamp'] = timestamp
        out.append(state_dict)
    return out


def flights_to_dict(flights, timestamp):
    out = []
    for flight in flights:
        flight['timestamp'] = timestamp
        out.append(flight)
    return out


def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")


@task(multiple_outputs=True)
def run_parameters(api, dag_run=None):
    out = api

    data_interval_start = format_datetime(dag_run.data_interval_start)
    data_interval_end = format_datetime(dag_run.data_interval_end)

    data_file_name = f'dags/data/data_{out["nom"]}_{data_interval_start}_{data_interval_end}.json'
    out["data_file_name"] = data_file_name

    # SQL pour charger les données dans la DWH
    with open("dags/load_from_file.sql", "r") as f:
        load_from_file_sql = f.read().format(target_table=out['target_table'], data_file_name=data_file_name)
    out['load_from_file_sql'] = load_from_file_sql

    if out['timestamp_required']:
        end = int(time.time())
        begin = end - 3600
        out["url"] = out["url"].format(begin=begin, end=end)

    return {"run_params": out}


@task_group()
def ingestion_donnees_tg(creds, run_params):
    get_flight_data(creds=creds, run_params=run_params) >> load_from_file(run_params)


@task(multiple_outputs=True)
def get_flight_data(creds, run_params):
    # Retrouve les paramètres de XCOM
    data_file_name = run_params['data_file_name']
    url = run_params["url"]
    colonnes = run_params["columns"]

    # Télécharge les données
    req = requests.get(url, auth=HTTPBasicAuth(creds["username"], creds["password"]))
    req.raise_for_status()
    resp = req.json()

    # Transforme les données selon l'API d'origine
    if "states" in resp:
        timestamp = resp['time']
        results_json = states_to_dict(resp['states'], colonnes, timestamp)
    else:
        timestamp = int(time.time())
        results_json = flights_to_dict(resp, timestamp)

    with open(data_file_name, 'w') as f:
        json.dump(results_json, f)

    return {"filename": data_file_name, "timestamp": timestamp, "rows": len(results_json)}


@task()
def load_from_file(run_params):
    with duckdb.connect("dags/data/bdd_airflow") as conn:
        conn.sql(run_params['load_from_file_sql'])


@task()
def check_row_numbers():
    # TODO
    pass


def check_duplicates():
    # TODO
    pass


@task_group
def data_quality_tg():
    # TODO
    pass


@dag(
    start_date=datetime(2024, 10, 1),
    schedule=None,
    catchup=False,
    concurrency=1,
)
def flights_pipeline():
    run_parameters_task = run_parameters.expand(api=liste_des_apis)
    (
            EmptyOperator(task_id="start")
            >> run_parameters_task
            >> ingestion_donnees_tg.partial(creds=CREDENTIALS).expand_kwargs(run_parameters_task)
            >> EmptyOperator(task_id="end")
    )



flights_pipeline()
