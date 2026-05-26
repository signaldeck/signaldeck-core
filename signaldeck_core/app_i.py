import logging.config
import json



import numpy as np
import pandas as pd
import glob
import os

from .models.manager import manager


config_path = "config/haus.json"

def config():
    with open("logging_config_i.json", 'r') as logging_configuration_file:
        config_dict = json.load(logging_configuration_file)
        logging.config.dictConfig(config_dict)

def get_manager():
    houseManager = manager(config_path,collect_data=False)

    mqtt= houseManager.processor["mqtt_subscriber"]
    venus_soc= houseManager.processor["venus_soc"]
    venus_power= houseManager.processor["venus_power"]
    zappi= houseManager.processor["zappi"]
    goodwe= houseManager.processor["goodwe"]

    topic_strom = "tele/strom/SENSOR"
    topic_shelly = "shellypro3em-strom/status/em:0"

    print("mqtt.hist(topic_strom,\"total_out\")")
    print("mqtt.hist(topic_shelly,\"total_power\")")

    print("goodwe.hist(\"ppv\")")
    print("venus_soc.hist(\"soc\")")
    print("venus_power.hist(\"power\")")
    print("zappi.hist(\"che\")")


def list_csv_files(folder_path: str):
    """Gibt eine Liste aller CSV-Dateinamen (ohne Pfad) im angegebenen Ordner zurück."""
    pattern = os.path.join(folder_path, "*.csv")
    return [os.path.join(folder_path,os.path.basename(f)) for f in glob.glob(pattern)]

def close():
    houseManager.shutdown()
    print("Shutdown complete")
    exit(1)



#mqtt.hist(topic_strom,"total_out",days=1,fullDay=True)
#close()