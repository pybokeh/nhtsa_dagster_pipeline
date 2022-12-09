from dagster import (
    op,
    In,
    Nothing,
)
from datetime import datetime
from tqdm import tqdm
from urllib.error import HTTPError
import duckdb
import json
import os
import pandas as pd
import requests
import traceback


@op(
    config_schema={'table_name': str}
)
def upload_df_to_duckdb(context, df: pd.DataFrame) -> Nothing:
    with duckdb.connect(database=os.getenv("DUCKDB_PATH"), read_only=False) as con:
        table_name = context.op_config['table_name']
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")


@op
def fetch_manufacturers(context) -> pd.DataFrame:
    """
    Obtain vehicle manufacturer information from NHTSA API
    """

    df_list = []
    page = 1
    while True:
        url = f'https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?ManufacturerType=&format=json&page={page}'
        context.log.info(f"Fetching page {page}")
        r = requests.get(url)
        json_dict = json.loads(r.text)
        if json_dict['Count'] == 0:
            context.log.info("Count is equal to zero/0 - exiting loop")
            break
        else:
            # json_normalize() will drop records where the record_path contains an empty list
            # To prevent this, see this SO question:
            # https://stackoverflow.com/questions/63813378/how-to-json-normalize-a-column-in-pandas-with-empty-lists-without-losing-record
            # For loop below is checking for "emptiness" of VehicleTypes, if empty,then fill with "dummy" dictionary
            for i, record in enumerate(json_dict['Results']):
                if not record['VehicleTypes']:
                    json_dict['Results'][i]['VehicleTypes'] = [{'IsPrimary': 'Null', 'Name': 'Null'}]

            context.log.info(f"    Count={json_dict['Count']}")

            df = pd.json_normalize(
                json_dict['Results'],
                record_path=['VehicleTypes'],
                meta=['Country', 'Mfr_CommonName', 'Mfr_ID', 'Mfr_Name'],
            )
            df_list.append(df)
            page = page + 1

    df_combined = pd.concat(df_list, ignore_index=True)
    context.log.info(f"Number of rows in manufacturers dataframe: {df_combined.shape[0]}")

    # return dataframe without duplicate records (dupes caused by vehicletypecode)
    return df_combined[['Mfr_ID', 'Mfr_Name', 'Mfr_CommonName', 'Country']].drop_duplicates()


@op
def fetch_mfr_id_list(context) -> list:
    """
    Fetch list of manufacturer ID from duckdb
    """
    with duckdb.connect(database=os.getenv("DUCKDB_PATH"), read_only=False) as con:
        df_mfr_ids = con.execute("SELECT distinct Mfr_ID FROM manufacturers").fetchdf()

    return df_mfr_ids['Mfr_ID'].tolist()


@op
def fetch_wmi_by_manufacturer_id(context, mfr_id_list: list) -> pd.DataFrame:
    """
    Obtain WMI codes by manufacturer using NHTSA's API
    """
    df_list = []
    for mfr_id in mfr_id_list:
        url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetWMIsForManufacturer/{str(mfr_id)}?format=csv'
        df = pd.read_csv(url)
        # Some WMI codes can "look" like int types and so we want to make sure they are explicitly defined as str
        # when concatenating the dataframes together.  Otherwise, will get a pyarrow error due to int/str confusion.
        # Relevant background: https://github.com/wesm/feather/issues/349
        df['wmi'] = df['wmi'].astype('str')
        df_list.append(df)
    df_concat = pd.concat(df_list, ignore_index=True)

    context.log.info(f"Number of (rows, columns): {df_concat.shape}")
    context.log.info(f"WMIs by mfrs (n=5):\n{df_concat.head()}")

    return df_concat


@op(
    # Since we don't want this op to execute until upstream op is done which doesn't return data, we need to
    # add this ins argument
    ins={"start": In(Nothing)}
)
def fetch_wmi_list(context) -> list:
    """
    Fetch a list of WMI codes from duckdb
    """
    with duckdb.connect(database=os.getenv("DUCKDB_PATH"), read_only=False) as con:
        df_wmi = con.execute("SELECT distinct wmi FROM wmi_by_mfr").fetchdf()

    return df_wmi['wmi'].tolist()


@op
def fetch_makes_from_wmi(context, wmi_list: list) -> pd.DataFrame:
    """
    Obtain make information by WMI codes from NHTSA's API
    """
    df_list = []
    for wmi in tqdm(wmi_list):
        try:
            context.log.info(f"Pulling data for wmi: {wmi}")
            url = f'https://vpic.nhtsa.dot.gov/api/vehicles/decodewmi/{wmi}?format=json'
            res = requests.get(url, timeout=10)
            df = pd.json_normalize(json.loads(res.text), record_path=['Results'])
            df = df.assign(WMI=wmi)
            df_list.append(df)
            context.log.info(f"wmi added: {wmi}")
        except HTTPError as err:
            if err.code == 404:
                context.log.debug(f'wmi code that failed: {wmi} - {repr(err)}')
                traceback.print_exc()
            else:
                raise
        except json.JSONDecodeError as err:
            context.log.debug(f'wmi code that failed: {wmi} - {repr(err)}')
        except requests.exceptions.Timeout:
            context.log.debug(f"Timeout error:  Timeout exceeded 10 seconds for wmi: {wmi}")

    df_concat = pd.concat(df_list, ignore_index=True)
    context.log.info(f"# of (rows, columns): {df_concat.shape}")
    context.log.info(f"Dataframe makes from wmi:\n{df_concat.head()}")

    return df_concat


@op
def fetch_makes(context) -> pd.DataFrame:
    """
    Obtain vehicle makes from NHTSA API
    """

    df = pd.read_csv('https://vpic.nhtsa.dot.gov/api/vehicles/GetAllMakes?format=csv')

    context.log.info(f"# of (rows, columns): {df.shape}")

    return df


@op
def fetch_make_id_list(context) -> list:
    """
    Fetch a list of make IDs from DuckDB
    """
    with duckdb.connect(database=os.getenv("DUCKDB_PATH"), read_only=False) as con:
        df_make_ids = con.execute("SELECT distinct make_id FROM makes").fetchdf()

    return df_make_ids['make_id'].tolist()


@op
def fetch_model_names(context, make_id_list: list) -> pd.DataFrame:
    """
    Obtain vehicle model names from NHTSA API (passenger cars, trucks, and motorcycles only, last 5 years)
    """

    # A good source for vehicle makes: https://cars.usnews.com/cars-trucks/car-brands-available-in-america

    # Instead of hard-coding that we want last 15 model years, we can programmatically define the years for us
    current_year = datetime.today().year
    start_year = datetime.today().year - 14

    df_list = []
    for year in range(start_year, current_year + 1):
        for make_id in make_id_list:
            for vehicle_type in ['passenger', 'truck']:
                url = f'https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeIdYear/makeId/{make_id}/modelyear/{year}/vehicletype/{vehicle_type}?format=csv'
                df = pd.read_csv(url)
                df = df.assign(year=year)
                # Some model_names can "look" like int types and so we want to make sure they are explicitly defined as str
                # when concatenating the dataframes together.  Otherwise, will get a pyarrow error due to int/str confusion.
                # Relevant background: https://github.com/wesm/feather/issues/349
                df['model_name'] = df['model_name'].astype('str')
                df_list.append(df)

    df_concat = pd.concat(df_list, ignore_index=True)
    context.log.info(f"# of (rows, columns): {df_concat.shape}")
    context.log.info(f"Sample of model names (n=5):\n{df_concat.head()}")

    return df_concat
