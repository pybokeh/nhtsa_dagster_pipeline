from dagster import configured, job, repository
from ops.nhtsa_ops import (
    fetch_manufacturers,
    fetch_mfr_id_list,
    fetch_makes,
    fetch_make_id_list,
    fetch_model_names,
    fetch_wmi_by_manufacturer_id,
    fetch_wmi_list,
    fetch_makes_from_wmi,
    upload_df_to_duckdb
)


upload_mfrs_to_duckdb = configured(
    upload_df_to_duckdb,
    name='upload_mfrs_to_duckdb'
)(
    {'table_name': 'manufacturers'}
)


upload_makes_to_duckdb = configured(
    upload_df_to_duckdb,
    name='upload_makes_to_duckdb'
)(
    {'table_name': 'makes'}
)


upload_wmi_by_mfr_to_duckdb = configured(
    upload_df_to_duckdb,
    name='upload_wmi_by_mfr_to_duckdb'
)(
    {'table_name': 'wmi_by_mfr'}
)


upload_wmi_by_make_to_duckdb = configured(
    upload_df_to_duckdb,
    name='upload_wmi_by_make_to_duckdb'
)(
    {'table_name': 'wmi_by_make'}
)


upload_models_to_duckdb = configured(
    upload_df_to_duckdb,
    name='upload_models_to_duckdb'
)(
    {'table_name': 'models'}
)


@job
def fetch_1_mfr_make_job():
    """
    Job to extract manufacturer and makes information from NHTSA API and save them as Snowflake tables
    """

    upload_mfrs_to_duckdb(fetch_manufacturers())
    upload_makes_to_duckdb(fetch_makes())


@job
def fetch_2_wmi_job():
    """
    Job to extract WMI (World Manufacturer Identifier) codes from NHTSA's API
    """
    done = upload_wmi_by_mfr_to_duckdb(fetch_wmi_by_manufacturer_id(fetch_mfr_id_list()))
    upload_wmi_by_make_to_duckdb(fetch_makes_from_wmi(fetch_wmi_list(start=done)))


@job
def fetch_3_model_name_job():
    """
    Job to extract model names from NHTSA's API
    """
    upload_models_to_duckdb(fetch_model_names(fetch_make_id_list()))


@repository
def nhtsa_repo():
    return [
        fetch_1_mfr_make_job,
        fetch_2_wmi_job,
        fetch_3_model_name_job
    ]
