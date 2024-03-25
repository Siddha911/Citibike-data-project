'-----------------------------------------------------------------------------------------------------------------------------------'
"""Data extracting from API block"""
import io
import pandas as pd
import requests
import zipfile
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def extract_zip_file(zip_url, file_name):
    """
    Extract the specified file from a ZIP archive and return its contents as a bytes object.
    """
    response = requests.get(zip_url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    file_content = zip_file.read(file_name)
    return file_content


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Loading data for the second half of 2023.
    """
    dataframes = []
    
    for month in range(6, 13):
        url = f'https://s3.amazonaws.com/tripdata/JC-2023{month:02}-citibike-tripdata.csv.zip'  # set the url template
        zip_file_name = f'JC-2023{month:02}-citibike-tripdata.csv'  # set the file name template
        file_content = extract_zip_file(url, zip_file_name) 
        file_object = pd.read_csv(io.StringIO(file_content.decode()), sep=',')
        dataframes.append(file_object)
        
    df = pd.concat(dataframes)
    df["ride_id"] = df["ride_id"].astype('str')
    df["rideable_type"] = df["rideable_type"].astype('str')
    df["started_at"] = df["started_at"].astype('datetime64')
    df["ended_at"] = df["ended_at"].astype('datetime64')
    df["start_station_name"] = df["start_station_name"].astype('str')
    df["start_station_id"] = df["start_station_id"].astype('str')
    df["end_station_name"] = df["end_station_name"].astype('str')
    df["end_station_id"] = df["end_station_id"].astype('str')
    df["start_lat"] = df["start_lat"].astype('float')
    df["start_lng"] = df["start_lng"].astype('float')
    df["end_lat"] = df["end_lat"].astype('float')
    df["end_lng"] = df["end_lng"].astype('float')
    df["member_casual"] = df["member_casual"].astype('str')
    return df


@test
def test_output(output, *args) -> None:
    """
    Code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
  
'-----------------------------------------------------------------------------------------------------------------------------------'
"""Data transforming block"""
from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Drop the dupclicate rows if exists.
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.DROP_DUPLICATE,
        arguments=df.columns,  # Specify column names to use when comparing duplicates
        axis=Axis.ROW,
        options={'keep': 'first'},  # Specify whether to keep 'first' or 'last' duplicate
    )

    return BaseAction(action).execute(df)


@test
def test_output(output, *args) -> None:
    """
    Code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
  
'-----------------------------------------------------------------------------------------------------------------------------------'
"""Data loading to GCS block"""
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Exporting data to a Google Cloud Storage bucket.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'citibike_data_project'
    object_key = '2023_06-12_citibike_trips.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
  
'-----------------------------------------------------------------------------------------------------------------------------------'
"""Data extracting from GCS block"""
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Extracting data from a Google Cloud Storage bucket.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'citibike_data_project'
    object_key = '2023_06-12_citibike_trips.parquet'

    return GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )


@test
def test_output(output, *args) -> None:
    """
    Code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

'-----------------------------------------------------------------------------------------------------------------------------------'
"""Data loading to BigQuery block"""
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Loading data to a BigQuery warehouse.
    """
    table_id = 'dezoomcamp-409909.citibike_data.bike_rides_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
  
'-----------------------------------------------------------------------------------------------------------------------------------'
