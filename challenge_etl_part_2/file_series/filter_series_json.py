import requests
import pandas as pd
import boto3
import io
import os
import json
import time


def get_serie_ids():
    bucket_name = 'raw-briito'
    input_s3_key = 'RAW/LOCAL/CSV/PROCESSED/filtered_series.csv'

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=input_s3_key)
    data = response['Body'].read()

    df = pd.read_csv(io.BytesIO(data), sep='|')
    serie_ids = df['id'].unique()

    return serie_ids


def fetch_series_data(api_key, serie_ids):
    bucket_name = 'raw-briito'
    s3_prefix = 'RAW/TMDB/JSON/2023/10/SERIES'
    s3_client = boto3.client('s3')

    series_per_file = 100
    current_serie_data = []
    file_number = 1

    for serie_id in serie_ids:
        url = f"https://api.themoviedb.org/3/find/{serie_id}?api_key={api_key}&external_source=imdb_id"

        try:
            response = requests.get(url)
            response.raise_for_status()
            serie = response.json()

            serie_data = {
                "tv_results": serie.get('tv_results'),
                "tv_episode_results": serie.get('tv_episode_results'),
                "tv_season_results": serie.get('tv_season_results'),
            }

            current_serie_data.append(serie_data)

            if len(current_serie_data) >= series_per_file:

                s3_key = f'{s3_prefix}/{file_number:03d}_filtered_series.json'
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(current_serie_data)
                )
                current_serie_data = []
                file_number += 1

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f"Erro ao fazer a solicitação à API: {str(e)}")
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f"Ocorreu um erro: {str(e)}")
            }

    if current_serie_data:
        s3_key = f'{s3_prefix}/{file_number:03d}_filtered_series.json'
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(current_serie_data),
        )


def lambda_handler(event, context):
    api_key = os.environ['TMDB_KEY']

    my_ids = get_serie_ids()
    fetch_series_data(api_key, my_ids)

    return {
        'statusCode': 200,
        'body': 'Dados gravados com sucesso em arquivos divididos no bucket raw-briito'
    }
