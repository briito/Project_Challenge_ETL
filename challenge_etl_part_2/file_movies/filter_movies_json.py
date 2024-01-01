import requests
import pandas as pd
import boto3
import io
import os
import json
import time


def get_movie_ids():
    bucket_name = 'raw-briito'
    input_s3_key = 'RAW/LOCAL/CSV/PROCESSED/filtered_movies.csv'

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=input_s3_key)
    data = response['Body'].read()

    df = pd.read_csv(io.BytesIO(data), sep='|')
    movie_ids = df['id'].unique()

    return movie_ids


def fetch_movie_data(api_key, movie_ids):
    bucket_name = 'raw-briito'
    s3_prefix = 'RAW/TMDB/JSON/2023/10/MOVIES'
    s3_client = boto3.client('s3')

    movies_per_file = 100
    current_movie_data = []
    file_number = 1

    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            movie = response.json()

            movie_data = {
                "adult": movie.get('adult'),
                "backdrop_path": movie.get('backdrop_path'),
                "belongs_to_collection": movie.get('belongs_to_collection'),
                "budget": movie.get('budget'),
                "genres": movie.get('genres'),
                "homepage": movie.get('homepage'),
                "id": movie.get('id'),
                "imdb_id": movie.get('imdb_id'),
                "original_language": movie.get('original_language'),
                "original_title": movie.get('original_title'),
                "popularity": movie.get('popularity'),
                "poster_path": movie.get('poster_path'),
                "production_companies": movie.get('production_companies'),
                "production_countries": movie.get('production_countries'),
                "release_date": movie.get('release_date'),
                "revenue": movie.get('revenue'),
                "runtime": movie.get('runtime'),
                "spoken_languages": movie.get('spoken_languages'),
                "status": movie.get('status'),
                "tagline": movie.get('tagline'),
                "title": movie.get('title'),
                "video": movie.get('video'),
                "vote_average": movie.get('vote_average'),
                "vote_count": movie.get('vote_count')
            }

            current_movie_data.append(movie_data)

            if len(current_movie_data) >= movies_per_file:
                s3_key = f'{s3_prefix}/{file_number:03d}_filtered_movies.json'
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(current_movie_data),
                )
                current_movie_data = []
                file_number += 1

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a solicitação à API: {str(e)}")
        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")

    if current_movie_data:
        s3_key = f'{s3_prefix}/{file_number:03d}_filtered_movies.json'
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(current_movie_data),
        )


def lambda_handler(event, context):
    api_key = os.environ['TMDB_KEY']

    my_ids = get_movie_ids()
    fetch_movie_data(api_key, my_ids)

    return {
        'statusCode': 200,
        'body': 'Dados gravados com sucesso em arquivos divididos no bucket raw-briito'
    }

