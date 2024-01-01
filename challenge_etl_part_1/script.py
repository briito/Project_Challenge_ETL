import boto3

s3_client = boto3.client(
    's3',
    aws_access_key_id='EXAMPLE',
    aws_secret_access_key='EXAMPLE',
    region_name='us-east-1'
)

movies_file = 'movies.csv'
series_file = 'series.csv'

movies_s3_key = 'RAW/LOCAL/CSV/MOVIES/2023/09/movies.csv'
series_s3_key = 'RAW/LOCAL/CSV/SERIES/2023/09/series.csv'

bucket_name = 'raw-briito'


def upload_to_s3(file_name, s3_key):
    try:
        s3_client.upload_file(file_name, bucket_name, s3_key)
        print(f'{file_name} foi carregado com sucesso para o S3 em {s3_key}')
    except Exception as e:
        print(f'Erro ao carregar {file_name} para o S3: {str(e)}')


upload_to_s3(movies_file, movies_s3_key)
upload_to_s3(series_file, series_s3_key)
