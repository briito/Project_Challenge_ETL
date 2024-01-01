import boto3
import pandas as pd
import io


def lambda_handler(event, context):
    bucket_name = 'raw-briito'
    input_s3_key = 'RAW/LOCAL/CSV/SERIES/2023/09/series.csv'
    output_s3_key = 'RAW/LOCAL/CSV/PROCESSED/filtered_series.csv'

    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=bucket_name, Key=input_s3_key)
        data = response['Body'].read()

        file_like_object = io.BytesIO(data)

        df = pd.read_csv(file_like_object, sep='|')

        filtered_df = df[df['genero'].str.contains(
            'Drama|Romance', case=False, na=False)]

        filtered_data_csv = filtered_df.to_csv(index=False, sep='|')

        s3.put_object(Bucket=bucket_name, Key=output_s3_key,
                      Body=filtered_data_csv)

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': str(e)
        }

    return {
        'statusCode': 200,
        'body': f'Dados gravados com sucesso no bucket {bucket_name}'
    }
