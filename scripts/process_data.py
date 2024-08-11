import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def process_data():
    # Définir les chemins de fichiers en tant que chaînes de caractères
    demo_file = '/home/ubuntu/airflow/dags/Demographic_Data.csv'
    geo_file = '/home/ubuntu/airflow/dags/Geographic_Data.csv'
    air_file = '/home/ubuntu/airflow/dags/Air_pollution_Data.csv'
    
    # Charger les données
    try:
        demo_df = pd.read_csv(demo_file)
        geo_df = pd.read_csv(geo_file)
        air_df = pd.read_csv(air_file)
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        return
    
    # Concaténation horizontale des DataFrames
    combined_df = pd.concat([demo_df, geo_df, air_df], axis=1)
    
    # Supprimer les colonnes dupliquées
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    
    # Supprimer la colonne 'City'
    combined_df.drop(columns=['City'], inplace=True)
    
    # Enregistrer les données traitées localement
    local_path = '/home/ubuntu/airflow/dags/Processed_Data.csv'
    combined_df.to_csv(local_path, index=False)
    print(f"Processed data saved to {local_path}")

    # Téléverser le fichier vers S3
    s3_bucket_name = 'examdonnees2peplinebucket-yml'  # Remplacez par le nom de votre bucket
    s3_file_path = 'Processed_Data.csv'      # Le chemin dans le bucket S3
    
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(local_path, s3_bucket_name, s3_file_path)
        print(f"File uploaded to s3://{s3_bucket_name}/{s3_file_path}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
    except Exception as e:
        print(f"Failed to upload file: {e}")

process_data()
