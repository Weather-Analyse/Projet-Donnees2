import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import numpy as np
import boto3
import os

def televerser_vers_s3(chemin_fichier, nom_bucket, nom_objet=None):
    s3_client = boto3.client('s3')
    if nom_objet is None:
        nom_objet = os.path.basename(chemin_fichier)
    try:
        s3_client.upload_file(chemin_fichier, nom_bucket, nom_objet)
        print(f"Téléversement réussi de {chemin_fichier} vers {nom_bucket}/{nom_objet}")
    except Exception as e:
        print(f"Échec du téléversement de {chemin_fichier} vers S3 : {e}")

def analyze_data():
    # Charger les données traitées
    df = pd.read_csv('/home/ubuntu/airflow/dags/Processed_Data.csv')
    
    # Vérifiez les colonnes du DataFrame
    print("Colonnes disponibles dans le DataFrame:")
    print(df.columns.tolist())
    
    # Vérifiez et renommez les colonnes pour la cohérence
    df.columns = df.columns.str.strip()  # Enlever les espaces superflus
    df.rename(columns={
        'Density (people/km²)': 'Densité (personnes/km²)',
        'Urbanization (%)': 'Urbanisation (%)',
        'Average Income (USD)': 'Revenu Moyen (USD)',
        'Education Level (% with Bachelor\'s or higher)': 'Niveau d Éducation (% avec Licence ou plus)',
        'Altitude (m)': 'Altitude (m)',
        'Proximity to Industry (km)': 'Proximité de l Industrie (km)',
        'AQI': 'Indice de Qualité de l\'Air',  # Renommer la colonne AQI
        'CO': 'co',
        'NO': 'no',
        'NO2': 'no2',
        'O3': 'o3',
        'SO2': 'so2',
        'PM2.5': 'pm2_5',
        'PM10': 'pm10'
    }, inplace=True)

    # Convertir les colonnes non numériques en NaN et les supprimer
    numeric_features = ['Population', 'Densité (personnes/km²)', 'Urbanisation (%)', 'Revenu Moyen (USD)', 
                        'Niveau d Éducation (% avec Licence ou plus)', 'Altitude (m)', 'Proximité de l Industrie (km)',
                        'co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'Indice de Qualité de l\'Air']
    
    # Convertir les colonnes en numérique et supprimer les lignes avec NaN
    df[numeric_features] = df[numeric_features].apply(pd.to_numeric, errors='coerce')
    df.dropna(subset=numeric_features, inplace=True)
    
    chemins_images = []

    # Analyse de corrélation
    correlation_matrix = df[numeric_features].corr()
    plt.figure(figsize=(14, 12))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Carte de Chaleur des Corrélations entre Pollution de l\'Air et Facteurs Démographiques/Géographiques')
    plt.tight_layout()
    chemin_correlation = '/home/ubuntu/airflow/dags/carte_correlation.png'
    plt.savefig(chemin_correlation)
    plt.close()
    chemins_images.append(chemin_correlation)
    
    # Analyse ACP
    X = df[numeric_features]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    pca = PCA()
    pca.fit(X_scaled)
    
    # Visualisation ACP
    explained_variance_ratio = pca.explained_variance_ratio_
    cumulative_variance_ratio = np.cumsum(explained_variance_ratio)
    
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, len(cumulative_variance_ratio) + 1), cumulative_variance_ratio, 'bo-')
    plt.xlabel('Nombre de Composantes')
    plt.ylabel('Ratio Cumulatif de Variance Expliquée')
    plt.title('ACP : Ratio Cumulatif de Variance Expliquée')
    plt.grid(True)
    chemin_acp = '/home/ubuntu/airflow/dags/analyse_acp.png'
    plt.savefig(chemin_acp)
    plt.close()
    chemins_images.append(chemin_acp)
    
    # Graphique à barres des niveaux moyens de polluants par location
    polluants = ['co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10']
    avg_polluants = df.groupby('Location')[polluants].mean()
    
    plt.figure(figsize=(14, 7))
    avg_polluants.plot(kind='bar', stacked=True)
    plt.title('Niveaux Moyens de Polluants par Location')
    plt.xlabel('Location')
    plt.ylabel('Concentration (μg/m³)')
    plt.legend(title='Polluants', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    chemin_polluants = '/home/ubuntu/airflow/dags/niveaux_moyens_polluants_par_location.png'
    plt.savefig(chemin_polluants)
    plt.close()
    chemins_images.append(chemin_polluants)
    
    # Nuage de points : Population vs Niveaux de PM2.5
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='Population', y='pm2_5', hue='Location', size='Urbanisation (%)', sizes=(50, 200))
    plt.title('Population vs Niveaux de PM2.5')
    plt.xlabel('Population')
    plt.ylabel('Concentration de PM2.5 (μg/m³)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    chemin_population_vs_pm25 = '/home/ubuntu/airflow/dags/population_vs_pm25.png'
    plt.savefig(chemin_population_vs_pm25)
    plt.close()
    chemins_images.append(chemin_population_vs_pm25)
    
    # Boîte à moustaches des niveaux de polluants
    plt.figure(figsize=(14, 7))
    df_melt = df.melt(id_vars=['Location'], value_vars=polluants, var_name='Polluant', value_name='Concentration')
    sns.boxplot(x='Polluant', y='Concentration', data=df_melt)
    plt.title('Distribution des Niveaux de Polluants')
    plt.xticks(rotation=45)
    plt.tight_layout()
    chemin_distribution_polluants = '/home/ubuntu/airflow/dags/distribution_polluants.png'
    plt.savefig(chemin_distribution_polluants)
    plt.close()
    chemins_images.append(chemin_distribution_polluants)
    
    # Nuage de points : Altitude vs Niveaux de PM2.5
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='Altitude (m)', y='pm2_5', hue='Location', size='Proximité de l Industrie (km)', sizes=(50, 200))
    plt.title('Altitude vs Niveaux de PM2.5')
    plt.xlabel('Altitude (m)')
    plt.ylabel('Concentration de PM2.5 (μg/m³)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    chemin_altitude_vs_pm25 = '/home/ubuntu/airflow/dags/altitude_vs_pm25.png'
    plt.savefig(chemin_altitude_vs_pm25)
    plt.close()
    chemins_images.append(chemin_altitude_vs_pm25)

    # Graphique des AQI par location
    plt.figure(figsize=(14, 7))
    aqi_df = df.groupby('Location')['Indice de Qualité de l\'Air'].mean()
    aqi_df.plot(kind='bar', color='skyblue')
    plt.title('Indice de Qualité de l\'Air (AQI) par Location')
    plt.xlabel('Location')
    plt.ylabel('AQI')
    plt.tight_layout()
    chemin_aqi_par_location = '/home/ubuntu/airflow/dags/aqi_par_location.png'
    plt.savefig(chemin_aqi_par_location)
    plt.close()
    chemins_images.append(chemin_aqi_par_location)
    
    # Téléverser toutes les images dans S3
    nom_bucket = 'examdonnees2peplinebucket-yml'
    for chemin_image in chemins_images:
        televerser_vers_s3(chemin_image, nom_bucket)

analyze_data()
