# Pipeline d'Analyse de la Pollution de l'Air

Ce projet est un pipeline d'analyse de la pollution de l'air, qui permet de collecter, traiter et analyser des données de pollution atmosphérique. Le pipeline est orchestré à l'aide d'Apache Airflow.

## Prérequis

Avant de commencer, assurez-vous de disposer d'un environnement Linux pour l'installation d'Apache Airflow. Si vous êtes sur Windows, des étapes spécifiques sont également disponibles pour configurer l'environnement.

### Installation d'Apache Airflow

#### Sur Linux

Veuillez suivre les instructions d'installation d'Apache Airflow pour Linux disponibles ici : [Installation Apache Airflow sur Linux](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

#### Sur Windows

Si vous êtes sur Windows, suivez ce guide pour installer Apache Airflow : [Installation Apache Airflow sur Windows](https://vivekjadhavr.medium.com/how-to-easily-install-apache-airflow-on-windows-6f041c9c80d2)

## Configuration de l'Environnement

1. Après avoir installé Airflow, activez l'environnement virtuel :
    ```bash
    source airflow-env/bin/activate
    ```

2. Installez les dépendances nécessaires :
    ```bash
    pip install pandas matplotlib seaborn scikit-learn numpy boto3 requests
    ```

## Organisation des Fichiers

Placez tous les dossiers et fichiers du projet dans le répertoire suivant :
```bash
airflow/dags
