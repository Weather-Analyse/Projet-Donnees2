import pandas as pd
import requests

def collect_air_data():
    api_key = 'd2d43ea8d690ccf7561cb98d3afa3903'
    base_url = 'http://api.openweathermap.org/data/2.5/air_pollution'
    
    locations = {
        'Los Angeles': {'lat': 34.0522, 'lon': -118.2437},
        'Paris': {'lat': 48.8566, 'lon': 2.3522},
        'Tokyo': {'lat': 35.6762, 'lon': 139.6503},
        'Antananarivo': {'lat': -18.8792, 'lon': 47.5079},
        'Nairobi': {'lat': -1.2921, 'lon': 36.8219},
        'Lima': {'lat': -12.0464, 'lon': -77.0428},
    }
    
    air_data = {}
    
    for city, coords in locations.items():
        params = {
            'lat': coords['lat'],
            'lon': coords['lon'],
            'appid': api_key
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  
            data = response.json()
            components = data['list'][0]['components']
            aqi = data['list'][0]['main']['aqi']
            air_data[city] = {
                'AQI': aqi,
                'CO': components.get('co', None),
                'NO': components.get('no', None),
                'NO2': components.get('no2', None),
                'O3': components.get('o3', None),
                'SO2': components.get('so2', None),
                'PM2.5': components.get('pm2_5', None),
                'PM10': components.get('pm10', None)
            }
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve data for {city}: {e}")
        except (KeyError, IndexError) as e:
            print(f"Error processing data for {city}: {e}")
    
    air_df = pd.DataFrame.from_dict(air_data, orient='index')
    air_df.to_csv('/home/ubuntu/airflow/dags/Air_pollution_Data.csv', index_label='City')

