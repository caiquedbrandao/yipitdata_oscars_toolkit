import os
import requests
import pandas as pd
from urllib.parse import unquote
from utils import convert_jsons_to_dataframe, save_dataframe_to_csv


def get_film_details(url):
    try:
        response = requests.get(url)
        details = response.json()
        return details
    except Exception as e:
        print(f"Error during request to get film details: {e}")
        return {}


def get_oscars_films_data():

    url_api = "http://oscars.yipitdata.com/"
    processed_data = list()
    film_counter = 0
    execution_log = dict()
    execution_log['erros'] = list()

    try:
        response = requests.get(url_api)
        data = response.json()
        data_results = data['results']

        print('*' * 100)
        print('OSCARS FILMS - 1927 to 2014')
        print('*' * 100)

        for result in data_results:
            year = result['year']
            for film in result['films']:
                film_counter += 1
                print(f'Film {film_counter}: ', film['Film'])
                film_data = {
                    'Year': year,
                    'Film': film['Film'],
                    'Detail URL': film['Detail URL'],
                    'Producer(s)': film['Producer(s)'],
                    'Production Company(s)': film['Production Company(s)'],
                    'Wiki URL': film['Wiki URL'],
                    'Winner': film['Winner']
                }

                try:
                    detail_url = film['Detail URL']
                    details = get_film_details(detail_url)
                    for key, value in details.items():
                        clean_key = key.strip()
                        film_data[clean_key] = value
                    processed_data.append(film_data)
                    film_data['log'] = None

                except Exception as e:
                    print('Error: ', e)
                    film_data['log'] = 'Error trying to get films details, url blocked'
                    pass

                if film_counter % 10 == 0:
                    # Save csv file
                    df = convert_jsons_to_dataframe(processed_data)
                    df.columns = df.columns.str.lower().str.replace(' ', '_')
                    save_dataframe_to_csv(
                        df, "datasets/oscar_nominated_films_raw.csv"
                    )

        # Save csv file
        df = convert_jsons_to_dataframe(processed_data)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        save_dataframe_to_csv(
            df, "datasets/oscar_nominated_films_raw.csv"
        )
        if len(df) > 0:
            execution_log = {
                'status': 'success',
                'records': len(df)
            }


    except Exception as e:
        execution_log['erros'].append(e)
        pass

    print(execution_log)

    return execution_log
