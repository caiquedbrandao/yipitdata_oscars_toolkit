import pandas as pd
from utils import save_dataframe_to_csv

def generate_dataset_refined():

    execution_log = dict()
    execution_log['erros'] = list()

    try:
        df = pd.read_csv(
            'datasets/oscar_nominated_films_trusted.csv', sep=";", encoding='utf-8', low_memory=False
        )

        # release_date
        pattern = r'(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s+\d{4}|\w+\s+\d{4})'
        df['release_date'] = df['release_dates'].str.extract(pattern)
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

        # running_time
        df['running_time_minutes'] = df['running_time'].str.extract(r'(\d+)\s*min?', expand=False).str.strip()

        final_columns = [
            'year', 'film', 'producer(s)', 'production_company(s)', 'wiki_url', 'oscar_winner',
            'production_company', 'release_dates', 'release_date', 'running_time', 'running_time_minutes',
            'cinematography', 'country', 'directed_by', 'distributed_by', 'edited_by',
            'language', 'music_by', 'produced_by', 'screenplay_by', 'starring', 'story_by',
            'title', 'written_by', 'based_on', 'box_office', 'narrated_by', 'production_companies',
            'budget', 'original_budget_cleaned', 'original_budget_integer', 'budget_usd'
        ]

        df = df[final_columns]

        save_dataframe_to_csv(
            df, "datasets/oscar_nominated_films_refined.csv"
        )
        if len(df) > 0:
            execution_log['status'] = 'success'
            execution_log['records'] = len(df)

    except Exception as e:
        execution_log['erros'].append(e)
        pass

    print(execution_log)

    return execution_log
