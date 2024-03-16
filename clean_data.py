import pandas as pd
import re
from utils import save_dataframe_to_csv


def convert_budget_to_usd(df, exchange_rates):

    def clean_budget(budget):

        budget = str(budget).upper()
        budget = budget.strip()

        if '] OR ' in budget:
            budget = '0'

        # Remove informações entre colchetes e espaços extras
        budget = re.sub(r'\[.*?\]', '', str(budget)).strip()

        range_pattern = re.compile(
            r'(\$?\d+(,\d{3})*(\.\d+)?)(–|-)(\$?\d+(,\d{3})*(\.\d+)?)(\s*\(?\[?\d*\]?\)?\s*MILLION)?'
        )
        if (range_pattern.search(budget) or '–$' in budget):
            budget = '0'

        elif 'RE-RELEASE' in budget:
            budget = f'{budget[0:4]}000000'

        elif ('£' not in budget and '₤' not in budget and '€' not in budget) and (
                'USD$' in budget or 'US$' in budget or budget[0] == '$'):

            if 'EST.' not in budget and 'ESTIMATED' not in budget:
                pass
            else:
                budget = budget.replace('EST.', '')
                budget = budget.replace('ESTIMATED', '')
                budget = re.sub(r'[^\d.,£€$]+', '', budget)
                budget = budget.replace('MILLION', '').strip()
                if ',' in budget:
                    budget = budget.replace(',', '')
                else:
                    zeros_necessarios = 6
                    budget = f"{budget}{'0' * zeros_necessarios}"

            if 'MILLION' not in budget and '(' not in budget and '–' not in budget:
                # Remover caracteres não numéricos exceto ponto e vírgula
                budget = re.sub(r'[^\d.,£€$]+', '', budget)
                budget = budget.replace(',', '')

            pesquisa = re.search(r'USD\$\s*\d+(\.\d+)?\s*MILLION', budget, re.IGNORECASE)
            if pesquisa:
                budget = pesquisa.group().replace(' ', '').replace('MILLION', '').strip()
                if '.' in budget:
                    parte_inteira, parte_decimal = budget.split('.')
                    zeros_necessarios = 6 - len(parte_decimal)
                    budget = f"{parte_inteira}{parte_decimal}{'0' * zeros_necessarios}"
                else:
                    budget = f"{budget.strip()}000000"

            if ('MILLION' in budget and '(' not in budget and '–' not in budget and '–' not in budget):
                budget = budget.replace(',', '')
                budget = budget.replace('MILLION', '').strip()
                if '.' in budget:
                    parte_inteira, parte_decimal = budget.split('.')
                    zeros_necessarios = 6 - len(parte_decimal)
                    budget = f"{parte_inteira}{parte_decimal}{'0' * zeros_necessarios}"
                else:
                    budget = f"{budget.strip()}000000"


            budget = re.sub(r'[^\d£€$]+', '', budget)

        elif ('£' in budget or '₤' in budget or '€' in budget):

            if 'EST.' in budget:
                budget = budget.replace('EST.', '')

            pesquisa = re.search(r'\$\s*\d+(\.\d+)?\s*MILLION', budget, re.IGNORECASE)
            if pesquisa:
                valor = pesquisa.group().replace(' ', '').replace(
                    'MILLION', ''
                ).replace('$', '').strip()
                if '.' in valor:
                    parte_inteira, parte_decimal = valor.split('.')
                    # Calcula quantos zeros são necessários após a parte decimal
                    zeros_necessarios = 6 - len(parte_decimal)
                    budget = f"${parte_inteira}{parte_decimal}{'0' * zeros_necessarios}"
                else:
                    budget = f"${valor}000000"

            if ('MILLION' in budget and '(' not in budget and '–' not in budget):
                budget = budget.replace(',', '')
                budget = budget.replace('MILLION', '').strip()
                if '.' in budget:
                    budget = f'{budget}00000'
                    budget = budget.replace('.', '')
                else:
                    budget = f'{budget}000000'

            budget = budget.replace(',', '')
            budget = re.sub(r'[^\d£₤€$]+', '', budget)
            budget = budget.replace('₤', '£')

        budget = budget.replace('NAN', '0')

        return budget


    # Apply the cleaning function to the 'budget' column
    df['original_budget_cleaned'] = df['budget'].apply(clean_budget)
    df['original_budget_integer'] = df['original_budget_cleaned'].str.replace(
        '$', '', regex=False).str.replace('£', '', regex=False)

    def convert_currency(original_budget_cleaned):
        budget_dollar = 0
        try:
            if '$' in original_budget_cleaned:
                budget_dollar = float(original_budget_cleaned.replace('$', '').strip())

            if '£' in original_budget_cleaned:
                budget_dollar = float(original_budget_cleaned.replace('£', '').strip()) * float(exchange_rates['GBP'])


        except Exception as e:
            print('ERRO CONVERSÃO: ', e)
            pass

        return budget_dollar

    # Apply the convert currency function to the 'original_budget_cleaned' column
    df['budget_usd'] = df['original_budget_cleaned'].apply(convert_currency)

    return df


def clean_dataset(exchange_rates_to_dolar):

    execution_log = dict()
    execution_log['erros'] = list()

    try:
        df = pd.read_csv(
            'datasets/oscar_nominated_films_raw.csv', sep=";", encoding='utf-8', low_memory=False
        )

        # df = df.loc[df['film'] == 'Cleopatra']

        string_columns = [
            'film', 'detail_url', 'producer(s)', 'production_company(s)',
            'production_company', 'release_dates', 'running_time',  'wiki_url',
            'cinematography', 'country', 'directed_by', 'distributed_by', 'edited_by', 'language', 'music_by',
            'produced_by', 'screenplay_by', 'starring', 'story_by', 'title', 'written_by', 'based_on',
            'narrated_by', 'production_companies'

        ]
        for column in string_columns:
            df[column] = df[column].astype(str).str.strip()
            df[column] = df[column].str.replace('nan', 'not_found')
            df[column] = df[column].str.replace(r'\[.*?\]', '', regex=True).str.strip()

        df['year'] = df['year'].str.strip().str[:4].astype(int)

        df = df.drop('id_json', axis=1)
        df = df.rename(
            columns={
                'winner': 'oscar_winner'
            }
        )

        df = convert_budget_to_usd(df, exchange_rates_to_dolar)

        save_dataframe_to_csv(
            df, "datasets/oscar_nominated_films_trusted.csv"
        )

        if len(df):
            execution_log['status'] = 'success'
            execution_log['records'] = len(df)

    except Exception as e:
        execution_log['erros'].append(e)
        pass

    print(execution_log)

    return execution_log
