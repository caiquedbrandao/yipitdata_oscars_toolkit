from fetch_data import get_oscars_films_data
from clean_data import clean_dataset
from analyze_data import generate_dataset_refined

def main(exchange_rates_to_dolar):
    raw = get_oscars_films_data()
    trusted = clean_dataset(exchange_rates_to_dolar)
    refined = generate_dataset_refined()

if __name__ == '__main__':

    exchange_rates_to_dollar = dict()

    while len(exchange_rates_to_dollar) == 0:
        try:
            rate_euro_to_dollar = input("Rate EUR to US Dollar (e.g: 1.10): ")
            rate_euro_to_dollar = float(rate_euro_to_dollar.replace(',', '.'))
            rate_pound_sterling_to_dollar = input("Rate GBP To US Dollar (e.g: 1.30): ")
            rate_pound_sterling_to_dollar = float(rate_pound_sterling_to_dollar.replace(',', '.'))
            exchange_rates_to_dollar = {'EUR': rate_euro_to_dollar, 'GBP': rate_pound_sterling_to_dollar}
        except Exception as e:
            print(f'Incorrect parameter: {e}. Try again')

    main(exchange_rates_to_dollar)
