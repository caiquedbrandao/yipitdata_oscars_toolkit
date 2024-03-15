import os
import pandas as pd


def remove_line_breaks(value):
    if isinstance(value, str):
        return value.replace('\n', ' ')
    else:
        return value


def convert_jsons_to_dataframe(jsons):
    df = pd.DataFrame()
    for idx, json_data in enumerate(jsons):
        df_temp = pd.json_normalize(json_data)
        df_temp['id_json'] = idx
        try:
            df_temp = df_temp.applymap(remove_line_breaks)
        except Exception as e:
            print(e)
            pass
        df = pd.concat([df, df_temp], ignore_index=True)
    return df


def save_dataframe_to_csv(dataframe, file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    dataframe.to_csv(file_path, index=False, sep=";", encoding='utf-8')
    return True


def read_csv(diretorio, caminho_arquivo):
    caminho_arquivo = os.path.join(diretorio, caminho_arquivo)
    df = pd.read_csv(caminho_arquivo, sep=";", encoding='utf-8', low_memory=False)
    return df
