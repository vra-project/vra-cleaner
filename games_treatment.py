'''
Programa utilizado para obtener la informacion del dataset de juegos, una vez
limpio
'''

# %%
# Se cargan las librerías necesarias para realizar este proceso

import ast
import pandas as pd
import numpy as np

# %%
# Se define la funcion que ayudara en la limpieza de los datos


def get_from_dict(value):
    '''
    Dado que mucha informacion viene dada en diccionarios, se usa esta funcion
    para obtener la parte importante
    '''
    if value == []:
        return []
    return [field['name'] for field in value]


def get_dev_function(value):
    '''
    A diferencia de la funcion anterior, en este caso se obtendra el nombre y
    cargo de cada uno de los desarrolladores del juego
    '''
    if value == []:
        return {}
    return {field['Name']: ', '.join(field['Position']) for field in value}


# %%
# Se define la funcion que usara la ETL


def g_treatment(clean_df, games_df):
    '''
    Transforma la inforamcion a un formato mas comodo para leer
    '''
    cols = games_df.columns.tolist()
    fused_df = (
        clean_df[clean_df.columns[:10]]
        .merge(
            games_df[
                [cols[1]] + cols[3:5] + cols[7:11] + cols[26:30] + [cols[43]]
                + [cols[49]]
                ],
            left_on='game_id',
            right_on='RAWG_link',
            suffixes=('', '_')
            )
        .drop_duplicates('name')
        .drop(['RAWG_link', 'game_id'], axis=1)
        .replace(['nan'], np.NaN)
        )
    fused_df.drop(
        [col for col in fused_df.columns if col.endswith('_')],
        axis=1,
        inplace=True
        )

    for col in fused_df.columns[16:]:
        fused_df[col] = fused_df[col].fillna('[]').map(ast.literal_eval)

    for col in fused_df.columns[16:20]:
        fused_df[col] = fused_df[col].map(get_from_dict)
    fused_df['advanced_devs'] = fused_df['advanced_devs'].map(get_dev_function)
        
    return fused_df