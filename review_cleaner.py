'''
Programa utilizado para limpiar las reviews obtenidas de la pagina RAWG.io
'''

# %%
# Se cargan las librerias necesarias

from math import ceil
import pandas as pd

# %%
# Se definen las constantes
FOLDER = 'reviews/'
N_REVIEWS = 50000

# %%
# Se crea una función para dar un nombre único a cada juego

def get_id(name, year, platforms, n_count, n_count_2):
    '''
    Se crea un nombre único para los juegos con nombre repetido
    '''
    if n_count == 1:
        return name
    if n_count_2 > 1:
        return f'{name} ({year}) - {(", ").join(platforms)}'
    return f'{name} ({year})'

# %%
# Se define la funcion que se usara para limpiar reviews y juegos


def r_cleaner(games_df, reviews_df):
    '''
    Se define la funcion utilizada para limpiar las reviews y los juegos
    existentes
    '''

    reviews_df['id'] = reviews_df['id'].astype(int)
    reviews_df['review_rating'] = reviews_df['review_rating'].astype(int)

    # Se agrupan los usuarios segun la media de sus valoraciones, las reviews
    # totales y el numero de reviews con cada nota distinta
    # Permaneceran los usuarios con 5 o mas reviews y que tengan, como minimo,
    # una valoracion con cada valor (1, 3, 4, 5) para evitar usuarios con solo
    # reviews positivas o negativas

    print('Se obtienen los usuarios validos')
    users_df = (
        reviews_df
        .groupby('user_id', as_index=False)
        ['review_rating']
        .agg({
            'count': 'count',
            '1': lambda x: (x == 1).sum(),
            '3': lambda x: (x == 3).sum(),
            '4': lambda x: (x == 4).sum(),
            '5': lambda x: (x == 5).sum()
            })
        .loc[lambda df: df['count'] > 4]
        )

    for num in [str(n) for n in range(1, 6) if n != 2]:
        users_df = users_df.loc[users_df[num] > 0]

    # Se limpian las reviews permaneciendo las de usuarios validos
    reviews_df = reviews_df.merge(users_df[['user_id']], on='user_id')

    # Se eliminan las reviews de juegos que no esten disponibles en el dataset
    print('Se limpian las reviews de juegos inexistentes')

    reviews_df = (
        reviews_df
        .merge(
            games_df[['RAWG_link']],
            left_on='game_id',
            right_on='RAWG_link'
            )
        .drop('RAWG_link', axis=1)
        .drop_duplicates('id')
        .sort_values('id')
        .reset_index(drop=True)
        )

    # Se realiza la misma limpieza, pero con los juegos con review
    print('Se limpian los juegos sin un minimo de reviews validas')
    games_reviews_df = (
        reviews_df
        .groupby('game_id', as_index=False)
        ['review_rating']
        .agg({
            'RAWG_rating': 'mean',
            'RAWG_nreviews': 'count'
            })
        .loc[lambda df: df['RAWG_nreviews'] > 5]
        .assign(RAWG_rating=lambda df: df['RAWG_rating'].round(2))
        )

    # Se limpia el dataset usando los juegos con varias reviews
    reviews_df = (
        reviews_df
        .merge(games_reviews_df[['game_id']], on='game_id')
        .sort_values('id')
        )

    # Para limpiar games_df, se usan los valores limpios de games_reviews_df
    games_reviews_df = (
        games_reviews_df
        .merge(reviews_df[['game_id']], on='game_id')
        .drop_duplicates('game_id')
        )

    # Se obtiene un nombre para los juegos con nombres repetidos
    game_count = games_df.drop_duplicates('id')['name'].value_counts()
    games_df = (
        games_df
        .assign(
            n_count=games_df['name'].map(lambda name: game_count[name])
            )
    )
    games_df = (
        games_df
        .merge(
            games_df
            .groupby(['name', 'first_release_date'])['id']
            .count()
            .reset_index()
            .rename(columns={'id': 'n_count_2'}),
            on=['name', 'first_release_date']
            )
        )
    games_df['name'] = games_df.apply(
        lambda game: get_id(
            game['name'],
            game['first_release_date'],
            game['platforms'],
            game['n_count'],
            game['n_count_2']
            ),
        axis=1
        )
    games_df.drop(['id', 'n_count', 'n_count_2'], axis=1, inplace=True)

    # Se obtienen los nuevos datos de RAWG_rating y RAWG_nreviews
    print('Se obtiene el dataset limpio')
    games_df = (
        games_df.drop(['RAWG_rating', 'RAWG_nreviews'], axis=1)
        .merge(
            games_reviews_df, left_on='RAWG_link', right_on='game_id'
            )
        .drop('game_id', axis=1)
        .reset_index(drop=True)
        )

    games_df = (
        games_df
        .rename(columns={'RAWG_link': 'game_id'})
        .sort_values('name')
        )
    cols = games_df.columns.tolist()
    games_df = games_df[
        cols[3:0:-1] + [cols[0]] + [cols[9]] + cols[4:9] + cols[-2:] +
        cols[10:-2]
        ]

    # Se exportan las reviews
    print('Se obtienen las reviews limpias')
    clean_reviews = dict()
    for top_name in list(range(
            N_REVIEWS,
            int(ceil(reviews_df['id'].max()/N_REVIEWS)*N_REVIEWS)+1,
            N_REVIEWS
            )):
        low_name = top_name - (N_REVIEWS-1)
        mini_reviews_df = (
            reviews_df.loc[
                (reviews_df['id'] >= low_name) & (reviews_df['id'] <= top_name)
                ]
            .drop_duplicates('id')
            .sort_values('id')
            .reset_index(drop=True)
            )
        clean_reviews[
            f'reviews_clean_{low_name:07d}_{top_name:07d}.feather'
            ] = mini_reviews_df

    # Se devuelven los DataFrames de reviews y el dataset de juegos limpio
    return games_df, clean_reviews
