'''
Script utilizado para realizar una limpieza del dataset de juegos a usar en
el desarrollo del algoritmo
Aunque se realice esta limpieza, el resto de datos seguiran siendo utiles
para mostrar sus datos en la aplicacion final
'''

# %%
# Se cargan las librerías necesarias para realizar este proceso

import ast
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

# %%
# Se define la herramienta capaz de realizar one_hot_encoding en funcion
# de los valores que haya en una lista
mlb = MultiLabelBinarizer(sparse_output=True)

# %%
# Se define una lista de keywords que no deberan aparecer en los resultados
banned_keys = [
    'digital distribution', 'steam', 'achievements', 'steam achievements',
    'playstation trophies', 'bink video', 'sequel', 'steam trading cards',
    'gog.com', 'xbox live', 'greatest hits', 'platform exclusive',
    'steam cloud', 'downloadable content', 'games on demand', 'ps3',
    'playstation network', 'playstation plus',
    'xbox one backwards compatibility', 'switch', 'virtual console',
    'direct2drive', 'fantasy', 'sci-fi', 'role playing', 'adventure',
    'strategy', 'platformer', 'action-adventure', 'shooter', 'remake',
    'compilation', 'hack and slash', 'launch titles', 'arcade', 'porting',
    'xbox one x enhanced', 'simulation', 'driving/racing',
    'top-down perspective'
    ]

# %%
# Se definen las funciones utiles en todo el proceso de limpieza


def age_cols(row):
    '''
    Tratamos la columna de age_ratings
    '''
    if row is None or pd.isnull(row):
        return np.NaN
    rts = ast.literal_eval(row)
    if isinstance(rts, dict):
        rts = [rts]
    rts = {rat['rating'][:4]: int(rat['rating'][5:]) for rat in rts}
    if 'PEGI' in rts.keys():
        return rts['PEGI']
    return rts['ESRB']


def franchise_col(row):
    '''
    Tratamos la columna de franquicias
    '''
    if pd.isnull(row):
        return []
    data = ast.literal_eval(row)
    franchises = []
    for franchise in data:
        if isinstance(franchise, dict):
            franchises.append(franchise['name'])
        else:
            franchises.append(franchise)
    return list(set(franchises))


def dev_col(row):
    '''
    Tratamos la columna de desarrolladoras
    '''
    if pd.isnull(row):
        return []
    data = ast.literal_eval(row)
    devs = []
    countries = []
    for dev in data:
        devs.append(dev['name'])
        if 'country' in dev.keys():
            countries.append(dev['country'])
    return list(set(devs)), list(set(countries))


def pub_col(row):
    '''
    Tratamos la columna de publishers
    '''
    if pd.isnull(row):
        return []
    data = ast.literal_eval(row)
    pubs = [pub['name'] for pub in data]
    return list(set(pubs))


def rawg_rat(row):
    '''
    Tratamos la columna de RAWG_nrewiews
    '''
    if pd.isnull(row):
        return 0
    reviews = ast.literal_eval(row)
    return sum(reviews.values())


def get_mode(d_f, fixed_col, col):
    '''
    Obtiene la moda de un DataFrame para la columna solicitada y agrupando
    con las columnas dadas
    '''
    return (
        d_f
        .groupby(fixed_col + [col])
        [col]
        .agg(['count', 'max'])
        .sort_values(fixed_col + ['count'])
        .reset_index()
        .drop_duplicates(fixed_col)
        .drop(['count', 'max'], axis=1)
        )


def obtain_mode_df(d_f, col):
    '''
    Se obtienen los 3 valores necesarios para rellenar la funcion de merge_mode
    '''
    genre_theme_df = get_mode(d_f, ['genres', 'themes'], col)
    genre_df = get_mode(d_f, ['genres'], col)
    mode = d_f[col].value_counts().index[0]

    return genre_theme_df, genre_df, mode


def merge_mode(value_col, genres, themes, genre_theme_df, genre_df, mode):
    '''
    Se rellenan los valores nulos con la moda
    '''
    if not pd.isnull(value_col):
        return value_col
    sp_df = genre_theme_df.loc[
        (genre_theme_df['genres'] == genres) &
        (genre_theme_df['themes'] == themes)
        ]
    if len(sp_df) == 1:
        return sp_df.iloc[0, -1]
    sp_df = genre_df.loc[genre_df['genres'] == genres]
    if len(sp_df) == 1:
        return sp_df.iloc[0, -1]
    return mode


def fill_mode(d_f, col):
    '''
    Realiza el proceso anterior completo
    '''
    genre_theme_df, genre_df, mode = obtain_mode_df(d_f, col)
    return (
        d_f
        .apply(
            lambda x: merge_mode(
                x[col], x['genres'], x['themes'],
                genre_theme_df, genre_df, mode
                ),
            axis=1
            )
        )


def col_onehot(d_f, col):
    '''
    Se transforma una columna que contenga listas a one hot encoding
    '''
    return d_f.join(
          pd.DataFrame
          .sparse
          .from_spmatrix(
              mlb.fit_transform(d_f.pop(col)),
              index=d_f.index,
              columns=mlb.classes_
                  )
          )


def get_mean(d_f, fixed_col, col, roundng):
    '''
    Obtiene la media de un DataFrame para la columna solicitada y agrupando
    con las columnas dadas
    '''
    if roundng:
        return round(d_f.groupby(fixed_col, as_index=False)[col].mean(), 0)
    return d_f.groupby(fixed_col, as_index=False)[col].mean()


def obtain_mean_df(d_f, col, roundng):
    '''
    Se obtienen los 3 valores necesarios para rellenar la funcion de merge_mean
    '''
    genre_theme_df = get_mean(d_f, ['genres', 'themes'], col, roundng).dropna()
    genre_df = get_mean(d_f, ['genres'], col, roundng).dropna()
    if roundng:
        mean = int(d_f[col].mean())
    else:
        mean = d_f[col].mean()

    return genre_theme_df, genre_df, mean


def merge_mean(value_col, genres, themes, genre_theme_df, genre_df, mean):
    '''
    Se rellenan los valores nulos con la media
    '''
    if not pd.isnull(value_col):
        return value_col
    sp_df = genre_theme_df.loc[
        (genre_theme_df['genres'] == genres) &
        (genre_theme_df['themes'] == themes)
        ]
    if len(sp_df) == 1:
        return sp_df.iloc[0, -1]
    sp_df = genre_df.loc[genre_df['genres'] == genres]
    if len(sp_df) == 1:
        return sp_df.iloc[0, -1]
    return mean


def fill_mean(d_f, col, roundng=True):
    '''
    Realiza el proceso anterior completo
    '''
    genre_theme_df, genre_df, mean = obtain_mean_df(d_f, col, roundng)
    return (
        d_f
        .apply(
            lambda x: merge_mean(
                x[col], x['genres'], x['themes'],
                genre_theme_df, genre_df, mean
                ),
            axis=1
            )
        )


def keyword_explosion(d_f, fixed_col):
    '''
    Se obtiene la cuenta total de cada keyword en funcion de los grupos
    buscados
    '''
    return (
        d_f[fixed_col + ['keywords']]
        .explode('keywords')
        .dropna()
        .value_counts()
        .reset_index()
        .rename(columns={0: 'count'})
        )


def get_new_keywords(keywords, genres, themes, keys, n=6):
    '''
    Se define el numero de keywords que tendra cada juego, y se rellena en
    funcion de las mas usadas por genero y tematica. Se excluiran aquellas que
    esten en la lista de baneadas
    '''
    keywords = [keyword for keyword in keywords if keyword not in banned_keys]
    while len(keywords) < n:
        to_add = (
            keys[0].loc[
                (keys[0]['genres'] == genres) &
                (keys[0]['themes'] == themes),
                'keywords'
            ]
            .tolist()
        )
        for i in range(n):
            try:
                if to_add[i] not in keywords:
                    keywords.append(to_add[i])
            except IndexError:
                break
        to_add = (
            keys[1].loc[keys[1]['genres'] == genres, 'keywords'].tolist()
        )
        for i in range(n):
            try:
                if to_add[i] not in keywords:
                    keywords.append(to_add[i])
            except IndexError:
                break
        to_add = keys[2]['keywords'].tolist()
        for i in range(6):
            if to_add[i] not in keywords:
                keywords.append(to_add[i])
    return keywords[:n]


def get_top(d_f, col, topx, min_games):
    '''
    Para la col solicitada, se obtiene el topx juegos segun su OC_rating con un
    minimo min_games de juegos
    '''
    return (
        d_f
        .explode(col)
        .groupby(col, as_index=False)
        ['OC_rating']
        .agg(['max', 'count'])
        .reset_index()
        .loc[lambda df: df['count'] > min_games]
        .sort_values(['max', 'count'], ascending=False)
        .iloc[:topx, 0]
        .tolist()
        )


# %%
# Se definen la funcion que se usara en la ETL


def g_cleaner(games_df):
    '''
    Dado un DataFrame, se limpiara este para lograr unos valores utiles de cara
    a desarrollar el algoritmo
    '''

    games_df = (
        games_df
        .replace(['nan', 'None', '[]', '{}'], np.NaN)
        .drop([
            'bundles', 'category', 'devs', 'expanded_games', 'expansions',
            'game_engines', 'HLTB_link', 'HLTB_name', 'n_count', 'OC_link',
            'OC_name', 'OC_nreviews', 'parent_game', 'porting', 'ports',
            'RAWG_name', 'release_dates', 'remakes', 'remasters',
            'standalone_expansions', 'status', 'storyline', 'supporting',
            'updated_at'
            ],
            axis=1
            )
        )

    games_df['id'] = games_df['id'].astype(int)
    games_df = (
        games_df
        .groupby('id', as_index=False)
        .agg({'platforms': lambda x: x.tolist()})
        .merge(
            games_df.drop('platforms', axis=1).drop_duplicates('id'),
            on='id'
            )
        )

    # Se cambian los tipos de datos para su correcto tratamiento
    duration_col = [col for col in games_df if col.endswith('duration')]
    rating_col = [col for col in games_df if col.endswith('rating')]
    for col in duration_col + rating_col:
        games_df[col] = games_df[col].astype(float)

    dates_col = [col for col in games_df if 'date' in col]
    for col in dates_col:
        games_df[col] = pd.to_datetime(games_df[col]).dt.year

    # Se limpian aquellos con datos incorrectos de RAWG, pues no se lograria un
    # correcto cruce con las reviews
    games_df = (
        games_df
        .loc[games_df['RAWG_equal_name'] == 'True']
        .drop('RAWG_equal_name', axis=1)
        )

    # Se borran los valores nulos de las columnas que lo permiten dado su
    # infimo porcentaje y eliminamos la columna, pues no podra ser utilizada en
    # el desarrollo del algoritmo
    print('Comienza el tratamiento del dataset')

    games_df.dropna(subset=['summary'], inplace=True)
    games_df.drop('summary', axis=1, inplace=True)

    # Se limpian las columnas que sean necesarias
    games_df['age_ratings'] = games_df['age_ratings'].map(age_cols)
    games_df['franchises'] = games_df['franchises'].map(franchise_col)
    games_df = games_df.reset_index(drop=True)
    games_df['developer'] = games_df['developer'].fillna('[]')
    games_df['publisher'] = games_df['publisher'].fillna('[]')
    games_df[['developer', 'country']] = pd.DataFrame(
        games_df['developer'].map(dev_col).tolist()
        )
    games_df['publisher'] = games_df['publisher'].map(pub_col)
    games_df['RAWG_nreviews'] = games_df['RAWG_nreviews'].map(rawg_rat)
    games_df['devs'] = (
        games_df['advanced_devs'].fillna('[]').map(ast.literal_eval)
        )
    games_df.drop('advanced_devs', axis=1, inplace=True)
    games_df = games_df.loc[games_df['RAWG_nreviews'] > 0]

    # Se tratan los ratings llenando los datos nulos de OC con los de MC, e
    # iterando para el resto
    print('Se obtienen los ratings')
    games_df.loc[games_df['OC_equal_name'] != 'True', 'OC_rating'] = np.NaN
    games_df['OC_rating'] = games_df['OC_rating'].replace(0, np.NaN)
    games_df['OC_rating'] = (
        games_df['OC_rating']
        .fillna(
            round(
                games_df['MC_rating'] /
                (games_df['MC_rating'].mean() / games_df['OC_rating'].mean()),
                0
                )
            )
        )

    games_df['OC_rating'] = fill_mean(games_df, 'OC_rating')
    games_df.drop(['MC_rating', 'OC_equal_name'], axis=1, inplace=True)

    # Se aplican la funcion de moda a las columnas requeridas
    for col in ['age_ratings', 'game_modes', 'player_perspectives']:
        games_df[col] = fill_mode(games_df, col)

    # Se tratan las keywords
    print('Se tratan las keywords')
    games_df['keywords'] = (
        games_df['keywords'].fillna('[]').map(ast.literal_eval)
        )

    key_count = []
    for cols in [['genres', 'themes'], ['genres'], []]:
        key_count.append(
            keyword_explosion(games_df, cols)
            .loc[lambda df: ~(df['keywords'].isin(banned_keys))]
            )

    games_df['keywords'] = games_df.apply(
        lambda x: get_new_keywords(
            x['keywords'], x['genres'], x['themes'], key_count
            ),
        axis=1)

    # Se trata los datos de duracion
    print('Se trata la duracion')
    games_df.loc[games_df['HLTB_equal_name'] != 'True', duration_col] = np.NaN
    games_df.drop('HLTB_equal_name', axis=1, inplace=True)
    games_df[duration_col] = games_df[duration_col].replace(0, np.NaN)
    for col in duration_col:
        games_df[col] = fill_mean(games_df, col, False)

    # Se tratan los devs para quedarnos unicamente con los que esten en las
    # posiciones de director, escritor, disenador o productor
    print('Se tratan los devs')
    games_df['devs'] = games_df['devs'].map(
        lambda devs: [
            dev['Name'] for dev in devs
            if dev['Position'][0] in [
                    'director', 'writer', 'designer', 'producer'
                    ]
            ]
        )

    # Para ciertas columnas, solo se conservara un top de variables, de
    # cara a no dejar un one_hot_encoding de muchas columnas
    # Este top se hará por los juegos con una mejor nota segun los nuevos
    # valores de OC y con un minimo de juegos
    print('Se obtienen los valores top')
    col_top = [
        'developer', 'publisher', 'keywords', 'devs', 'franchises', 'country'
        ]
    for col, topx, min_games in zip(
            col_top,
            [50, 50, 200, 100, 100, 15],
            [5, 5, 10, 5, 2, 10]):
        games_df = (
            games_df
            .drop(col, axis=1)
            .merge(
                games_df
                .explode(col)
                .loc[lambda df: df[col].isin(
                    get_top(games_df, col, topx, min_games)
                    )]
                .groupby('id', as_index=False)
                [col]
                .agg(lambda x: x.tolist()),
                on='id',
                how='left'
                )
            )

    # Se pasan las variables a one_hot_encoding
    print('Se realiza el one_hot_encoding')
    col_hot = ['game_modes', 'player_perspectives']
    for col in col_hot:
        games_df[col] = games_df[col].map(ast.literal_eval)
        cols = games_df.columns
        games_df = col_onehot(games_df, col)
        games_df = games_df.rename(columns={
            f'{col_name}': f'{col_name}_{col}'
            for col_name in games_df if col_name not in cols
            })

    # Se pasan las variables con valores nulos a one_hot_encoding
    col_nan = ['genres', 'themes']
    for col in col_nan:
        games_df[col] = games_df[col].fillna('[]').map(ast.literal_eval)
        cols = games_df.columns
        games_df = col_onehot(games_df, col)
        games_df = games_df.rename(columns={
            f'{col_name}': f'{col_name}_{col}'
            for col_name in games_df if col_name not in cols
            })

    # Se pasan las variables restantes a one_hot_encoding
    for col in col_top:
        games_df[col] = games_df[col].map(
            lambda x: x if isinstance(x, list) else []
            )
        cols = games_df.columns
        games_df = col_onehot(games_df, col)
        games_df = games_df.rename(columns={
            f'{col_name}': f'{col_name}_{col}'
            for col_name in games_df if col_name not in cols
            })

    # Se crea un array de las columnas de one_hot
    for col_name in col_top + col_nan + col_hot:
        cols_to_treat = [col for col in games_df if col.endswith(col_name)]
        games_df[col_name] = games_df[cols_to_treat].values.tolist()
        games_df.drop(cols_to_treat, axis=1, inplace=True)

    # Se devuelve el dataset limpio previo a la limpieza de las reviews
    print('Primera limpieza completada')
    return games_df
