'''
Programa utilizado para realizar la ETL para obtener el dataset y las reviews
limpias
'''
# %%
# Se cargan las librerías necesarias para realizar este proceso

from configparser import ConfigParser
import warnings
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from games_cleaner import g_cleaner
from review_cleaner import r_cleaner

# %%
# Se cargan las claves necesarias para utilizar a lo largo del proceso
# También se necesitan claves de acceso a nuestro servidor de AWS

config = ConfigParser()
config.read('secrets.toml', encoding='utf-8')

BUCKET_S3 = config['AWS']['bucket_s3']
ORIGINAL_NAME = 'dataset/games.feather'
NEW_FILE_NAME = 'clean_dataset/games_clean.feather'
FOLDER = 'reviews/'
CLEAN_FOLDER = 'clean_reviews/'
warnings.filterwarnings('ignore')

# %%
# Se carga el dataset existente

try:
    games_df = pd.read_feather(f'{BUCKET_S3}/{ORIGINAL_NAME}')
    print('Dataset cargado correctamente desde S3')
except OSError or ClientError:
    print('No se ha podido cargar el dataset')

# %%
# Se realiza la conexion con S3
bucket = (
    boto3.resource('s3', region_name='us-east-1')
    .Bucket(name=BUCKET_S3[5:])
    )

# %%
# Se leen las reviews disponibles

av_files = [
    obj.key for obj in bucket.objects.filter(Prefix=FOLDER)
    if len(obj.key) > len(FOLDER)
    ]

reviews_list = []
for file in av_files:
    reviews_list.append(
        pd.read_feather(f'{BUCKET_S3}/{file}').drop('review_text', axis=1)
        )
    print(file)

reviews_df = pd.concat(reviews_list)
print('Reviews cargadas')

# %%
# Se limpia el dataset
first_clean_df = g_cleaner(games_df)
clean_df, clean_reviews = r_cleaner(first_clean_df, reviews_df)

for review in clean_reviews:
    clean_reviews[review].to_feather(
        f'{BUCKET_S3}/{CLEAN_FOLDER}{review}',
        compression='lz4')

print('Reviews limpias')

clean_df.reset_index(drop=True).astype(str).to_feather(
    f'{BUCKET_S3}/{NEW_FILE_NAME}',
    compression='lz4'
)

print('Dataset limpio')
