#!/usr/bin/env python
# coding: utf-8

import re
import unicodedata

import click
import pandas as pd
from sqlalchemy import create_engine


def to_snake(name: str) -> str:
    name = re.sub(r'\s*\(.*?\)', '', name)  # retire les unités "(MW)"
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()  # é → e
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)  # espaces, tirets, points, ' → _
    return name.strip('_').lower()


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=to_snake)
    df['date_heure'] = pd.to_datetime(df['date_heure'], utc=True)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df.drop(columns=['heure'])


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='eco2mix_national', help='PostgreSQL database name')
@click.option('--target-table', default='energy_data', help='Target table name')
@click.option('--path', default='../../eco2mix-national.csv', help='Path to the eco2mix CSV file')
@click.option('--chunksize', default=200, type=int, help='Rows per chunk')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, path, chunksize):
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(path, sep=';', chunksize=chunksize)

    for i, chunk in enumerate(df_iter):
        chunk = transform(chunk)
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='replace' if i == 0 else 'append',
            index=False,
        )
        print(f'Chunk {i + 1} : {len(chunk)} lignes insérées')


if __name__ == '__main__':
    run()
