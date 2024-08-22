import sys
import glob
import os
import json
import re
import pandas as pd


def get_column_names(schema, ds_name, sorting_key='column_position'):
    column_details = schema[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=10000)
    return df_reader


def to_sql(df, db_conn_url, ds_name):
    df.to_sql(name=ds_name,
              con=db_conn_url,
              if_exists='append',
              index=False
              )


def db_loader(src_base_dir, db_conn_url, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found with name {ds_name}')

    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_url, ds_name)


def process_files(ds_names=None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    if src_base_dir is None:
        src_base_dir = "file-to-db-loader/data/retail_db"

    if db_host is None:
        db_host = "localhost"

    if db_port is None:
        db_port = 5432

    if db_name is None:
        db_name = "retail_db"

    if db_user is None:
        db_user = "postgres"

    if db_pass is None:
        db_pass = 2254

    db_conn_url = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f"Processing {ds_name}")
            db_loader(src_base_dir, db_conn_url, ds_name)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f"Error Processing {ds_name}")


if __name__ == '__main__':
    # if len(sys.argv) == 2:
    #     ds_names = json.loads(sys.argv[1])
    ds_names = ["orders"]
    process_files(ds_names)
    # else:
    #     process_files()
