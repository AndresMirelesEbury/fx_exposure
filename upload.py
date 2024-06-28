import os
import json

import pandas as pd

def get_schemas(table):
    with open("schemas.json") as f:
        schemas = json.loads(f.read())
    return schemas[table]


def upload_table(df, t_name, bq_client, config, date_str):
    schema = get_schemas(t_name)

    file_format = "NEWLINE_DELIMITED_JSON"
    local_tmp_file = f"{t_name}.json"
    df.to_json(local_tmp_file, orient="records", lines=True)

    write_method = 'WRITE_APPEND'

    dst_table = config["outputs"]["dataset"] + "." + t_name

    date = pd.to_datetime(date_str)
    date_str = date.strftime('%Y-%m-%d')
    df['balance_date'] = date_str
    
    # upload
    print(f'Uploading to `{config["project"]["project_id"] + "." + dst_table}`...')
    file_format = "NEWLINE_DELIMITED_JSON"
    local_tmp_file = "tmp_upload_file.json"
    df.to_json(local_tmp_file, orient="records", lines=True)

    # Remove previos executions if exists
    delete_query = f"delete from {dst_table} where date(balance_date) = date('{date_str}')"
    bq_client.query(delete_query, result_to_df=False)

    bq_client.upload_local_file(
        source=local_tmp_file,
        destination_table=dst_table,
        schema=schema,
        file_format=file_format,
        write_disposition=write_method,
    )

    os.remove(local_tmp_file)
    print(f'Upload successful')
