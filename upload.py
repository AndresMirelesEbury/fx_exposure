import os
import json

import pandas as pd

def get_schemas(table):
    with open("schemas.json") as f:
        schemas = json.loads(f.read())
    return schemas[table]


def upload_table(df, t_name, bq_client, config):
    schema = get_schemas(t_name)

    file_format = "NEWLINE_DELIMITED_JSON"
    local_tmp_file = f"{t_name}.json"
    df.to_json(local_tmp_file, orient="records", lines=True)

    write_method = "WRITE_APPEND"

    dst_table = config["outputs"]["dataset"] + "." + t_name

    
    bq_client.upload_local_file(
        source=local_tmp_file,
        destination_table=dst_table,
        schema=schema,
        file_format=file_format,
        write_disposition=write_method,
    )

    print("Upload to BQ done!",dst_table)

    os.remove(local_tmp_file)