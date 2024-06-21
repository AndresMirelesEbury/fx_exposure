from eb_data_utils.bigquery import EburyBigQueryClient
from eb_data_utils.criptography import EburySecretsManager
from eb_data_utils.storage import EburyStorageClient


def get_bq_link(config):
    project_id = config["project"]["project_id"]
    project_id_sm = config["project"]["project_id_secret_manager"]

    secret_manager_sa_path = config["secrets"]["service_account_path"]
    secret_manager_client = EburySecretsManager(
        project_id=project_id_sm, credentials=secret_manager_sa_path
    )

    bq_client = EburyBigQueryClient(
        config["secrets"]["gcp_service_account_bq"],
        project_id=project_id,
        secret_manager_client=secret_manager_client,
        scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive"],
    )
    gs_client = EburyStorageClient(
        config["secrets"]["gcp_service_account_gcs"],
        project_id=project_id,
        secret_manager_client=secret_manager_client,
    )
    return bq_client, gs_client
