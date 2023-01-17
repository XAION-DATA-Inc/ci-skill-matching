from infra.secret_manager import SecretManager
from models.data_normalizer import Normalizer


if __name__ == "__main__":
    secret_manager = SecretManager()
    db_config = secret_manager.get_mysql_config()
    normalizer = Normalizer(db_config=db_config)
    # normalizer.normalize_job_location()
    normalizer.normalize_job_title()