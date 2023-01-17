import sys
from infra.secret_manager import SecretManager
from models.recommender import Recommender


if __name__ == "__main__":
    args = dict(arg.split('=') for arg in sys.argv[1:])
    start_id = int(args.get('start', 0))
    secret_manager = SecretManager()
    db_config = secret_manager.get_mysql_config()
    recommender = Recommender(db_config=db_config)
    # normalizer.normalize_job_location()
    recommender.add_project_recommendations(start_id)