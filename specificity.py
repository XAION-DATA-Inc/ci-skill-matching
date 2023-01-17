from elasticsearch import Elasticsearch
import pymysql
from pymysql.cursors import DictCursor
from infra.secret_manager import SecretManager


if __name__ == "__main__":
    secret_manager = SecretManager()

    elastic_cred = secret_manager.get_elastic_cred()
    es = Elasticsearch(host=elastic_cred.host, port=elastic_cred.port, http_auth=(elastic_cred.auth_username, elastic_cred.auth_password))
    config = secret_manager.get_mysql_config()
    db_connection = pymysql.connect(**config.dict(), cursorclass=DictCursor)
    cursor = db_connection.cursor()

    INDEX_NAME = 'candidates'
    #######################################################################
    per_page = 10000
    query_body = {
        "_source": "specificity",
        "size": per_page,
    }
    
    data = es.search(index=INDEX_NAME, body=query_body, scroll='2m')