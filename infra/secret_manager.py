import json
from functools import lru_cache
from typing import Any, Dict

from boto3.session import Session
from infra.elasticsearch import ElasticSearchCred

from infra.mysql import DbConfig

class SecretManager:
    def __init__(self):
        self.region_name = "ap-northeast-1"
        self.service_name = "secretsmanager"

    def __get_secret(self, secret_name: str) -> Dict[str, Any]:
        session = Session()
        client = session.client(service_name=self.service_name, region_name=self.region_name)

        resp = client.get_secret_value(SecretId=secret_name)
        return dict(json.loads(resp["SecretString"]))

    @lru_cache()
    def get_mysql_config(self) -> DbConfig:
        secret = self.__get_secret("mysql_prd")
        return DbConfig(
            user=secret["username"],
            passwd=secret["password"],
            host=secret["host"],
        )

    @lru_cache()
    def get_elastic_cred(self) -> ElasticSearchCred:
        secret = self.__get_secret("elasticsearch_auth")
        return ElasticSearchCred(**secret, host='172.31.21.180', port=9200)