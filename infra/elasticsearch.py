from pydantic import BaseModel


class ElasticSearchCred(BaseModel):
    auth_username: str
    auth_password: str
    host: str
    port: int
