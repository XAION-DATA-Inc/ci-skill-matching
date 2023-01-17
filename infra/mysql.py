from pydantic import BaseModel

class DbConfig(BaseModel):
    host: str
    user: str
    passwd: str
    charset: str = "utf8"
    use_unicode: bool = True
