import json
import pymysql
from infra.secret_manager import SecretManager
from models.analyzer import Analyzer
from pymysql.cursors import DictCursor


def save_boost_queries(result):
    db_connection = pymysql.connect(**db_config.dict(), cursorclass=DictCursor)  
    rows = []
    for r in result:
        yoe = {
            "boost": round(1 + r[1], 5),
        }
        if r[0][0]:
            yoe["gte"] = r[0][0]
        if r[0][1]:
            yoe["lt"] = r[0][1]
        if "gte" in yoe or "lt" in yoe:            
            rows.append({
                "query": json.dumps({
                    "range": {
                        "years_of_experience": yoe
                    },
                }),
            })
    with db_connection.cursor() as cursor:
        cursor.executemany("INSERT INTO dashboard.search_boosters (query, index_name, created_from) VALUES (%(query)s, 'candidate', 'pool_rate_analyzer')", rows)
    db_connection.commit()
    db_connection.close()

if __name__ == "__main__":
    secret_manager = SecretManager()
    db_config = secret_manager.get_mysql_config()
    analyzer = Analyzer(db_config=db_config)
    # result = analyzer.analyze_pool_rate()
    # result = [((0, 5), 0.020678578598674963), ((5, 10), 0.037484346737164324), ((10, 15), 0.04321897384926483), ((15, 20), 0.045777933286680665), ((20, 25), 0.04057066428889879), ((25, 30), 0.030060553633217992), ((30, 35), 0.02154328241284763), ((35, 40), 0.018154311649016642)]
    # print(result)
    # save_boost_queries(result)
    # db_connection = pymysql.connect(**db_config.dict(), cursorclass=DictCursor)
    # with db_connection.cursor() as cursor:
    #     cursor.execute("""
    #         SELECT * FROM career_index2.user_profiles up WHERE up.id = %s 
    #     """, 1002168709)
    #     for row in cursor:
    #         text = row["self_promotion"]
    #         print (analyzer.get_skill_tags(text))
    result = analyzer.extract_job_required_skills()

#     text = """東京都でデザイン・ファッション系その他の仕事を志望し、転職活動を行っています。
# \nどうぞよろしくお願い致します。
# ■ヤングファッション・カジュアルファッションの接客販売経験。
# """
#     print(analyzer.get_skill_tags(text))

