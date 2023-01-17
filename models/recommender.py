from infra.mysql import DbUser
import pymysql
from pymysql.cursors import DictCursor

class Recommender(DbUser):
    def add_project_recommendations(self, start_id: int):
        r_connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        w_connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor, autocommit=True)
        with r_connection.cursor() as r_cursor, w_connection.cursor() as w_cursor:            
            def __fetch_next_user_id(current_id: int):
                r_cursor.execute(
                    """
                        SELECT
                            DISTINCT us.user_id
                        FROM
                            career_index2.user_skills us
                        WHERE
                            us.user_id > %s
                        ORDER BY
                            us.user_id
                        LIMIT 1
                    """, [current_id]
                )
                print(current_id)
                row = r_cursor.fetchone()
                return row["user_id"] if row else None
            user_id = __fetch_next_user_id(start_id)
            while user_id:
                w_cursor.execute(
                    """
                        INSERT
                            INTO
                            career_index2.user_job_offer_recommendations 
                        SELECT
                            us.user_id as user_profile_id,
                            joc.job_offer_id ,
                            count(joc.skill_name) as score
                        FROM
                            career_index2.job_offer_combinations joc
                        INNER JOIN career_index2.user_skills us ON
                            joc.skill_name = us.skill
                        INNER JOIN career_index2.user_profiles up ON
                            up.id = us.user_id
                        WHERE
                            us.user_id = %s
                            and joc.job_type_id = up.hope_job_type_id
                            and joc.prefecture_id = up.hope_work_location_id
                        GROUP BY
                            joc.job_offer_id
                        order by
                            score desc
                        limit 25
                    """, [user_id]
                )
                user_id = __fetch_next_user_id(user_id)
            