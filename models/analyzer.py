import csv
import json
from math import inf
import re
from traceback import print_exc
from typing import Dict, Tuple
import pymysql
from pymysql.cursors import DictCursor
from infra.mysql import DbConfig
import scipy.stats as stats

from models.resume_helpers import get_years_experiences
from xlsxwriter.workbook import Workbook
import MeCab
import nltk
import unicodedata
from models.config import stopwords, none_requirement_titles, requirement_titles, invalid_lone_tokens

class Analyzer:
    income_tag_map: Dict[str, Tuple[float, float]] = {}

    def __init__(self, db_config: DbConfig):
        self.db_config = db_config
        self.db_connection = pymysql.connect(**db_config.dict(), cursorclass=DictCursor)
        self.mt = MeCab.Tagger("-Ochasen -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd")
        self.df = {}

    
    def generate_utility_matrix(self):
        self.db_connection.connect()
        income_matrix = {}
        with self.db_connection.cursor() as job_history_cursor:
            job_history_cursor.execute(
                """
                SELECT
                    ujh.annual_income ,
                    up.hope_annual_income_id ,
                    up.recent_annual_income
                FROM
                    career_index2.user_job_histories ujh
                INNER JOIN career_index2.user_profiles up on
                    up.id = ujh.profile_id
                LIMIT 1000
                """
            )
            for job_history in job_history_cursor:
                actual_income_tag = self.__get_income_tag(job_history["annual_income"])
                if actual_income_tag:
                    income_matrix[actual_income_tag] = income_matrix.get(actual_income_tag, {})
                    hope_annual_income_tag = job_history["hope_annual_income_id"]
                    income_matrix[actual_income_tag][hope_annual_income_tag] = income_matrix[actual_income_tag].get(hope_annual_income_tag, 0) + 1
        return income_matrix

    def __get_income_tag(self, income: int, retryable = True) -> str:
        # try to match 
        for (tag_id, income_range) in self.income_tag_map.items():
            if income >= income_range[0] and income <= income_range[1]:
                return tag_id
        # if the income does not match any range in income_tag_map, fetch mapping from db
        if retryable:
            self.__fetch_income_tag_map()
            return self.__get_income_tag(income=income, retryable=False)
        # cannot fetch mapping from db or mapping still fails to match
        return ""

    def fetch_income_tag_map(self):
        self.income_tag_map = {}
        with self.db_connection.cursor() as income_tag_map_cursor:
            income_tag_map_cursor.execute("SELECT * FROM career_index.hope_annual_incomes")
            for row in income_tag_map_cursor:
                lower = re.search(r"(\d+)(?:万円)*～", str(row["名称"]))
                lower = int(lower.group(1)) if lower else -inf
                upper = re.search(r"～(\d+)", str(row["名称"]))
                upper = int(upper.group(1)) if upper else inf
                self.income_tag_map[str(row["ID"])] = (lower, upper)
            income_tag_map_cursor.close()

    def analyze_pool_rate(self):
        self.db_connection.connect()
        groups: Dict[int, Tuple[int, int]] = {}
        with self.db_connection.cursor() as candidate_cursor:
            candidate_cursor.execute(
                """
                SELECT
                    c.id ,
                    c.name ,
                    c.user_id ,
                    m.created ,
                    c.resume ,
                    m.pooled 
                FROM
                    linkedin.messages m 
                INNER JOIN
                    linkedin.candidates c ON
                    c.user_id = m.url                 
                """
            )
            for conversation in candidate_cursor:
                # if candidate["reply_rate"] == 0 or candidate["conversation_count"] <= 1:
                #     continue
                resume = json.loads(conversation["resume"]) if conversation["resume"] else None
                if not resume:
                    continue
                experiences = resume.get("experiences", [])

                # days_in_last_job: int = None
                # for exp in reversed(experiences):
                #     end_date = dateutil.parser.parse(exp.get("end_date", None)) if exp.get("end_date", None) and exp.get("end_date", None) != "Present" else None 
                #     begin_date = dateutil.parser.parse(exp.get("begin_date", None)) if exp.get("begin_date", None) else None 
                #     if (begin_date and conversation["created"] > begin_date) and (not end_date or end_date > conversation["created"]):
                #         days_in_last_job = (conversation["created"] - begin_date).days
                #         break

                # if days_in_last_job:
                #     years = int(days_in_last_job//365)
                #     groups[years] = groups.get(years, [0, 0])                    
                #     if conversation["pooled"] == '1':
                #         groups[years][0] += 1
                #     else:
                #         groups[years][1] += 1
                
                # estimated_birth_year = birth_year_estimate(resume)["estimated_birth_year"]                
                # if estimated_birth_year:
                #     age = date.today().year - estimated_birth_year
                #     groups[age] = groups.get(age, [0, 0])                    
                #     if conversation["pooled"] == '1':
                #         groups[age][0] += 1
                #     else:
                #         groups[age][1] += 1

                yoe = get_years_experiences(experiences)

                if yoe:
                    yoe = (yoe//3)*3
                    groups[yoe] = groups.get(yoe, [0, 0])                    
                    if conversation["pooled"] == '1':
                        groups[yoe][0] += 1                    
                    groups[yoe][1] += 1
        candidate_cursor.close()
        self.db_connection.close()
        return sorted((((years, years + 3), pooled/total) for (years, [pooled, total]) in groups.items() if total > 1000), key=lambda x: x[0])

    def get_spearman(self):
        self.db_connection.connect()
        with self.db_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    up.id ,
                    up.hope_annual_income_id as hope_annual_income_id,
                    up.number_of_career_change as number_of_career_change,
                    AVG(NULLIF(ujh.annual_income, 0)) as actual_average_annual_income,
                    AVG(NULLIF(ujh.workforce, 0)) as workforce,
                    AVG(ujh.left_year - ujh.joined_year) as years_at_job
                FROM
                    career_index2.user_profiles up
                inner join career_index2.user_job_histories ujh on
                    up.id = ujh.profile_id
                group by
                    up.id
                """
            )
            source_fields = ["hope_annual_income_id"]
            target_fields = ["number_of_career_change", "actual_average_annual_income", "workforce", "years_at_job"]
            result = {}
            rows = cursor.fetchall()
            for src in source_fields:
                for dst in target_fields:
                    x = []
                    y = []
                    for row in rows:
                        if row[src] and row[dst]:
                            x.append(int(row[src]))
                            y.append(row[dst])
                    corr = stats.spearmanr(x, y).correlation
                    if src not in result:
                        result[src] = {}
                    result[src][dst] = corr
            return result

    def get_sample(self):
        self.db_connection.connect()
        with self.db_connection.cursor() as cursor:
            cursor.execute("""
                select
                    jt.ＩＤ as id,
                    jt.`職種(小)` as name
                from
                    career_index.job_types jt
            """)
            jt_map = {}
            for row in cursor:
                jt_map[row["id"]] = row["name"]

            cursor.execute("""
                select
                    p.ID as id,
                    p.名称 as name
                from
                    career_index.prefectures p
            """)
            p_map = {}


            for row in cursor:
                p_map[row["id"]] = row["name"]

            cursor.execute(
                """
                select
                    *
                from
                    (
                    SELECT
                        FLOOR(AVG(ujh.left_year - ujh.joined_year)) as avg_working_time,
                        GROUP_CONCAT(ujh.job_type_id) as job_history , 
                        GROUP_CONCAT(ujh.management_experience_id) as management_experience ,
                        res.*
                    from
                        (
                        SELECT
                            YEAR (f.created_at) - YEAR(up.birthday) as age,
                            up.id as user_id,
                            up.number_of_career_change ,
                            jt.`職種(小)` as hope_job_type,
                            i.`業種(小)` as hope_industry ,
                            jo.id as job_id,
                            jo.job_types ,
                            jo.industry_id, 
                            up.hope_work_location_id as hope_work_location,
                            jo.prefectures as work_location,
                            jo.salary,
                            jo.qualification_requirements,
                            jo.skill_required ,
                            c.established_at,
                            up.married,
                            up.prefecture_id,
                            up.recent_job_type_id,
                            up.self_promotion
                        FROM
                            career_index2.user_profiles up
                        INNER join career_index2.favorites f on
                            up.user_id = f.user_id
                        INNER join career_index2.job_offers jo on
                            f.job_offer_id = jo.id
                        inner join career_index2.companies c on
                            jo.company_id = c.id
                        inner join career_index.job_types jt on
                            jt.ＩＤ = up.hope_job_type_id
                        inner join career_index.industries i on
                            i.ＩＤ = up.hope_industry_id
                        WHERE
                            jo.max_salary > 0
                            and jo.max_salary < 100000000
                        limit 2000) as res
                    inner join career_index2.user_job_histories ujh on
                        res.user_id = ujh.profile_id
                    group by
                        res.user_id,
                        res.job_id) as r
                """
            )
            workbook = Workbook("result.xlsx")
            worksheet = workbook.add_worksheet()
            i = 1
            # m: dict[bool, dict[bool, int]] = {True: {True: 0, False: 0}, False: {True: 0, False: 0}}
            # age_jt_change_matrix = {}
            for row in cursor:
                xlsx_row = {}
                try:
                    # job_types = list(json.loads(row["job_types"]))
                    # job_type_changes = row["recent_job_type_id"] not in job_types
                    # row["job_types"] = ",".join(str(jt_map[jt_id]) for jt_id in job_types)
                    # row["work_location"] = list(json.loads(row["work_location"]))
                    # row["ready_to_relocate"] = row["prefecture_id"] not in row["work_location"]
                    # m[row["married"]][row["ready_to_relocate"]] += 1
                    # age_group = (int(row["avg_working_time"])//5)*5 if row["avg_working_time"] else 0
                    # age_jt_change_matrix[age_group] = age_jt_change_matrix.get(age_group, {})
                    # if row["recent_job_type_id"] and job_types:
                    #     age_jt_change_matrix[age_group][job_type_changes] = age_jt_change_matrix[age_group].get(job_type_changes, 0) + 1    
                    # row["hope_work_location"] = p_map.get(row["hope_work_location"], "")
                    # row["job_history"] = str(row["job_history"]).split(",")
                    # row["job_history"] = ",".join(str(jt_map[jt_id]) for jt_id in row["job_history"])
                    xlsx_row["user"] = row["user_id"]
                    xlsx_row["job"] = row["job_id"]
                    xlsx_row["skill_required"] = self.get_skill_tags(row["qualification_requirements"] + "\n" + row["skill_required"])
                    xlsx_row["self_promotion"] = self.get_skill_tags(row["self_promotion"])
                    xlsx_row["skill_match_rate"] = len([s for s in xlsx_row["self_promotion"] if s in xlsx_row["skill_required"] and self.df.get(s, 1) < 100])/(len(xlsx_row["skill_required"]) + len(xlsx_row["self_promotion"])) if xlsx_row["skill_required"] or xlsx_row["self_promotion"] else 0
                    xlsx_row["skill_required"] = ",".join(xlsx_row["skill_required"])
                    xlsx_row["self_promotion"] = ",".join(xlsx_row["self_promotion"])                    
                    del row["work_location"]
                    del row["qualification_requirements"]
                except:
                    print_exc()
                    continue
                if i == 1:
                    worksheet.write_row(0, 0, tuple(xlsx_row.keys()))
                if xlsx_row["self_promotion"] and xlsx_row["skill_required"]:
                    worksheet.write_row(i, 0, tuple(list(xlsx_row.values())))
                    i += 1
            workbook.close()
            # print(age_jt_change_matrix)
            # for k,v in age_jt_change_matrix.items():
            #     try:
            #         print(k, v[True]/(v[True] + v[False]), sep=": ")
            #     except:
            #         pass
    
    def get_skill_tags(self, text: str):
        if not text:
            return []
        node = self.mt.parseToNode(text)
        keys = []
        while node:
            # print(node.surface, node.feature)
            if "名詞" in node.feature and node.surface not in keys:
                keys.append(node.surface)
            node = node.next
        db_connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        keys = [str(key).strip() for key in keys if str(key).strip() not in ['n', 'null']]
        if not keys:
            return []
        skills = []
        with db_connection.cursor() as skill_cursor:
            for key in keys:
                if key in self.df.keys():
                    skills.append(key)
                    continue
                sql = f"SELECT s.skill_name FROM dashboard.wantedly_skill_dictionary s WHERE s.skill_name = %s"
                skill_cursor.execute(sql, key)
                row = skill_cursor.fetchone()
                if row:
                    skills.append(row["skill_name"])
                    self.df[row["skill_name"]] = self.df.get(row["skill_name"], 0) + 1
        return list(set(skills))

    def extract_user_skills(self):
        db_connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        f = open("user_skills.csv", mode="a")
        csv_writer = csv.DictWriter(f, fieldnames=["user_id", "skill"], doublequote=True)
        csv_writer.writeheader()
        n = 0
        with db_connection.cursor() as cursor:
            cursor.execute("select up.id, up.self_promotion from career_index2.user_profiles up LIMIT 1000 OFFSET %s", [n])
            rows = cursor.fetchall()
            while rows:
                for row in rows:
                    skills = self.get_skill_tags(row["self_promotion"])
                    csv_writer.writerows(
                        {
                            "user_id": row["id"],
                            "skill": skill
                        } for skill in skills
                    )
                    for skill in skills:
                        self.df[skill] = self.df.get(skill, 0) + 1
                print(n)
                cursor.execute("select up.id, up.self_promotion from career_index2.user_profiles up LIMIT 1000 OFFSET %s", [n])
                rows = cursor.fetchall()
        f.close()
        f = open("user_skill_counts.json", mode="w")
        json.dump(self.df, f, ensure_ascii=False)
        f.close()

    def extract_job_required_skills(self):
        self.df = {}
        db_connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        f = open("job_required_skills.csv", mode="a")
        csv_writer = csv.DictWriter(f, fieldnames=["job_id", "skill"], doublequote=True)
        # csv_writer.writeheader()
        n = 0
        with db_connection.cursor() as cursor:
            # cursor.execute("select jo.id, jo.qualification_requirements , jo.skill_required from career_index2.job_offers jo where jo.id > %s LIMIT 1000", [n])
            sql = "select cp.id, cp.application_requirement from dashboard.company_projects cp where cp.id > %s order by cp.id LIMIT 1000"
            cursor.execute(sql, [n])
            rows = cursor.fetchall()
            while rows:
                for row in rows:
                    skills = self.get_skill_tags(row["application_requirement"])
                    csv_writer.writerows(
                        {
                            "job_id": row["id"],
                            "skill": skill
                        } for skill in skills
                    )
                n = rows[-1]["id"]
                print(n)
                cursor.execute(sql, [n])
                rows = cursor.fetchall()
        f.close()
        f = open("job_skill_counts.json", mode="w")
        json.dump(self.df, f, ensure_ascii=False)
        f.close()
        self.df = {}
