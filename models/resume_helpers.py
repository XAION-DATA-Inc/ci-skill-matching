import datetime


def birth_year_estimate(resume: dict) -> dict:
    max_school_index = 0
    confidence = 1.0
    estimated_birth_year = 0
    school_index = 0
    school_year_mapping = [
        [25, 28],  # DOCTOR dgree
        [23, 25],  # MASTER degree
        [19, 21],  # ASSOCIATE College
        [19, 23],  # BACHELOR degree
        [16, 19],  # HIGH School
        [13, 16],  # MIDDLE School
    ]
    if "educations" in resume:
        for education in reversed(resume["educations"]):
            school_index = estimate_school_type(education.get("school", ""), education.get("degree", ""))
            if school_index:
                max_school_index = school_index if max_school_index <= school_index else max_school_index
                if "begin_date" in education and education["begin_date"]:
                    estimated_birth_year = (
                        int(extract_year(education["begin_date"])) - school_year_mapping[school_index][0]
                    )
                    return {"confidence": confidence, "estimated_birth_year": estimated_birth_year}
                elif "end_date" in education and education["end_date"]:
                    estimated_birth_year = (
                        int(extract_year(education["end_date"])) - school_year_mapping[school_index][1]
                    )
                    return {"confidence": confidence, "estimated_birth_year": estimated_birth_year}

    # If the candidates doesn't have any education info, it will be estimated from first career
    confidence = 0.5
    if "experiences" in resume:
        for experience in reversed(resume["experiences"]):
            # If it is intern, skipping.
            if is_intern(experience.get("title", "")) or is_intern(experience.get("employee_status", "")):
                pass
            else:
                if "begin_date" in experience and experience["begin_date"]:
                    # If candidate has final education without any years, it will be estimated from the first job year.
                    if school_index:
                        estimated_birth_year = (
                            int(extract_year(experience["begin_date"])) - school_year_mapping[school_index][1]
                        )
                    # If candidates does not have any educational info, it will be estimated as bachelor degree.
                    else:
                        estimated_birth_year = int(extract_year(experience["begin_date"])) - 23
                    return {"confidence": confidence, "estimated_birth_year": estimated_birth_year}
                else:
                    return {"confidence": 0, "estimated_birth_year": 0}
    return {"confidence": 0, "estimated_birth_year": 0}


def is_intern(word):
    if not word:
        return False
    intern_words = ["インターン", "Intern"]
    for intern_word in intern_words:
        if intern_word in word.lower():
            return True
    return False


def extract_year(datespan: str):
    if "年" in datespan:
        datespan = datespan.split("年")[0]
        return datespan
    # Date format 2015-04-05 / 2015-04 / 2015
    datespan = datespan.split("-")[0]
    datespan = datespan.split("/")[0]
    return datespan


def estimate_school_type(school: str, degree: str):
    school_mappings = [
        ["middle school", "中学"],
        ["high school", "高校", "高等"],
        ["bachelor", "学士", "university", "college", "大学"],
        ["associate", "高専", "専門", "準学士", "短期大学", "national college of technology", "National Institute of Technology"],
        ["master", "修士"],
        ["doctor", "博士"],
    ]
    for i, school_mapping in enumerate(reversed(school_mappings)):
        for school_word in school_mapping:
            if (school and school_word in school.lower()) or (degree and school_word in degree.lower()):
                return i
    return 0


def get_years_experiences(experiences):
    if len(experiences) != 0:
        if experiences[-1].get("begin_date") is not None and experiences[-1].get("begin_date") != "":
            started_year = experiences[-1]["begin_date"].strip().strip('-').split("-")[0]
            years_experiences = int(datetime.date.today().year) - int(started_year)
            return years_experiences
    return None