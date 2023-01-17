# ci-skill-matching
This project is not production-ready and contains multiple unrelated scripts and functions. All the tables are in the career_index2 database
## Objective 
Recommend projects for users based on the skills, location and job title.

## Ituition 
Join tables to match location and job title then recommend the projects with large number of skills matched.

## Preparation
- Extracting the skills from `user_profiles` self PR into `user_skills` (`extract_user_skills`)
- Extracting the skills from `job_offers` requirements into `job_offer_skills` (`extract_job_required_skills`)
- Normalizing job titles and locations in `job_offers` into `job_offer_titles` and `job_offer_locations` (`normalize_job_title` and `normalize_job_location`)

## Implementation
- Initial idea: joins `user_profiles`, `user_skills`, `job_offer_skills`, `job_offer_titles`, `job_offer_locations`
- To simplify the query, I joined `job_offer_skills`, `job_offer_titles`, `job_offer_locations` beforehand into `job_offer_combinations`
```
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
	us.user_id = 1000002216
	and joc.job_type_id = up.hope_job_type_id
	and joc.prefecture_id = up.hope_work_location_id
GROUP BY
	joc.job_offer_id
order by
	score desc
limit 25	
```
- I store the result into `user_job_offer_recommendations` for caching in `add_project_recommendations`
