# ci-skill-matching
This project is not production-ready and contains multiple unrelated scripts and functions.
## Objective 
Recommend projects for users based on the skills, location and job title.

## Ituition 
Join tables to match location and job title then recommend the projects with large number of skills matched.

## Preparation
- Extracting the skills from user_profiles self PR into user_skills (extract_user_skills)
- Extracting the skills from job_offers requirements into job_offer_skills (extract_job_required_skills)
- Normalizing job titles and locations in job_offers into job_offer_titles and job_offer_locations (normalize_job_title and normalize_job_location)

## Implementation
- Initial idea: joins user_profiles, user_skills, job_offer_skills, job_offer_titles, job_offer_locations
- To simplify the query, I joined job_offer_skills, job_offer_titles, job_offer_locations beforehand into job_offer_combinations
