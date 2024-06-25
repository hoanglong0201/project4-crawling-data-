import requests
import json
import pymongo
API = 'https://api.topdev.vn/td/v2/jobs?fields[job]=id,slug,title,salary,company,extra_skills,skills_str,skills_arr,skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,addresses,status_display,detail_url,job_url,salary,published,refreshed,applied,candidate,requirements_arr,packages,benefits,content,features,is_free,is_basic,is_basic_plus,is_distinction&fields[company]=slug,tagline,addresses,skills_arr,industries_arr,industries_str,image_cover,image_galleries,benefits&page=0&locale=vi_VN&ordering=jobs_new'
folder = 'data_job'
url_last_page = 'https://api.topdev.vn/td/v2/jobs?page=1&locale=vi_VN&ordering=jobs_new'
page =requests.get(url_last_page)
global LAST_PAGE
LAST_PAGE = json.loads(page.text)["meta"]["last_page"]
print(LAST_PAGE)
for page_num in range(1, LAST_PAGE + 1):
    url_page = requests.get(API.replace(f"page=0", f"page={page_num}", 1))
    with open(f"{folder}/data_{page_num}", 'w', encoding='utf-8') as file:
        json.dump(json.loads(url_page.text), file, ensure_ascii=False, indent=4)

conn = pymongo.MongoClient("localhost",27017)
db = conn['jobs']

