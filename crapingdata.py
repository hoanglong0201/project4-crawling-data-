import requests
import json
import pymongo
import mysql.connector
API = 'https://api.topdev.vn/td/v2/jobs?fields[job]=id,slug,title,salary,company,extra_skills,skills_str,skills_arr,skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,addresses,status_display,detail_url,job_url,salary,published,refreshed,applied,candidate,requirements_arr,packages,benefits,content,features,is_free,is_basic,is_basic_plus,is_distinction&fields[company]=slug,tagline,addresses,skills_arr,industries_arr,industries_str,image_cover,image_galleries,benefits&page=0&locale=vi_VN&ordering=jobs_new'
folder = 'data_job'
url_last_page = 'https://api.topdev.vn/td/v2/jobs?page=1&locale=vi_VN&ordering=jobs_new'
page = requests.get(url_last_page)
LAST_PAGE = json.loads(page.text)["meta"]["last_page"]

# connect to mongodb
client = pymongo.MongoClient("localhost", 27017)
db = client['jobs']
collection = db['jobs']

def fetch_data():
    for page_num in range(1, LAST_PAGE + 1):
        url_page = requests.get(API.replace(f"page=0", f"page={page_num}", 1))
        with open(f"{folder}/data_{page_num}", 'w', encoding='utf-8') as file:
            json.dump(json.loads(url_page.text), file, ensure_ascii=False, indent=4)

company_id ={"0"}
company_name ={"null"}
def fetch_company():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                company_id.add(job["company"]["id"])
                company_name = dict(name = job["company"]["display_name"], id = job["company"]["id"])

def process():
    conn = mysql.connector.connect(
        host = 'localhost',
        user ='root',
        passwd ='02012020hl',
        database='topdev'
    )
    curr = conn.cursor(buffered=True)
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                company_id = (job["company"]["id"])
                company_name = (job["company"]["display_name"])
                curr.execute(""" insert into jobs values (%s, %s)""", (company_id,company_name))
    conn.commit()
process()
