import requests
import json
import pymongo
import mysql.connector
from bs4 import BeautifulSoup
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
        with open(f"{folder}/data_topdev", 'w', encoding='utf-8') as file:
            json.dump(json.loads(url_page.text), file, ensure_ascii=False, indent=4)
def fetch_job():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                job_title = job['title']
                job_level = job["job_levels_str"]
                features = job["features"]
                skill_job = job["skills_str"]
                salary = job["salary"]
                dic = dict( job_id = job_id, title = job_title, salary = salary, level = job_level, features = features, skill = skill_job)
                print(dic)
def fetch_requirements():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                req = ''
                for detail in job["requirements_arr"]:
                    req = (detail["value"])
                dicreq = dict(id = job_id, req = req)
                print(dicreq)
def fetch_benefits():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                benef =[]
                for benf in job["company"]["benefits"]:
                    benef.append(benf["value"])
                dicbenef = dict(id = job_id, benefit = benef)
                print(dicbenef)
# fetch_benefits()
# fetch_requirements()
def fetch_company():
    set_company_id = {0}
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                company_id = company["company"]["id"]
                if( company_id in set_company_id):
                    continue
                else:
                    url_company = company["company"]["detail_url"]
                    img_logo = company["company"]["image_logo"]
                    company_name = company["company"]["display_name"]
                    req = requests.get(url_company)
                    soup = BeautifulSoup(req.text, 'html')
                    link = soup.find('a', class_="mt-2 inline-block break-all text-blue-dark hover:underline")
                    try:
                        get_link = link.text.strip()
                    except:
                        get_link = "Not found!"
                    company_address = company["company"]["addresses"]["full_addresses"]
                    diccom = dict(company_id=company_id, image_logo = img_logo, name = company_name, link=get_link, address=company_address)
                set_company_id.add(company["company"]["id"])
def process():
    conn = mysql.connector.connect(
        host = 'localhost',
        user ='root',
        passwd ='02012020hl',
        database='topdev'
    )
    curr = conn.cursor(buffered=True)
    data = {}
    set_company_id = {0}
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                company_id = company["company"]["id"]
                if (company_id in set_company_id):
                    continue
                else:
                    url_company = company["company"]["detail_url"]
                    img_logo = company["company"]["image_logo"]
                    company_name = company["company"]["display_name"]
                    req = requests.get(url_company)
                    soup = BeautifulSoup(req.text, 'html')
                    link = soup.find('a', class_="mt-2 inline-block break-all text-blue-dark hover:underline")
                    try:
                        get_link = link.text.strip()
                    except:
                        get_link = "Not found!"
                    company_address = company["company"]["addresses"]["full_addresses"]
                    diccom = dict(company_id=company_id, image_logo=img_logo, name=company_name, link=get_link, address=company_address)
                set_company_id.add(company["company"]["id"])
                curr.execute(""" insert into companies values (%s,%s)""", (diccom['company_id'],diccom['name']))
    conn.commit()
process()
