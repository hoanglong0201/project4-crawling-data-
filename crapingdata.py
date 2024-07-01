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

def decode(s):
    t = ''
    for i in s:
        if(i == "*" ):
            i = i.replace("*", "0")
        t = t + i
    t = int(t)
    return t
# connect to mongodb
# client = pymongo.MongoClient("localhost", 27017)
# db = client['jobs']
# collection = db['jobs']
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['topdev']
mycol = mydb['jobs']

#connect to mysql
conn = mysql.connector.connect(
        host = 'localhost',
        user ='root',
        passwd ='02012020hl',
        database='topdev'
)
curr = conn.cursor(buffered=True)
def fetch_data():
    for page_num in range(1, LAST_PAGE + 1):
        url_page = requests.get(API.replace(f"page=0", f"page={page_num}", 1))
        with open(f"{folder}/data_{page_num}", 'w', encoding='utf-8') as file:
            json.dump(json.loads(url_page.text), file, ensure_ascii=False, indent=4)
def fetch_job():
    set_job_id = {0}
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                company_id = job["owned_id"]
                if(job_id in set_job_id):
                    continue
                else:
                    job_title = job['title']
                    job_level = job["job_levels_str"]
                    skill_job = job["skills_str"]
                    dic = dict(job_id=job_id, title=job_title, company_id = company_id, level=job_level, skill=skill_job)
                    curr.execute(""" insert into jobs values (%s,%s,%s,%s,%s)""",
                                 (dic['job_id'], dic['title'], dic['company_id'], dic['level'], dic['skill']))
                set_job_id.add(job["id"])
    conn.commit()
def insert_data():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                x = job
                ins = mycol.insert_one(x)


def fetch_benefits():
    set_job_id = {0}
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                benef = ''
                if(job_id in set_job_id):
                    continue
                else:
                    for benf in job["company"]["benefits"]:
                        benef = benef + (benf["value"]) + ", "
                    dicbenef = dict(id=job_id, benefit=benef)
                    curr.execute(""" insert into benefits values (%s,%s)""",
                                 (dicbenef['id'], dicbenef['benefit']))
                set_job_id.add(job["id"])
    conn.commit()
def fetch_salary():
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for job in parse["data"]:
                job_id = job["id"]
                negotiable = job["salary"]["is_negotiable"]
                unit = job["salary"]["unit"]
                min = decode(job["salary"]["min"])
                max = decode(job["salary"]["max"])
                currency = job["salary"]["currency"]
                min_estimate = job["salary"]["min_estimate"]
                max_estimate = job["salary"]["max_estimate"]
                currency_estimate = job["salary"]["currency_estimate"]
                dictsal = dict(job_id=job_id, negotiable=negotiable, unit=unit, min=min, max=max, currency=currency,
                                   min_estimate=min_estimate, max_estimate=max_estimate,
                                   currency_estimate=currency_estimate)
                curr.execute(""" insert into salaries values (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                 (dictsal['job_id'], dictsal['negotiable'], dictsal['unit'], dictsal['min'],
                                  dictsal['max'], dictsal['currency'], dictsal['min_estimate'], dictsal['max_estimate'],
                                  dictsal['currency_estimate']))
    conn.commit()
def fetch_company():
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
                    get_country = soup.find('div', class_="flex gap-2")
                    get_aboutus = soup.find('p')
                    about_us = get_aboutus.text.strip()
                    try:
                        get_size = soup.find_all('p', class_="mt-2")[1]
                    except:
                        get_size= 'Not found!'
                    try:
                        get_link = link.text.strip()
                        country = get_country.text.strip()
                        size = get_size.text.strip()
                    except:
                        get_link = "Not found!"
                        country = "Not found!"
                        size = "Not found!"
                    diccom = dict(company_id=company_id, name=company_name, image_logo = img_logo, link = get_link, country = country, size = size, about_us =about_us )
                    curr.execute(""" insert into companies values (%s,%s,%s,%s,%s,%s,%s)""",
                             (diccom['company_id'],diccom['name'], diccom['country'],diccom['size'], diccom['image_logo'], diccom['link'], diccom['about_us']))
                set_company_id.add(company["company"]["id"])
    conn.commit()

def fetch_ward():
    set_ward_id = {0}
    set_defalut_wid = 0
    set_defalut_wname ='undefined'
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                for id_add in company["company"]["addresses"]["collection_addresses"]:
                    ward_id = id_add["ward"]["id"]
                    if(ward_id in set_ward_id):
                        continue
                    else:
                        ward_name = id_add["ward"]["value"]
                        dictward = dict(ward_id=ward_id, ward_name=ward_name)
                        try:
                            curr.execute(""" insert into ward values (%s,%s)""",
                                         (dictward['ward_id'], dictward['ward_name']))
                        except:
                            curr.execute(""" insert into ward values (%s,%s)""",
                                         (set_defalut_wid, set_defalut_wname))

                    set_ward_id.add(id_add["ward"]["id"])
    conn.commit()

def fetch_province():
    set_province_id = {0}
    df_province_id = 0
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                for id_add in company["company"]["addresses"]["collection_addresses"]:
                    province_id = id_add["province"]["id"]
                    if(province_id in set_province_id):
                        continue
                    else:
                        province_name = id_add["province"]["value"]
                        dictprov = dict(province_id= province_id, province_name= province_name)
                        try:
                            curr.execute(""" insert into province values (%s,%s)""",
                                    (dictprov['province_id'], dictprov['province_name']))
                        except:
                            curr.execute(""" insert into province values (%s,%s)""",
                                         (df_province_id, dictprov['province_name']))
                    set_province_id.add(id_add["province"]["id"])
    conn.commit()
def fetch_district():
    set_district_id = {0}
    set_df_did = 0
    set_df_dname ='undefined'
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                for id_add in company["company"]["addresses"]["collection_addresses"]:
                    district_id = id_add["district"]["id"]
                    if(district_id in set_district_id):
                        continue
                    else:
                        district_name = id_add["district"]["value"]
                        dictdist = dict(district_id= district_id, district_name= district_name)
                        try:
                            curr.execute(""" insert into district values (%s,%s)""",
                                    (dictdist['district_id'], dictdist['district_name']))
                        except:
                            curr.execute(""" insert into district values (%s,%s)""",
                                         (set_df_did, set_df_dname))
                    set_district_id.add(id_add["district"]["id"])
    conn.commit()
def fetch_address():
    set_company_id = {0}
    set_province_id = 0
    for pageNumber in range(1, LAST_PAGE + 1):
        with open(f"data_job/data_{pageNumber}", 'r', encoding="utf-8") as file:
            parse = json.load(file)
            for company in parse["data"]:
                company_id = company["company"]["id"]
                if (company_id in set_company_id):
                    continue
                else:
                    for id_add in company["company"]["addresses"]["collection_addresses"]:
                        company_address_id = id_add["id"]
                        ward_id = id_add["ward"]["id"]
                        province_id = id_add["province"]["id"]
                        district_id = id_add["district"]["id"]
                        street = id_add["street"]
                        # if(company_address_id in set_company_address_id):
                        #     continue
                        # else:
                        dicadd = dict(company_id = company_id, company_address_id = company_address_id, ward_id = ward_id, province_id = province_id, district_id = district_id, street = street)
                        try:
                            curr.execute(""" insert into address values (%s,%s,%s,%s,%s,%s)""",
                                     (dicadd['company_id'], dicadd['company_address_id'],dicadd['ward_id'], dicadd['province_id'],dicadd['district_id'], dicadd['street']))
                        except:
                            curr.execute(""" insert into address values (%s,%s,%s,%s,%s,%s)""",
                                         (dicadd['company_id'], dicadd['company_address_id'], dicadd['ward_id'],
                                          set_province_id, dicadd['district_id'], dicadd['street']))
                set_company_id.add(company["company"]["id"])
    conn.commit()

fetch_data()
insert_data()
fetch_company()
fetch_salary()
fetch_ward()
fetch_province()
fetch_district()
fetch_job()
fetch_address()
fetch_benefits()