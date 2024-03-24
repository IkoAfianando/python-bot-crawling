import csv
import requests
import time
import tqdm
import warnings
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

URL_BASE = 'https://rekrutmenbersama2024.fhcibumn.id'
URL_JOB = f'{URL_BASE}/job'
URL_LOAD_RECORD = f'{URL_BASE}/job/loadRecord'
URL_GET_DETAIL = f'{URL_BASE}/job/get_detail_vac'

headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
}


def requests_all_job():
    session = requests.Session()
    soupjob = BeautifulSoup(session.get(URL_JOB, headers=headers, verify=False).content, 'html.parser')
    csrftoken = soupjob.find('input', dict(name='csrf_fhci'))['value']
    jobs = session.post(
        URL_LOAD_RECORD,
        data=dict(csrf_fhci=csrftoken, company='all'),
        headers=headers,
        verify=False
    )
    return jobs.json()


def parse_to_csv(data, path):
    keys = data[0].keys()
    with open(path, 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


def get_detail_jobs(job_id):
    session = requests.Session()
    soupjob = BeautifulSoup(session.get(URL_JOB, headers=headers, verify=False).content, 'html.parser')
    csrftoken = soupjob.find('input', dict(name='csrf_fhci'))['value']
    detail = session.post(
        URL_GET_DETAIL,
        data=dict(csrf_fhci=csrftoken, id=job_id),
        headers=headers,
        verify=False
    )
    if detail.status_code == 200:
        return detail.json()
    return None


def get_all_details(vacant_ids):
    data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_detail_jobs, id): id for id in vacant_ids}
        for future in tqdm.tqdm(futures):
            try:
                datum = future.result()
                if datum:
                    data.append(datum)
            except Exception as e:
                print("Err.. ", e)
                continue
    return data


if __name__ == '__main__':
    jobs = requests_all_job()
    print(len(jobs['data']['result']))
    parse_to_csv(jobs['data']['result'], 'data/all_jobs.csv')
    print("Sleeping...")
    time.sleep(5)  # Add a delay if necessary
    if 'data' in jobs and 'result' in jobs['data']:
        vacant_ids = [row['vacancy_id'] for row in jobs['data']['result']]
        print("Total Vacancies:", len(vacant_ids))
        details = get_all_details(vacant_ids)
        parse_to_csv(details, 'data/details.csv')
    else:
        print("No data found in the response.")