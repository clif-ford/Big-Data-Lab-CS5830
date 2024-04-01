  
# from bs4 import BeautifulSoup
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# import random
# import os
# import zipfile
# import datetime
# import requests

# def parse_html(num_files):
#     with open('/tmp/data/index.html', 'r') as html_file:
#         soup = BeautifulSoup(html_file, 'html.parser')
#         csv_links = [link['href'] for link in soup.find_all('a') if link['href'].endswith('.csv')]
#         selected_files = random.sample(csv_links, int(num_files))
    
#     with open('/tmp/data/output.txt', 'w') as outfile:
#         outfile.write('\n'.join(str(i) for i in selected_files))
    
#     return selected_files

# def zip_files(year):
#     with zipfile.ZipFile(f'/tmp/data/{year}_data.zip', 'w') as zipf:
#         for foldername, subfolders, filenames in os.walk('/tmp/data/dataset/'):
#             for file in filenames:
#                 zipf.write(os.path.join(foldername, file), arcname=file)

# def move_archive(year, destination):
#     os.system(f"mv /tmp/data/{year}_data.zip {destination}")

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime.datetime(2024, 3, 2),
#     'retries': 0,
# }

# dag = DAG(
#     dag_id='webscrapper',
#     default_args=default_args,
#     schedule_interval=None,
# )

# fetch_html = BashOperator(
#     task_id='fetch_html_page',
#     bash_command="mkdir -p /tmp/data/dataset && wget -O /tmp/data/index.html 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{{ dag_run.conf['year'] }}'",
#     dag=dag,
# )

# select_files = PythonOperator(
#     task_id='parse_html',
#     python_callable=parse_html,
#     op_args=["{{ dag_run.conf['num_files'] }}"],
#     dag=dag,
# )

# download_data = BashOperator(
#     task_id='download_data',
#     bash_command="""
#     while read line; do
#         wget -P /tmp/data/dataset/ "https://www.ncei.noaa.gov/data/local-climatological-data/access/{{ dag_run.conf['year'] }}/$line"
#     done < /tmp/data/output.txt
#     """,
#     dag=dag,
# )

# zip_data = PythonOperator(
#     task_id='zip_data',
#     python_callable=zip_files,
#     op_args=["{{ dag_run.conf['year'] }}"],
#     dag=dag,
# )

# move_data = PythonOperator(
#     task_id='move_data',
#     python_callable=move_archive,
#     op_args=["{{ dag_run.conf['year'] }}", "{{ dag_run.conf['path'] }}"],
#     dag=dag,
# )

# fetch_html >> select_files >> download_data >> zip_data >> move_data

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import zipfile
from bs4 import BeautifulSoup
import requests

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,  # Don't backfill previous runs
}

# Define the DAG
dag = DAG(
    dag_id='weather_data_extraction',
    default_args=default_args,
    description='Extract weather data from NOAA website',
    schedule_interval=None,  # Define the schedule interval as None to execute only once
)

# Function to fetch page
def fetch_page(year):
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
    root = '/opt/airflow/dags/weatherdata/'

    bash_command = f"curl -f {url}{year}/  -o {root}{year} && echo 'Page fetched successfully'"
    return bash_command

# Function to extract links from HTML
def extract_links_from_html(year):
    root = '/opt/airflow/dags/weatherdata/'
    num_stations = 25 

    with open(f'{root}{year}', 'r') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')

    links = [link.get('href') for link in soup.find_all('a', href=True) if link.get('href').endswith('.csv')]

    links = links[:num_stations]

    with open(f'{root}{year}_csvfilelist', 'w') as output_file:
        for link in links:
            output_file.write(f"{link}\n")

# Function to fetch data files
def fetch_datafiles(year):
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
    root = '/opt/airflow/dags/weatherdata/'

    with open(f'{root}{year}_csvfilelist', 'r') as file:
        csv_files = file.read().splitlines()

    for csv_file_url in csv_files:
        total_csv_url = f"{url}{year}/{csv_file_url}"
        response = requests.get(total_csv_url)

        if response.status_code == 200:
            csv_filename = csv_file_url.split('/')[-1]
            csv_file_path = f"{root}{csv_filename[:-4]}_{year}.csv"

            with open(csv_file_path, 'w', newline='') as csvfile:
                csvfile.write(response.text)
            print(f"Saved data from {csv_file_url} to {csv_file_path}")
        else:
            print(f"Failed to fetch data from {csv_file_url}")

# Function to zip CSV files
def zip_csv_files(year):
    root = '/opt/airflow/dags/weatherdata/'

    csv_files = [f for f in os.listdir(root) if f.endswith(f'{year}.csv')]
    with zipfile.ZipFile(f'{root}zip_{year}.zip', 'w') as zipf:
        for csv_file in csv_files:
            file_path = os.path.join(root, csv_file)
            zipf.write(file_path, arcname=csv_file)

# Define tasks
for year in range(1901, 2025):
    fetch_page_task = BashOperator(
        task_id=f'fetch_page_{year}',
        bash_command=fetch_page(year),
        dag=dag,
    )

    select_files_task = PythonOperator(
        task_id=f'select_files_{year}',
        python_callable=extract_links_from_html,
        op_kwargs={'year': year},
        dag=dag,
    )

    fetch_data_task = PythonOperator(
        task_id=f'fetch_data_{year}',
        python_callable=fetch_datafiles,
        op_kwargs={'year': year},
        dag=dag,
    )

    zip_files_task = PythonOperator(
        task_id=f'zip_files_{year}',
        python_callable=zip_csv_files,
        op_kwargs={'year': year},
        dag=dag,
    )

    fetch_page_task >> select_files_task >> fetch_data_task >> zip_files_task
