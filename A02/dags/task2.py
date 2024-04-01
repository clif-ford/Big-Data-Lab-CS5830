# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.contrib.sensors.file_sensor import FileSensor
# import random
# import os
# import requests
# from shutil import move
# import apache_beam as beam
# import pandas as pd
# import geopandas as gpd
# import matplotlib.pyplot as plt

# current_year = datetime.now().year

# # Define default_args, schedule_interval, and other DAG parameters
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'weather_data_pipeline',
#     default_args=default_args,
#     description='A pipeline to fetch, process, and visualize weather data',
#     schedule_interval=timedelta(minutes=1),
# )

# # Tasks from the previous snippet

# # Task 3: Process CSV files using Apache Beam
# def process_csv_files():
#     def extract_data(element):
#         fields = element.split(',')
#         return {'Lat': fields[0], 'Lon': fields[1], 'WindSpeed': fields[2], 'Temperature': fields[3]}

#     with beam.Pipeline() as pipeline:
#         (pipeline 
#          | 'Read CSV' >> beam.io.ReadFromText(f'./data/{current_year}/weather_data.csv')
#          | 'Parse CSV' >> beam.Map(extract_data)
#          | 'Write to DataFrame' >> beam.io.WriteToText(f'./data/{current_year}/processed_data'))

# process_csv_task = PythonOperator(
#     task_id='process_csv_files',
#     python_callable=process_csv_files,
#     dag=dag,
# )

# # Task 4: Compute monthly averages
# def compute_monthly_averages():
#     def compute_avg(element):
#         # Assuming element contains monthly data
#         return {'Lat': element['Lat'], 'Lon': element['Lon'], 'AvgWindSpeed': sum(element['WindSpeed']) / len(element['WindSpeed']), 'AvgTemperature': sum(element['Temperature']) / len(element['Temperature'])}

#     with beam.Pipeline() as pipeline:
#         (pipeline 
#          | 'Read Processed Data' >> beam.io.ReadFromText(f'./data/{current_year}/processed_data')
#          | 'Compute Averages' >> beam.Map(compute_avg)
#          | 'Write Averages' >> beam.io.WriteToText(f'./data/{current_year}/monthly_averages'))

# compute_averages_task = PythonOperator(
#     task_id='compute_monthly_averages',
#     python_callable=compute_monthly_averages,
#     dag=dag,
# )

# # Task 5: Create heatmaps
# def create_heatmaps():
#     df = pd.read_csv(f'./data/{current_year}/monthly_averages.csv')
#     gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Lon, df.Lat))

#     fig, ax = plt.subplots()
#     gdf.plot(ax=ax, kind='scatter', x='Lon', y='Lat', c='AvgTemperature', cmap='YlOrRd', alpha=0.6, edgecolor='k')
#     plt.title('Average Monthly Temperature')
#     plt.savefig(f'./data/{current_year}/temperature_heatmap.png')

# create_heatmaps_task = PythonOperator(
#     task_id='create_heatmaps',
#     python_callable=create_heatmaps,
#     dag=dag,
# )



from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
import pandas as pd
import geopandas as gpd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_visualization_pipeline',
    default_args=default_args,
    description='Pipeline for data visualization using Apache Beam',
    schedule_interval='*/1 * * * *',  # Run every minute
)

# Task 1: Wait for the archive to be available
file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath='/opt/airflow/dags/weatherdata/zip_file.zip',
    poke_interval=5,
    timeout=5,
    retries=3,
    dag=dag,
)

# Task 2: Unzip the archive
unzip_task = BashOperator(
    task_id='unzip_task',
    bash_command='unzip -o /opt/airflow/dags/weatherdata/zip_file.zip -d /opt/airflow/dags/weatherdata/output_dir/',
    dag=dag,
)

# Task 3: Extract and filter data using Apache Beam
def extract_and_filter_data(input_file, output_file, required_fields):
    df = pd.read_csv(input_file)
    # Filter dataframe based on required fields
    filtered_df = df[required_fields]
    filtered_df.to_csv(output_file, index=False)

extract_and_filter_data_task = PythonOperator(
    task_id='extract_and_filter_data_task',
    python_callable=extract_and_filter_data,
    op_kwargs={'input_file': '/opt/airflow/dags/weatherdata/input_file.csv',
               'output_file': '/opt/airflow/dags/weatherdata/filtered_output_file.csv',
               'required_fields': ['Windspeed', 'BulbTemperature']},
    dag=dag,
)

# Task 4: Compute monthly averages
def compute_monthly_averages(input_file, output_file):
    # Implement Apache Beam pipeline to compute monthly averages
    pass

compute_monthly_averages_task = PythonOperator(
    task_id='compute_monthly_averages_task',
    python_callable=compute_monthly_averages,
    op_kwargs={'input_file': '/opt/airflow/dags/weatherdata/filtered_output_file.csv',
               'output_file': '/opt/airflow/dags/weatherdata/monthly_averages.csv'},
    dag=dag,
)

# Task 5: Create data visualization using geopandas
def create_data_visualization(input_file):
    # Create visualization using geopandas
    pass

create_data_visualization_task = PythonOperator(
    task_id='create_data_visualization_task',
    python_callable=create_data_visualization,
    op_kwargs={'input_file': '/opt/airflow/dags/weatherdata/monthly_averages.csv'},
    dag=dag,
)

# Task 6: Create GIF animation (Optional)
create_gif_animation_task = BashOperator(
    task_id='create_gif_animation_task',
    bash_command='ffmpeg -i /opt/airflow/dags/weatherdata/png_images/*.png /opt/airflow/dags/weatherdata/output_animation.gif',
    dag=dag,
)

# Task 7: Delete CSV file
delete_csv_file_task = BashOperator(
    task_id='delete_csv_file_task',
    bash_command='rm /opt/airflow/dags/weatherdata/filtered_output_file.csv',
    dag=dag,
)

# Define task dependencies
file_sensor_task >> unzip_task >> extract_and_filter_data_task >> compute_monthly_averages_task
compute_monthly_averages_task >> create_data_visualization_task
create_data_visualization_task >> create_gif_animation_task
create_data_visualization_task >> delete_csv_file_task
