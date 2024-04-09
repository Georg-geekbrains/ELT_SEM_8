from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
from io import StringIO
from datetime import datetime
from airflow.models import Variable

# Функция для адаптации значений None в SQL запросе
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

# Регистрация адаптера для numpy.int64
register_adapter(pd._libs.tslibs.nattype.NaTType, adapt_numpy_int64)

DB_PASSWORD = Variable.get("DB_PASSWORD", deserialize_json=False)
db_params = {
    'database': 'airflow_db',
    'user': 'user1',
    'password': DB_PASSWORD,
    'host': 'localhost',
    'port': '5432'
}


# Функция для чтения CSV файлов
def read_csv(file_path):
    return pd.read_csv(file_path)

# Функция для преобразования даты
def convert_date(date_str):
    if '/' in date_str:
        return pd.to_datetime(date_str, format='%Y/%m/%d')
    elif '-' in date_str:
        return pd.to_datetime(date_str, format='%Y-%m-%d')
    else:
        return pd.NaT

# Функция для объединения данных и преобразований
def transform_data(**kwargs):
    ti = kwargs['ti']
    # Получаем данные из контекста выполнения задач
    client_df = ti.xcom_pull(task_ids='read_client_csv')
    booking_df = ti.xcom_pull(task_ids='read_booking_csv')
    hotel_df = ti.xcom_pull(task_ids='read_hotel_csv')

    # Приведение дат к одному формату
    booking_df['booking_date'] = booking_df['booking_date'].apply(convert_date)

    # Удаление невалидных колонок
    booking_df.dropna(subset=['client_id', 'booking_date', 'room_type', 'hotel_id', 'booking_cost', 'currency'], inplace=True)

    # Объединение таблиц
    merged_df = booking_df.merge(client_df, on='client_id').merge(hotel_df, on='hotel_id')

    # Приведение валют к одной
    currency_map = {'EUR': 'EUR', 'GBP': 'GBP'}
    merged_df['currency'] = merged_df['currency'].map(currency_map)

    # Передача результата в следующую задачу через XCom
    ti.xcom_push(key='transformed_data', value=merged_df)

def load_to_database(**kwargs):
    ti = kwargs['ti']
    # Получение преобразованных данных из XCom
    dataframe = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    # Проверка и удаление лишних колонок
    expected_columns = ['client_id', 'booking_date', 'room_type', 'hotel_id', 'booking_cost', 'currency']
    dataframe = dataframe[expected_columns]
    
    # Создание соединения с базой данных
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Преобразование DataFrame в список кортежей для вставки
    dataframe['booking_date'] = dataframe['booking_date'].dt.strftime('%Y-%m-%d')
    dataframe['client_id'] = dataframe['client_id'].astype(str)
    records = list(dataframe.itertuples(index=False, name=None))

    # Вставка данных в таблицу с использованием операции UPSERT
    table_name = 'transformed_data'
    for record in records:
        insert_query = f"""
        INSERT INTO {table_name} (client_id, booking_date, room_type, hotel_id, booking_cost, currency) 
        VALUES %s
        ON CONFLICT (client_id, booking_date) DO UPDATE
        SET room_type = EXCLUDED.room_type,
            hotel_id = EXCLUDED.hotel_id,
            booking_cost = EXCLUDED.booking_cost,
            currency = EXCLUDED.currency
        """
        psycopg2.extras.execute_values(cursor, insert_query, [record], template=None, page_size=100)

    conn.commit()

    # Закрытие соединения
    cursor.close()
    conn.close()
 


# Создание DAG
dag = DAG('data_processing_dag_8', description='DAG for processing booking data', schedule=None, start_date=datetime(2022, 4, 1), catchup=False)

# Операторы для чтения файлов
read_client_csv = PythonOperator(task_id='read_client_csv', python_callable=read_csv, op_kwargs={'file_path': '~/elt_project/Data/client.csv'}, dag=dag)
read_booking_csv = PythonOperator(task_id='read_booking_csv', python_callable=read_csv, op_kwargs={'file_path': '~/elt_project/Data/booking.csv'}, dag=dag)
read_hotel_csv = PythonOperator(task_id='read_hotel_csv', python_callable=read_csv, op_kwargs={'file_path': '~/elt_project/Data/hotel.csv'}, dag=dag)

# Оператор для трансформации данных
transform_data_operator = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Оператор для загрузки данных в базу данных
load_to_database_operator = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag
)

# Определение порядка выполнения задач
read_client_csv >> transform_data_operator >> load_to_database_operator
read_booking_csv >> transform_data_operator
read_hotel_csv >> transform_data_operator

