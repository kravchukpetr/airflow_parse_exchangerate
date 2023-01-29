import datetime as dt
import requests
import clickhouse_connect
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

input_params = {'url_template': 'https://api.exchangerate.host/',
                'currency_from': 'BTC',
                'currency_to': 'USD',
                'is_regular': True,
                'dt_from': None,
                'dt_to': None,
                'host': 'clickhouse',
                'port': 8123,
                'username': 'default',
                'password': '',
                }

def parse_exchangerate(**kwargs):
    """
    Функция получает параметры из конфигурационного словаря kwargs:
    url_template: шаблон url, с которого будет делаться парсинг
    currency_from: базовая валюта
    currency_to: вторая валюта
    is_regular: True - получить текущее значение курса по валютной паре;
                False - загрузить исторические данные по валютной паре за промежуток
                от dt_from до dt_to (формат даты YYYY-MM-DD)
    """

    url_template = kwargs['templates_dict']['url_template']
    currency_from = kwargs['templates_dict']['currency_from']
    currency_to = kwargs['templates_dict']['currency_to']
    is_regular = kwargs['templates_dict']['is_regular']
    dt_from = kwargs['templates_dict']['dt_from']
    dt_to = kwargs['templates_dict']['dt_to']
    host = kwargs['templates_dict']['host']
    port = kwargs['templates_dict']['port']
    username = kwargs['templates_dict']['username']
    password = kwargs['templates_dict']['password']

    
    client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
    run_parse(url_template, currency_from, currency_to, is_regular, dt_from, dt_to, client)
    
def run_parse(url_template, currency_from, currency_to, is_regular, dt_from, dt_to, client, **kwargs):
    """
    Парсинг курса валют через API exchangerate.host и загрузка сырых данных в clickhouse
    """
    # загрузка текущего курса по валютной паре
    if is_regular:
        url = url_template + 'convert?from=' + currency_from + '&to=' + currency_to
        try:
            response = requests.get(url)
            data = response.json()
            if data['result'] is not None:
                # запуск функции сохранения в clickhouse
                save_to_clickhouse(client, currency_from + currency_to,  datetime.strptime(data['date'], '%Y-%m-%d'), data['result'])
        except Exception as e:
            print("Ошибка парсинга по url " + str(url) + ". Ошибка: " + str(e))
    # загрузка исторических данных за промежуток дней
    else:
        # проверка, что задан промежуток для загрузки исторических данных
        if dt_from is not None and dt_to is not None:
            dt_from = datetime.strptime(dt_from, '%Y-%m-%d')
            dt_to = datetime.strptime(dt_to, '%Y-%m-%d')
            # цикл по датам заданного промежутка
            while dt_from <= dt_to:                
                url = url_template + str(dt_from.date()) + '?base=' + currency_from
                try:
                    response_hist = requests.get(url)
                    data_hist = response_hist.json()
                    # проверка, что значение получено по корректной валютной паре
                    if data_hist['base'] == currency_from and currency_to in data_hist['rates']:
                        # запуск функции сохранения в clickhouse
                        save_to_clickhouse(client, currency_from + currency_to, datetime.strptime(data_hist['date'], '%Y-%m-%d'), data_hist['rates'][currency_to])
                except Exception as e:
                    print("Ошибка парсинга по url " + str(url) + ". Ошибка: " + str(e))
                dt_from = dt_from + dt.timedelta(days=1)
        else:
            print('Не заданы даты для загрузки исторических данных')
    print('Выполнен парсинг данных')

def save_to_clickhouse(client, currency, dt, rate, **kwargs):
    """
    Сохранение записи о курсе валют в таблицу "сырых" данных в clickhouse
    """

    data = [[currency, dt,  rate]]
    client.insert('exchange.input_rates', data, column_names=['currency', 'timestamp', 'rate'])

def transform_in_clickhouse(**kwargs):
    """
    Преобразование "сырых" данных в clickhouse в оптимизированную таблицу
    """
    
    host = kwargs['templates_dict']['host']
    port = kwargs['templates_dict']['port']
    username = kwargs['templates_dict']['username']
    password = kwargs['templates_dict']['password']
    currency_from = kwargs['templates_dict']['currency_from']
    currency_to = kwargs['templates_dict']['currency_to']

    client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)

    # Получение списка партиций, которые необходимо перегрузить в целевую таблицу(exchange.rates)
    result = client.query("""WITH  rawParts AS (
                                        SELECT timestamp, MAX(insertTime) maxInsertTime
                                        FROM exchange.input_rates
                                        WHERE insertTime > toStartOfMinute(now()) - INTERVAL 3 HOUR
                                        GROUP BY timestamp),
                                    cleanParts AS (
                                        SELECT timestamp, MAX(insertTime) maxInsertTime
                                        FROM exchange.rates
                                        WHERE timestamp IN (SELECT timestamp FROM rawParts)
                                        GROUP BY timestamp)
                                    SELECT DISTINCT timestamp FROM rawParts
                                    LEFT JOIN cleanParts USING timestamp
                                    WHERE rawParts.maxInsertTime > cleanParts.maxInsertTime
                                    ORDER BY timestamp""")
    # Цикл по партициям, которые необходимо перегрузить
    for part_key in result.result_rows:
        # Удаление партиции из вспомогательной таблицы (tmp_rates)
        client.command("ALTER TABLE exchange.tmp_rates DROP PARTITION '" + part_key[0].strftime('%Y-%m-%d') + "'")
        # Вставка данных по партиции в вспомогательную таблицу (tmp_rates):
        # Выбираем данные из таблицы сырых данных (input_rates) и объединяем их с данными целеовй таблицы (exchange.rates)
        # Для каждой валютной пары и даты (currency + timestamp) оставляем только последнуюю запись по дате добавления (insertTime)
        client.command(f""" INSERT INTO exchange.tmp_rates(currency, timestamp, rate, insertTime)
                            SELECT *
                            FROM (
                                SELECT currency, timestamp, rate, insertTime FROM exchange.input_rates r WHERE timestamp = '{part_key[0].strftime('%Y-%m-%d')}'
                                UNION ALL
                                SELECT currency, timestamp, rate, insertTime FROM exchange.rates WHERE timestamp = '{part_key[0].strftime('%Y-%m-%d')}'
                                )
                                ORDER BY currency, timestamp, insertTime DESC
                                LIMIT 1 BY currency, timestamp""")
        # Удаляем партицию из целевой таблицы
        client.command("ALTER TABLE exchange.rates DROP PARTITION '" + part_key[0].strftime('%Y-%m-%d') + "'")
        # Перемещаем партицию из вспомогательной таблицы в целевую таблицу
        client.command("ALTER TABLE exchange.tmp_rates MOVE PARTITION '" + part_key[0].strftime('%Y-%m-%d') + "' TO TABLE exchange.rates")

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 1, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=5),
}

with DAG('airflow_parse_exchangerate',
         default_args=default_args,
         schedule_interval='0 */3 * * *',
         ) as dag:

    parse_exchangerate = PythonOperator(task_id='parse_exchangerate',
                                 python_callable=parse_exchangerate,
                                 templates_dict=input_params,
                                 provide_context=True,
                                 dag=dag,)
    transform_in_clickhouse = PythonOperator(task_id='transform_in_clickhouse',
                                 python_callable=transform_in_clickhouse,
                                 templates_dict=input_params,
                                 provide_context=True,
                                 dag=dag,)


parse_exchangerate >> transform_in_clickhouse
