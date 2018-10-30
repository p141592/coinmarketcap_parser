import argparse
import json
import os
import time
from datetime import datetime
import psycopg2
import requests

e = os.environ.get

FALSE_VALUE = None

# Обязательные поля. Будут пустой строкой, если не было в источнике
required_fields = (
    'max_supply', 'percent_change_7d', 'percent_change_24h', 'percent_change_1h', '24h_volume_usd', 'name',
)

# Преобразование названий филдов источника в наши названия
fields_map = {
    '24h_volume_usd': 'h_volume_usd'
}

# Поля, которые нужно исключить
exclude_fields = ('', )

data_update = str(datetime.now())


def get_data(donor):
    r = requests.get(donor)
    if r.status_code != 200:
        raise AssertionError('Something wrong with {}'.format(donor))
    return r.json()


def prepare_data(func):
    def wrap(*args, data):
        # Преобразовываем данные перед отправкой в базу
        # Получаем dict, проверяем на не пустое значение, на обязательное поле и на исключение
        result = dict()
        for k, v in data.items():
            if (v or (k in required_fields)) and (k not in exclude_fields):
                # Если ключ есть в fields_map -> преобразовываем
                # Фолси значение преобразовываем в наше FALSE_VALUE
                result[k if k not in fields_map else fields_map.get(k)] = v if v else FALSE_VALUE

            # Условия не выполнелись и значение пустое - пропускаем это поле

        result['data_update'] = data_update
        return func(*args, result)
    return wrap


class Parser:
    def __init__(self, **kwargs):
        self.debug = kwargs.get('debug')
        self.donor = kwargs.get('donor')
        self.table_name = kwargs.get('table_name')
        self.max_queue_length = kwargs.get('max_queue_length')

        self.db_name = kwargs.get('db_name')
        self.db_password = kwargs.get('db_name')
        self.db_host = kwargs.get('db_name')
        self.db_user = kwargs.get('db_name')

        self.settings_file = kwargs.get('setting.py')

        self.connect = None
        self.queue = []
        self.cursor = None

    def get_cursor(self):
        if not self.cursor:
            self.cursor = self.get_connect().cursor()

        return self.cursor

    def get_connect(self):
        if not self.connect:
            stop_len = 30
            current_len = 0
            while current_len < stop_len:
                try:
                    conn = psycopg2.connect(self.gen_credentials_str())
                except psycopg2.Error as err:
                    time.sleep(10)
                    print("Connect failed: {}".format(err))
                    current_len += 1
                    continue

                self.connect = conn
                break

            else:
                raise AssertionError("Can't connect to base")

        return self.connect

    def gen_credentials_str(self):
        return "dbname='{db}' user='{login}' host='{host}' password='{password}'".format(**self.get_credentials())

    def get_credentials(self):
        if all([self.db_name, self.db_password, self.db_host, self.db_user]):
            return dict(
                db=self.db_name,
                login=self.db_user,
                host=self.db_host,
                password=self.db_password
            )
        try:
            credentials = open(self.settings_file)
        except IOError as e:
            raise AssertionError("{} file load problems {}".format(self.settings_file, e))
        return json.load(credentials)

    def make_query(self, data):
        if data:
            return '''INSERT INTO {} ("{}") VALUES ({});''' \
                .format(self.table_name, '",  "'.join(data.keys()), ', '.join(data.values()))

    @prepare_data
    def push_to_queue(self, data: dict=None):
        query = self.make_query(data)
        if query:
            self.queue.append(query)
            if len(self.queue) == self.max_queue_length:
                self.make_request()

    def make_request(self):
        while self.queue:
            self.send_query(self.queue.pop())

        if not self.debug:
            self.commit()
        else:
            print('='*50 + '\nCOMMIT')

    def send_query(self, data):
        if not self.debug:
            self.get_cursor().execute(data)
        else:
            print(data)

    def commit(self):
        self.cursor.commit()

    def close(self):
        if self.queue:
            self.make_request()

        if self.connect:
            self.connect.close()
            self.cursor = None
            self.connect = None

    def run(self):
        try:
            for i in get_data(self.donor):
                self.push_to_queue(data=i)

        finally:
            self.close()


def main(kwargs):
    Parser(**kwargs).run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--debug', default=True)
    parser.add_argument('--donor', default=e('DONOR', 'https://api.coinmarketcap.com/v1/ticker/?limit=10000'))

    parser.add_argument('--table_name', default=e('TABLE_NAME', 'coinmarketcap'))
    parser.add_argument('--max_queue_length', default=e('MAX_QUEUE_LENGTH', 1000)) # Максимальное количество INSERT в одном запросе

    parser.add_argument('--db_name', default=e('DB_NAME', ''))
    parser.add_argument('--db_password', default=e('DB_PASSWORD', ''))
    parser.add_argument('--db_host', default=e('DB_HOST', ''))
    parser.add_argument('--db_user', default=e('DB_USER', ''))

    parser.add_argument('--settings_file', default=e('SETTINGS_FILE', 'settings.json')) # Если нет env переменных, берем данные коннекта отсюда
    """{"login": "", "host": "", "password": "", "db": ""}"""  # Что должно быть в этом файле

    main(parser.parse_args().__dict__)
