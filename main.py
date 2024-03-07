import numpy as np
import requests
import psycopg2
from psycopg2 import Error
import matplotlib.pyplot as plt
import datetime
from scipy.interpolate import make_interp_spline


# Класс для получения статистики и заброса ее в БД
class SaleStats:
    def __init__(self, wb_key):
        self.__wb_key = wb_key
        self.stats = self.getStats()
        if self.stats == -1:
            print("Не удалось получить статистику")
            exit(-1)
        else:
            print("Статистика успешно получена \n")

        if self.createTable() == -1:
            exit(-2)
        else:
            print("Таблица успешно создана \n")
        if self.insertData() == -1:
            exit(-3)
        else:
            print("Данные успешно загружены \n")

    def getStats(self):
        request = requests.get(f"https://statistics-api-sandbox.wildberries.ru/api/v1/supplier/sales",
                               headers={"Authorization": self.__wb_key},
                               params={"dateFrom": "2024-01-01"})
        if request.status_code != 200:
            return -1
        return request.json()

    def createTable(self):
        try:
            connection = psycopg2.connect(
                host='rc1b-9j8idwpeb6bwffv0.mdb.yandexcloud.net',
                user='test',
                password='testvslbi321',
                database='test-vsl',
                port="6432")
            cursor = connection.cursor()
            if connection:
                print("Соединение с PostgreSQL успешно установлено")

            query = '''DROP TABLE IF EXISTS sales '''
            cursor.execute(query)
            connection.commit()

            query = '''
                CREATE TABLE IF NOT EXISTS sales (
                    id SERIAL PRIMARY KEY,
                    date VARCHAR(255) NOT NULL,
                    forPay DECIMAL(10, 2)
                );
            '''

            cursor.execute(query)
            connection.commit()

            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
                return 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            return -1

    def insertData(self):
        try:
            connection = psycopg2.connect(
                host='rc1b-9j8idwpeb6bwffv0.mdb.yandexcloud.net',
                user='test',
                password='testvslbi321',
                database='test-vsl',
                port="6432")
            cursor = connection.cursor()
            if connection:
                print("Соединение с PostgreSQL успешно установлено")

            for item in self.stats:
                query = """ INSERT INTO sales (date, forPay) VALUES (%s,%s)"""
                values = (item["date"][0:10], item["forPay"])

                cursor.execute(query, values)
                connection.commit()

            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
                return 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            return -1


# Класс для визуализации данных
class Analysis:
    def __init__(self):
        self.data = self.select()
        if self.data == -1:
            exit(-1)
        else:
            print("Данные успешно получены из БД \n")

        self.data_grouped = self.groupByWeek(sorted(self.data))
        self.drawGraph(self.data_grouped)

    def select(self):
        try:
            connection = psycopg2.connect(
                host='rc1b-9j8idwpeb6bwffv0.mdb.yandexcloud.net',
                user='test',
                password='testvslbi321',
                database='test-vsl',
                port="6432")
            cursor = connection.cursor()
            if connection:
                print("Соединение с PostgreSQL успешно установлено")
            query = '''
                SELECT date, forPay FROM sales;
            '''
            cursor.execute(query)
            records = cursor.fetchall()
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
                return records
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            return -1

    def drawGraph(self, data):
        plt.rcParams.update({'font.size': 8})
        x = []
        y = []
        for item in data:
            x.append(item[0].replace("2023", "23").replace("2024", "24"))
            y.append(item[1])
        plt.bar(x, y)
        plt.title('Выручка')
        plt.legend(["Выручка за неделю"])
        plt.xlabel('Неделя')
        plt.ylabel('Выручка в рублях')
        plt.grid(axis='y')
        plt.xticks(rotation=30, ha='right')
        plt.show()

        x = []
        y = []
        for item in data:
            x.append(item[0].replace("2023", "23").replace("2024", "24"))
            y.append(item[1])

        plt.plot(x, y, 'go')

        X_Y_Spline = make_interp_spline(range(0, len(data)), y)
        X_ = np.linspace(0, len(data), 500)
        Y_ = X_Y_Spline(X_)

        plt.plot(X_, Y_, 'g-')
        plt.title('Выручка')
        plt.legend(["Выручка за неделю"])
        plt.xlabel('Неделя')
        plt.ylabel('Выручка в рублях')
        plt.grid(axis='y')
        plt.xticks(rotation=30, ha='right')
        plt.show()



    def groupByWeek(self, data):
        result_x = []
        result_y = []
        datetime.datetime = datetime.datetime.strptime(data[0][0], "%Y-%m-%d")
        curr_day = datetime.datetime.weekday()
        sum = 0
        start_day = data[0][0].replace("-", ".")
        count_days = curr_day
        for item in data:
            sum += item[1]
            count_days += 1
            if count_days == 7:
                count_days = 0
                result_y.append(sum)
                result_x.append(start_day.replace("-", ".") +" - "+item[0].replace("-", "."))
                sum = 0
            if count_days == 1:
                start_day = item[0]
        result_y.append(sum)
        result_x.append(start_day.replace("-", ".") + " - " + data[-1][0].replace("-", "."))
        return list(zip(result_x, result_y))


SaleStats("eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQwMjI2djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcyNTQ2NDM5NSwiaWQiOiI2ZGUyZDYyMi01NjIzLTRlYzgtYjYwZi05ZTk4ZGZmOTA5YzgiLCJpaWQiOjU2MDkyMDU1LCJvaWQiOjQyMDI4NCwicyI6MCwic2lkIjoiYzczYzA3YzQtNDQxNS00NGQ1LTk3OTUtNmE2NmViZTExMzM3IiwidCI6dHJ1ZSwidWlkIjo1NjA5MjA1NX0.owH8LZmbVWWBqzFzqWbfO2ChIk34InuUXbKkSxFT5hMU9BNtYdNx40nuv1nraSRKguqx5ECRSpaw1osEcX_Isw")
Analysis()
