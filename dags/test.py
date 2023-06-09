from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import mysql.connector
from airflow import DAG

url = 'https://finance.naver.com/sise/lastsearch2.nhn'
response = requests.get(url)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table', {'class': 'type_5'})
rows = table.tbody.find_all('tr')

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="crawling_data"
)

mycursor = mydb.cursor()

# Create a new table for the stock price data
mycursor.execute("CREATE TABLE IF NOT EXISTS korea_top1000_stocks (id INT primary key auto incremenet, rank INT, company VARCHAR(255), price INT, create_date DATETIME)")

for row in rows:
    cols = row.find_all('td')
    if len(cols) > 1:
        rank = cols[0].text.strip()
        company = cols[1].text.strip()
        price = cols[2].text.strip().replace(",", "")
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "INSERT INTO korea_top1000_stocks (create_date, rank, company, price) VALUES (%s, %s, %s, %s)"
        val = (date, rank, company, price)

        mycursor.execute(sql, val)
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")