#!/usr/bin/env python3

import psycopg2

Database_Name = "news"
Db = psycopg2.connect(database=Database_Name)
Cursor = Db.cursor()


def popular3_articles():
    print ("1. What are the most popular three articles of all time?")
    Cursor.execute("select A.slug, count(L.path) as views from log L "
                   "join articles A on L.path ILIKE concat('%', A.slug, '%') "
                   "where L.status ILIKE concat('%','200','%') group by A.slug"
                   " order by count(L.path) desc limit 3;")
    articles = Cursor.fetchall()
    for _article in articles:
        print("Article slug is: " + str(_article[0]),
              str(_article[1]) + " views.")


def popular_authors():
    print ("2. Who are the most popular article authors of all time?")
    Cursor.execute("select Ath.name, count(L.path) as views from log L join "
                   "articles A on L.path ILIKE concat('%', A.slug, '%') join "
                   "authors Ath on A.author = Ath.id "
                   "where L.status ILIKE concat('%','200','%') "
                   "group by Ath.name "
                   "order by count(L.path) desc;")
    authors = Cursor.fetchall()
    for _author in authors:
        print("Author: " + str(_author[0]), str(_author[1]) + " views.")


def bad_requests_percentage():
    print("3. On which days did more than 1% of requests lead to errors?")
    Cursor.execute("select     date(time) as Day, "
                   "(count( case when status not ilike '%ok%' then 1 "
                   "else null end) / cast(count(date(time)) as float))*100 "
                   "as percentage from log group by Day having cast "
                   "(((count( case when status not ilike '%ok%' then 1 else "
                   "null end) / cast(count(date(time)) as float))*100) "
                   "as integer) > 1 order by Day;")
    requests = Cursor.fetchall()
    for request in requests:
        print("Request day: "+str(request[0]), "Error Rate: " +
              str(round(request[1], 2)))


if __name__ == '__main__':
    popular3_articles()
    popular_authors()
    bad_requests_percentage()
    Db.close()
