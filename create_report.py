#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sys
import datetime
import psycopg2
import bleach


def get_top3articles():
    """Return top 3 mot viewed articles from the 'news database'."""
    db = psycopg2.connect(dbname="news")
    c = db.cursor()
    query = """select a.title, count(b.path) as total_views
            from
            (select title, '/article/' || slug as the_path from articles )  a
            inner join log b on a.the_path = b.path
            group by a.title,b.path order by total_views desc limit 3;"""
    c.execute(query)
    rows = c.fetchall()
    ARTICLES = []
    for row in rows:
        ARTICLES.append((bleach.clean(row[0]), bleach.clean(row[1])))

    db.close()
    return ARTICLES


def get_TopViewAuthors():
    """Return top 3 mot viewed articles from the 'news database'."""
    db = psycopg2.connect(dbname="news")
    c = db.cursor()
    query = """select c.name, count(*) as total_views
    from (select author, '/article/' || slug as the_path from articles )  a
    inner join log b
    on a.the_path = b.path
    inner join authors c on a.author =c.id
    group by c.name
    order by total_views desc ;"""
    c.execute(query)
    rows = c.fetchall()
    AUTHORS = []
    for row in rows:
        AUTHORS.append((bleach.clean(row[0]), bleach.clean(row[1])))

    db.close()
    return AUTHORS


def get_ErrorLogReport():
    """Return the record that
    more than 1% of requests lead to errors in 'news database'."""
    db = psycopg2.connect(dbname="news")
    c = db.cursor()
    query = """select to_char(date,'MONTH DD, YYYY'),
            (cast((100* cast(error_count as decimal)/count)
            as decimal(18,2))) ||'%' as error_percentage
            from
            (select date(time) as date,
             SUM(CASE WHEN status <> '200 OK'
                        THEN 1 ELSE 0 END) AS error_count,
             SUM(CASE WHEN status = '200 OK'
                        THEN 1 ELSE 0 END) AS ok_count,
            count(*) as count from log group by date(time) ) a
            where cast ((100* cast(error_count as decimal)/count)
            as decimal(18,2))>1;"""
    c.execute(query)
    rows = c.fetchall()
    ERROR_REPORT = []
    for row in rows:
        ERROR_REPORT.append((bleach.clean(row[0]), bleach.clean(row[1])))

    db.close()
    return ERROR_REPORT



def generate_report1():
    REPORT_CONTENT = '''\"%s" - %s views\n'''
    final_content = "".join(REPORT_CONTENT % (title, views)
                            for title, views in get_top3articles())
    return final_content


def generate_report2():
    REPORT_CONTENT = '''%s - %s views\n'''
    final_content = "".join(REPORT_CONTENT % (author_name, views)
                            for author_name, views in get_TopViewAuthors())
    return final_content


def generate_report3():
    REPORT_CONTENT = '''%s - %s errors\n'''
    final_content = "".join(REPORT_CONTENT % (date, error_rate)
                            for date, error_rate in get_ErrorLogReport())
    return final_content


def format_report(title, content):

    dividline1 = ("-----------------------------"
                  "-----------------------------\n")
    dividline2 = ("============================="
                  "=============================\n")
    report_format_result = "%s%s%s%s\n" % (title.upper(), dividline1,
                                           content, dividline2)
    return report_format_result


def create():
    try:
        file = open("report.txt", "w")
        report_title1 = ("What are the most popular three "
                         "articles of all time?\n")
        result1 = format_report(report_title1, generate_report1())
        print(result1)
        file.write(result1)

        report_title2 = ("Who are the most popular article "
                         "authors of all time?\n")
        result2 = format_report(report_title2, generate_report2())
        print(result2)
        file.write(result2)

        report_title3 = ("On which days did more than "
                         "1% of requests lead to errors?\n")
        result3 = format_report(report_title3, generate_report3())
        print(result3)
        file.write(result3)

        file.close()
    except Exception as e:
        print(e)
        print("error occured")
        sys.exit(0)

create()
