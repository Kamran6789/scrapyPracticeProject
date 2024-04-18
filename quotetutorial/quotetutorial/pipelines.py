import sqlite3  # Define your item pipelines here

import mysql
import mysql.connector
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import item


class QuotetutorialPipeline:
    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="",
            password="",
            database="myquotes"

        )
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("""drop table if exists quotes_tb""")
        self.curr.execute("""create table quotes_tb(title text,
        author text,
        tag text)""")

    def process_item(self, item, spider):
        self.store_db(item)
        print("Pipeline " + item['title'][0])
        return item

    def store_db(self, item):
        # Extracting all tags as a single string separated by commas
        tags = ', '.join(item['tag'])
        self.curr.execute("""INSERT INTO quotes_tb (title, author, tag)
                             VALUES (%s, %s, %s)""",
                          (item['title'][0],
                           item['author'][0],
                           tags))
        self.conn.commit()


