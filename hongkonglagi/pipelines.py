import pymysql
from scrapy.exceptions import DropItem

class MySQLPipeline:
    def __init__(self, mysql_host, mysql_user, mysql_password, mysql_db):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """This method is called by Scrapy when the spider starts."""
        return cls(
            mysql_host=crawler.settings.get('MYSQL_HOST'),
            mysql_user=crawler.settings.get('MYSQL_USER'),
            mysql_password=crawler.settings.get('MYSQL_PASSWORD'),
            mysql_db=crawler.settings.get('MYSQL_DB')
        )

    def open_spider(self, spider):
        """Open database connection when the spider starts."""
        self.connection = pymysql.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            database=self.mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connection.cursor()
        spider.logger.info("MySQL connection opened.")

        # Create the table if it doesn't exist
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS keluaran (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATETIME,
                first TEXT,
                second TEXT,
                third TEXT
            )
        ''')
        self.connection.commit()

    def close_spider(self, spider):
        """Close the database connection when the spider finishes."""
        if self.connection:
            self.connection.close()
            spider.logger.info("MySQL connection closed.")

    def process_item(self, item, spider):
        """Process each item and save it to MySQL."""
        first_str = ' '.join([f.replace(",", "") for f in item['first']])
        second_str = ' '.join([s.replace(",", "") for s in item['second']])
        third_str = ' '.join([t.replace(",", "") for t in item['third']])

        try:
            # Insert the data into the MySQL table
            self.cursor.execute('''
                INSERT INTO keluaran (date, first, second, third)
                VALUES (%s, %s, %s, %s)
            ''', (item['date'], first_str, second_str, third_str))
            self.connection.commit()
            spider.logger.info(f"Data inserted successfully for date {item['date']}")
        except pymysql.MySQLError as e:
            spider.logger.error(f"Database insert error: {e}")
            raise DropItem(f"Failed to insert item: {item}")
        return item
