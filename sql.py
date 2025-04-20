import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv


def query(query):
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute(query)

    output = []
    for row in cur: output.append(row)
    return output