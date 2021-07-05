import sqlite3
from sqlite3 import Error

DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_hostname(conn, hostname):
    """
    Create a new hostname into the hostname table
    :param conn:
    :param hostname:
    :return: hostname id
    """
    sql = ''' INSERT INTO hostname(hostname)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, hostname)
    conn.commit()
    return cur.lastrowid

def insert_ip(conn, ip):
    """
    Create a new task
    :param conn:
    :param ip:
    :return:
    """

    sql = ''' INSERT INTO host_ip(ip)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, ip)
    conn.commit()

    return cur.lastrowid

def insert_hostname_ip(conn, hostname_ip):
    """
    Create a new task
    :param conn:
    :param hostname_ip:
    :return:
    """

    sql = ''' INSERT INTO hostname_host_ip(hostname, ip, validade)
              VALUES(?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, hostname_ip)
    conn.commit()

    return cur.lastrowid

def insert_ip_hostname(conn, ip_hostname):
    """
    Create a new task
    :param conn:
    :param ip_hostname:
    :return:
    """

    sql = ''' INSERT INTO host_ip_hostname(ip, hostname, validade)
              VALUES(?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, ip_hostname)
    conn.commit()

    return cur.lastrowid

def create_database():
    sql_create_hostname = """ CREATE TABLE IF NOT EXISTS hostname (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        hostname TEXT NOT NULL UNIQUE,
                                        count INT
                                    ); """

    sql_create_host_ip = """CREATE TABLE IF NOT EXISTS host_ip (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    ip TEXT NOT NULL UNIQUE,
                                    count INT
                                );"""
    
    sql_create_hostname_host_ip = """CREATE TABLE IF NOT EXISTS hostname_host_ip (
                                    hostname TEXT,
                                    ip TEXT,
                                    validade TIMESTAMP,

                                    CONSTRAINT PK_hostname_host_ip PRIMARY KEY (hostname, ip)
                                );"""

    sql_create_host_ip_hostname = """CREATE TABLE IF NOT EXISTS host_ip_hostname (
                                    ip TEXT,
                                    hostname TEXT,
                                    validade TIMESTAMP,

                                    CONSTRAINT PK_host_ip_hostname PRIMARY KEY (ip, hostname)
                                );"""                                

    # create a database connection
    conn = create_connection(DATABASE)

    # create tables
    if conn is not None:
        # create hostname table
        create_table(conn, sql_create_hostname)

        # create host_ip table
        create_table(conn, sql_create_host_ip)

        # create hostname_host_ip table
        create_table(conn, sql_create_hostname_host_ip)

        # create host_ip_hostname table
        create_table(conn, sql_create_host_ip_hostname)

        conn.close()
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    create_database()