from sqlite3.dbapi2 import IntegrityError
from db_manager import *
import datetime

DATABASE = r"C:\sqlite\db\syn_dns_ether.db"
conn = create_connection(DATABASE)

if conn is not None:
    try:
        hostname = 'www.google.com'
        insert_hostname(conn, hostname)
    except IntegrityError as e:
        print(e)

    try:
        hostname = 'www.amazon.com'
        insert_hostname(conn, hostname)
    except IntegrityError as e:
        print(e)


    try:
        ip = '1.1.1.1'
        insert_ip(conn, ip)
    except IntegrityError as e:
        print(e)

    try:
        ip = '1.2.1.1'
        insert_ip(conn, ip)
    except IntegrityError as e:
        print(e)

    try:
        ip = '1.1.2.1'
        insert_ip(conn, ip)
    except IntegrityError as e:
        print(e)

    insert_ip_hostname(conn, '8.8.8.8', 'dns.google.com', datetime.datetime.now())

    insert_hostname_ip(conn, 'dns.google.com', '8.8.8.8', datetime.datetime.now())

    conn.close()
else:
    print('DEU M!')

