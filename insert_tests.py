from db_manager import *
import datetime

conn = create_connection(DATABASE)

if conn is not None:
    hostname = ('www.google.com',)
    insert_hostname(conn, hostname)

    hostname = ('www.amazon.com',)
    insert_hostname(conn, hostname)

    ip = ('1.1.1.1',)
    insert_ip(conn, ip)

    ip = ('1.2.1.1',)
    insert_ip(conn, ip)

    ip = ('1.1.2.1',)
    insert_ip(conn, ip)

    ip_hostname = ('8.8.8.8', 'dns.google.com', datetime.datetime.now())
    insert_ip_hostname(conn, ip_hostname)

    hostname_ip = ('dns.google.com', '8.8.8.8', datetime.datetime.now())
    insert_hostname_ip(conn, hostname_ip)

    conn.close()
else:
    print('DEU M!')

