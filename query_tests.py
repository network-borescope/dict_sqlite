from db_manager import *
import datetime

DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"
conn = create_connection(DATABASE)

if conn is not None:

    result = select_hostname_from_ip(conn, ('8.8.8.8',))
    print(result)

    result = select_ip_from_hostname(conn, ('dns.google.com',))
    print(result)

    result = select_host_ip_id(conn, '1.1.1.1')
    print(result)

    result = select_host_ip_id(conn, '1.2.1.1')
    print(result)

    result = select_host_ip_id(conn, '1.3.1.1')
    print(result)

    result = get_or_insert_ip(conn, "1.3.1.1")
    print(result)

    result = select_host_ip_id(conn, '1.3.1.1')
    print(result)

    result = get_or_insert_ip(conn, "1.3.1.1")
    print(result)

    conn.close()
else:
    print('DEU M!')

