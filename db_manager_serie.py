import sqlite3
import datetime
from sqlite3 import Error

BEGIN = "2021-07-19 00:09:00"
END = "2021-07-19 09:09:00"
SIZE_SERIE_TEMPORAL = 9 # 9 horas (2021-07-19 00:09:00 ate 2021-07-19 09:09:00)

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        create_database(db_file, conn)
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



def create_database(DATABASE, conn = None):
    sql_create_serie_temp = """ CREATE TABLE IF NOT EXISTS serie_temporal (
                                        ip_src TEXT,
                                        dist_src TEXT,
                                        servico TEXT,
                                        id_dst INTEGER,
                                        cr_time timestamp NOT NULL,
                                        last_mod timestamp NOT NULL,
                                        st TEXT NOT NULL,
                                        CONSTRAINT PK_serie_temp PRIMARY KEY (ip_src, dist_src, servico, id_dst)
                                    ); """


    close_at_end = False
    # create a database connection
    if conn is None:
        conn = create_connection(DATABASE)
        close_at_end = True

    # create tables
    if conn is not None:
        # create serie_temporal table
        create_table(conn, sql_create_serie_temp)

        if close_at_end: conn.close()
    else:
        print("Error! cannot create the database connection.")


def select_row_from_serie_temp(conn, ip, dist, serv, id ):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT * FROM serie_temporal WHERE ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0]

def check_if_row_exists(conn, ip, dist, serv, id):
    sql = ''' SELECT 1 FROM serie_temporal WHERE ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0][0]

def select_st_from_serie_temp(conn, ip, dist, serv, id ):
    """
    Query tasks by st_array
    :param conn:
    :param ip
    :param dist
    :param serv
    :param id
    :return: array serie temporal
    """

    sql = ''' SELECT st FROM serie_temporal WHERE ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    st = res[0][0]

    d_begin = datetime.datetime.strptime(BEGIN, "%Y-%m-%d %H:%M:%S")

    st_array = [0 for x in range(SIZE_SERIE_TEMPORAL+1)]

    for string_tupla in st.split(";"):
        tupla = string_tupla.split(",")

        datetime_tupla =  datetime.datetime.strptime(tupla[0], "%Y-%m-%d %H:%M:%S")
        delta = datetime_tupla - d_begin
        st_array_pos = delta.seconds // 3600 # quantas horas apos o inicio comeca a serie temporal
        st_array[st_array_pos] += int(tupla[1])

    return st_array

def update_st(conn, ip, dist, serv, id, new_st, last_mod):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' UPDATE serie_temporal SET st = st || ';' || ?, last_mod = ? WHERE ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (new_st, last_mod, ip, dist, serv, id))
    conn.commit()

    return cur.rowcount

def update_st0(conn, ip, dist, serv, id, new_st, last_mod):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' UPDATE serie_temporal SET st = ?, last_mod = ? WHERE ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (new_st, last_mod, ip, dist, serv, id))
    conn.commit()

    return cur.rowcount


### insert
def insert_st(conn, ip, dist, serv, id, st, time):
    """
    Create a new row into the serie_temporal table
    :param conn:
    :param ip
    :param dist
    :param serv
    :param id
    :param st
    :param time
    """

    sql = ''' INSERT INTO serie_temporal(ip_src, dist_src, servico, id_dst, cr_time, last_mod, st)
              VALUES(?, ?, ?, ?, ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, (ip, dist, serv, id, time, time, st))
    conn.commit()

    return cur.rowcount


# others

def aggregate_st(conn, ip):
    sql = ''' SELECT st FROM serie_temporal WHERE ip_src = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip,))
    conn.commit()

    res = cur.fetchall()
    aggregate = {}

    for tupla in res:
        st = tupla[0]
        
        tuplas_st = st.split(";")
        for string_tupla_st in tuplas_st:
            tupla_st = string_tupla_st.split(",")
            if tupla_st[0] not in aggregate:
                aggregate[tupla_st[0]] = int(tupla_st[1])
            else: aggregate[tupla_st[0]] += int(tupla_st[1])
    
    #result = ""
    #for timestamp, count in sorted(aggregate.items(), key=lambda item: item[0]):
        #result += timestamp + "," + str(count) + ";"
    
    #result = result[:-1]
    
    # return result

    d_begin = datetime.datetime.strptime(BEGIN, "%Y-%m-%d %H:%M:%S")

    st_array = [0 for x in range(SIZE_SERIE_TEMPORAL+1)]

    for timestamp, count in sorted(aggregate.items(), key=lambda item: item[0]):
        datetime_tupla =  datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        delta = datetime_tupla - d_begin
        st_array_pos = delta.seconds // 3600 # quantas horas apos o inicio comeca a serie temporal
        st_array[st_array_pos] += count

    return st_array

if __name__ == '__main__':
    #DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"
    DATABASE = r"syn_dns_serie_temporal.db"
    create_database(DATABASE)
