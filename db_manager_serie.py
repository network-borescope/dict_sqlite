import sqlite3
from sqlite3 import Error

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
                                        id_cliente INTEGER,
                                        ip_src TEXT,
                                        dist_src TEXT,
                                        servico TEXT,
                                        id_dst INTEGER,
                                        cr_time timestamp NOT NULL,
                                        last_mod timestamp NOT NULL,
                                        st TEXT NOT NULL,
                                        CONSTRAINT PK_serie_temp PRIMARY KEY (id_cliente, ip_src, dist_src, servico, id_dst)
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


def select_row_from_serie_temp(conn, id_cliente, ip, dist, serv, id ):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT * FROM serie_temporal WHERE id_cliente = ? AND ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (id_cliente, ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0]

def select_st_from_serie_temp(conn, id_cliente, ip, dist, serv, id ):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT st FROM serie_temporal WHERE id_cliente = ? AND ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (id_cliente, ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0][0]

def update_st(conn, id_cliente, ip, dist, serv, id, new_st, last_mod):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' UPDATE serie_temporal SET st = ?, last_mod = ? WHERE id_cliente = ? AND ip_src = ? AND dist_src = ? AND servico = ? AND id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (new_st, last_mod, id_cliente, ip, dist, serv, id))
    conn.commit()


### insert
def insert_st(conn, id_cliente, ip, dist, serv, id, st, time):
    """
    Create a new row into the serie_temporal table
    :param conn:
    :param id_cliente:
    :param ip
    :param dist
    :param serv
    :param id
    :param st
    :param time
    """

    sql = ''' INSERT INTO serie_temporal(id_cliente, ip_src, dist_src, servico, id_dst, cr_time, last_mod, st)
              VALUES(?, ?, ?, ?, ?, ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, (id_cliente, ip, dist, serv, id, time, time, st))
    conn.commit()

if __name__ == '__main__':
    #DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"
    DATABASE = r"syn_dns_serie_temporal.db"
    create_database(DATABASE)
