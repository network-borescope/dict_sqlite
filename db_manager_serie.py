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



def create_database(DATABASE):
    sql_create_serie_temp = """ CREATE TABLE IF NOT EXISTS serie_temp (
                                        ip_src TEXT,
                                        dist_src TEXT,
                                        serv TEXT,
                                        id_dst INTEGER,
                                        cr_time timestamp NOT NULL,
                                        last_mod timestamp NOT NULL,
                                        st TEXT NOT NULL,
                                        CONSTRAINT PK_serie_temp PRIMARY KEY (ip_src, dist_src, serv, id_dst)
                                    ); """



    # create a database connection
    conn = create_connection(DATABASE)

    # create tables
    if conn is not None:
        # create host_serie_temp table
        create_table(conn, sql_create_serie_temp)

        conn.close()
    else:
        print("Error! cannot create the database connection.")


def select_row_from_serie_temp(conn, ip, dist, serv, id ):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' SELECT * FROM serie_temp WHERE ip_src = ? and dist_src = ? and serv = ? and id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (ip, dist, serv, id))
    conn.commit()

    res = cur.fetchall()

    if len(res) == 0: return None

    return res[0]

def update_st(conn, ip, dist, serv, id, new_st, time):
    """
    Query tasks by host_ip
    :param conn: the Connection object
    :param host_ip: the ip of the host queried
    :return:
    """

    sql = ''' UPDATE serie_temp SET st = ?, last_mod = ? WHERE ip_src = ? and dist_src = ? and serv = ? and id_dst = ?'''
    cur = conn.cursor()
    cur.execute(sql, (new_st, time, ip, dist, serv, id))
    conn.commit()


### get or insert

if __name__ == '__main__':
    #DATABASE = r"C:\sqlite\db\coletaPoP-DF.db"
    DATABASE = r"syn_dns_serie.db"
    create_database(DATABASE)
