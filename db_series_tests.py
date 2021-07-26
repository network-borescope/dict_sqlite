from db_manager_serie import *


conn = create_connection(r"syn_dns_serie_temporal.db")

if conn is not None:
    insert_st(conn, 1, "1.1.1.1", 3, "HTTP", 33, "14:10:00,472", "14:10:00")

    result = select_row_from_serie_temp(conn, 1, "1.1.1.1", 3, "HTTP", 33 )
    print(result)

    st = select_st_from_serie_temp(conn, 1, "1.1.1.1", 3, "HTTP", 33 )
    print(st)

    new_st = st + ";" + "19:40:00,127"
    last_modified = "19:40:00"

    update_st(conn, 1, "1.1.1.1", 3, "HTTP", 33, new_st, last_modified)
    result = select_row_from_serie_temp(conn, 1, "1.1.1.1", 3, "HTTP", 33 )
    print("Modified: ",result)

    result = select_row_from_serie_temp(conn, 2, "1.1.1.1", 3, "HTTP", 33 )
    print(result)

    conn.close()

else: print("DEU ERRO")