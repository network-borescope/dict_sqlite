import datetime
from sys import argv, exit
import db_manager_serie

# GLOBALS

# data positions
D_DATA = 0
D_D_SEMANA = 1
D_HORA = 2
D_MIN = 3
D_ID_CLIENTE = 4
D_SIP= 5
D_DIST = 6
D_TTL = 7
D_DPORT = 8
D_SERV = 9
D_DID = 10
D_COUNT = 11

def build_timestamp(d_data, d_hora, d_min):
    year, month, day = d_data.split("-")
    
    d = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(d_hora), minute=int(d_min))

    return d

def main():
    arguments = argv[1:]

    if len(arguments) == 1:
        filename = arguments[0]
    else: exit(1)

    fin = open(filename, "r")

    count = 0
    data = []

    conn = db_manager_serie.create_connection(r"syn_dns_serie_temporal.db")
    for line in fin:
        
        count+=1
        if count % 1000 == 0: print(count)

        # reinicia as variaveis de memoria
        data = []

        # prepara novo dado a ser processado
        clean_line = line.strip()
        data = clean_line.split(",")

        if len(data) < 12: continue

        time = build_timestamp(data[D_DATA], data[D_HORA], data[D_MIN])
        st = db_manager_serie.select_st_from_serie_temp(conn, data[D_ID_CLIENTE], data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID])

        if st is None:
            st = str(time) + "," + data[D_COUNT]
            db_manager_serie.insert_st(conn, data[D_ID_CLIENTE], data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID], st, time)
        else:
            last_part_of_serie = st.split(";")[-1] # "timestamp,count"
            last_part_of_serie = last_part_of_serie.split(",") # [timestamp, count]

            if last_part_of_serie[0] == str(time):
                count = int(data[D_COUNT])
                count += int(last_part_of_serie[1]) # atualiza count

                pos = st.rfind(",") # encontra aonde comeca o count da ultima parte da serie
                new_st = st[:pos+1] + str(count)

            elif last_part_of_serie[0] < str(time): new_st = st + ";" + str(time) + "," + data[D_COUNT]
            else:
                print("ERRO: NOVA PARTE DA SERIE TEMPORAL EH MAIS ANTIGA QUE A EXISTENTE NO BANCO!!!")
                print("Existente no banco: ", last_part_of_serie[0])
                print("Sendo processada: ", time)
            
            db_manager_serie.update_st(conn, data[D_ID_CLIENTE], data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID], new_st, time)
            

    conn.close()
    fin.close()

if __name__ == '__main__':
    main()
