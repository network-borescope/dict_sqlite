import datetime
from ip_to_nome_lat_lon import site_from_ip
from sys import argv, exit, stdin
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

def date_to_day(d_data):
    year, month, day = d_data.split("-")
    day = datetime.datetime(year=int(year), month=int(month), day=int(day)).weekday()

    day_of_week = {0: "Segunda", 1: "Terca", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sabado", 6: "Domingo"}
    #return day_of_week[day]
    return str((day+1 % 7)+1)


def hour_to_timedelta(d_hora):
    hour, min, sec = d_hora.split(":")

    return datetime.timedelta(hours=int(hour), minutes=int(min), seconds=float(sec))

# pega query e query id do request
def request_parser(items, data):
    if len(items) < 10 or (items[7] != 'A?' and items[8] != 'A?'):
        data.clear()
        return False

    # verifica se tem flags
    pos = 7
    flags = items[pos]
    if flags[0] != '[':
        flags = ""
        pos -= 1
    query = items[pos+2][:-1] # remove o ponto do final da query
    data[D_QUERY] = query

    if items[6][0] >= '0' and items[6][0] <= '9': # assume que items[6] eh o query id
        query_id = items[6].replace("+", "")
        query_id = query_id.replace("%", "")
        data[D_QUERY_ID] = query_id
    else:
        print("#######################################", items[6])
        data.clear()
        return False

    return True

# pega query e query id do response
def response_parser(items, data, cnames):
    if items[6][0] >= '0' and items[6][0] <= '9':
        query_id = items[6].replace("*", "")
        query_id = query_id.replace("-", "")
        query_id = query_id.replace("|", "")
        query_id = query_id.replace("$", "")
        data[D_QUERY_ID] = query_id
    else:
        data.clear()
        return False

    n = len(items)
    i = 0
    while i < n:
        if items[i] == 'q:':
            i += 1
            if not (i < n and items[i] == "A?"):
                data.clear()
                return False

            i += 1
            data[D_QUERY] = items[i][:-1] # remove o ponto


            i += 1
            if not (i < n and items[i][0] != "0"):
                data.clear()
                return False

            i += 1
            if not (i < n): data.clear(); return False
            s = items[i][:-1] # remove o ponto
            if s != data[D_QUERY]: data.clear(); return False
            data[D_QUERY_POS] = i

            i += 1
            if not (i < n and items[i][0] == '['): data.clear(); return False

            i += 1
            if not (i < n): data.clear(); return False

            if items[i] == "A":
                return True

            data.clear()
            #if items[i] == "CNAME": return "CNAME"
            if items[i] == "CNAME":
                while i < n:
                    # name validade CNAME name
                    i += 1
                    if not i < n: return "CNAME"

                    cname = items[i]
                    if cname[len(cname)-1] == ",":
                        cname = cname[:-2] # remove a virgula e o ponto
                        cnames.add(cname)
                    else:
                        cname = cname[:-1] # remove o ponto
                        cnames.add(cname)
                        return "CNAME"

                    i += 1 # name
                    if not i < n: return "CNAME"

                    i += 1 # validade
                    if not i < n: return "CNAME"

                    i += 1 # CNAME
                    if not (i < n and items[i] == "CNAME"): return "CNAME"

                return "CNAME"

            return False

        i += 1

    data.clear()
    return False

def get_response_ips(items, know_ips, data):
    # query validade A ip
    n = len(items)
    i = data[D_QUERY_POS]


    continua = True
    while i < n and continua:
        ignora = False
        # pega a query
        #s = items[i][:-1] # remove o ponto da query
        if i >= n: break
        if items[i][:-1] != data[D_QUERY]: ignora = True

        i += 1
        # ignora validade da resposta

        i += 1
        if i >= n: break
        if not (items[i] == 'A'): ignora = True

        i += 1 # posicao do ip
        if i >= n: break
        if not (items[i] >= '0' and items[i] <= '9'): ignora = True

        ip = items[i]
        continua = False
        if ip[-1] == ',':
            ip = ip[:-1]
            continua = True

        if not ignora: know_ips[ip] = data[D_QUERY]

        i += 1

def main():
    arguments = argv[1:]

    if len(arguments) == 1:
        filename = arguments[0]
    else: exit(1)

    filename = filename.split(".")[0]

    count = 0
    #fin = open(filename, "r")
    fin = open(filename, "r")

    services = {"53": "DNS", "80": "HTTP", "443": "HTTP", "25": "SMTP", "587": "SMTP", "465": "SMTP", "110": "POP3", "995": "POP3", "143": "IMAP", "20": "FTP", "21": "FTP", "22": "SSH"}

    key_count = {} # tuplas/padroes: dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer) : count
    #know_ips = {} # { ip: hostname }
    #cnames = set() # cname list
    #dns_a_count = 0
    #dns_a_cname_count = 0

    dict_dst = {}


    data = []

    conn = db_manager_serie.create_connection("syn_dns_serie.db")
    for line in fin:
        count+=1
        if count % 1000000 == 0: print(count)

        # reinicia as variaveis de memoria
        key = ""
        data = []

        # inicia o processamento do novo hash
        clean_line = line.strip()
        items = clean_line.split(",")

        if len(items) < 12: continue
        if items[2] != "IP": continue

        n = len(items)

        # [data, hora, val_ttl, val_proto, val_ip_id ]
        val_proto = items[15][1:-2]
        data = [ items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9], items[10], items[11]]

        time = datetime.now()
        st = ""
        row = db_manager_serie.select_row_from_serie_temp(conn, data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID])
        if row[0] is not None: db_manager_serie.insert_st(conn, data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID], st, time)
        else:
            db_manager_serie.update_st(conn, data[D_SIP], data[D_DIST], data[D_SERV], data[D_DID], st, time)
            
        '''
        #service = "DESCONHECIDO"
        service = data[D_DPORT]
        if data[D_DPORT] in services: service = services[data[D_DPORT]]

        hour, min, sec = data[D_HORA].split(":")
        # dia-da-semana, hora, id_cliente, ip_origem, distancia, ttl, porta_destino (servico), id_destino (0 = qualquer)

        day = date_to_day(data[D_DATA])
        prefix = data[D_DATA] + "," + day + "," + hour + "," + min + ","

        #key = ""
        #key += day + ","
        #key += hour + ","
        key = prefix
        key += client_id + ","
        key += data[D_SIP] + ","
        key += data[D_DIST] + ","
        key += data[D_TTL] + ","
        key += data[D_DPORT] + ","
        key += service + ","
        #key += data[D_DIP] + ","
        key += str(destination_id)

        if key not in key_count:
            key_count[key] = 1
        else:
            key_count[key] += 1

        key1 = prefix + client_id + "," + data[D_SIP] + "," + data[D_DIST] + "," + service + "," + str(destination_id)
        key2 = prefix + client_id + "," + data[D_SIP] + "," + data[D_DIST] + "," + service + "," + "0"
        key3 = prefix + client_id + "," + data[D_SIP] + "," + data[D_DIST] + ",0" + ",0"

        if key1 not in dict_dst:
            dict_dst[key1] = 1
        else:
            dict_dst[key1] += 1

        if key2 not in dict_dst:
            dict_dst[key2] = 1
        else:
            dict_dst[key2] += 1

        if key3 not in dict_dst:
            dict_dst[key3] = 1
        else:
            dict_dst[key3] += 1


                    if proto_port == "17:53": # se for dns request
                        if not request_parser(items, data): continue

                        # { ip_src port_src ip_dst query query_id: False }
                        key = f"{data[D_SIP]} {data[D_SPORT]} {data[D_DIP]} {data[D_QUERY]} {data[D_QUERY_ID]}"

                        if key not in dns_match:
                            #cname = key in cnames
                            dns_match[key] = False

                        # query repetida
                        else:
                            print("######## Chave repetida(DNS Req)")

                    elif (data[D_PROTO] + ":" + data[D_SPORT]) == "17:53": # dns response
                        if response_parser(items, data, cnames) == "CNAME":
                            dns_a_count += 1
                            dns_a_cname_count += 1

                        elif len(data) > 0:
                            dns_a_count += 1

                            key = f"{data[D_DIP]} {data[D_DPORT]} {data[D_SIP]} {data[D_QUERY]} {data[D_QUERY_ID]}"

                            if key in dns_match:
                                if not dns_match[key]: # primeiro match daquela chave
                                    dns_match[key] = True
                                    get_response_ips(items, know_ips, data)
                                else:
                                    print("####### Match repetida(DNS Response)")

                    else: # nao eh DNS
                        continue
                    '''

    conn.close()
    with open("out_" + filename + ".txt", "w") as fout:
        for key in key_count:
            print(key + "," + str(key_count[key]), file=fout)

    with open("hist_dst_" + filename + ".csv", "w") as fout:
        for key in dict_dst:
            print(key + "," + str(dict_dst[key]), file=fout)


if __name__ == '__main__':
    main()
