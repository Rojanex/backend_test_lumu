from datetime import datetime
from collections import Counter
from itertools import islice
from tabulate import tabulate

def extract_info_from_queries(record):

    """
        Function that takes each record and filters key words, so it only has 
        the needed information. Then creates a dictionary compatible with Lumu API and returns it. 
    """
    
    split_words = [ "query:", "info:", "client", "query:", 'queries:' ]
    filtered_record = ' '.join(i for i in record.split() if i not in split_words).split()
    api_record = { 
        "timestamp": datetime.strptime(filtered_record[0] + ' ' + filtered_record[1], '%d-%b-%Y %H:%M:%S.%f').isoformat(),
        "name": filtered_record[5],
        "client_ip": filtered_record[3].split('#')[0],
        "type": filtered_record[7]
    }
    return api_record



def read_file(query_file):

    """
        Read query file and return a list with all of the records
    """
    records_list = []
    with open(query_file, 'r') as file:
        for line in file:
            record = extract_info_from_queries(line)
            records_list.append(record)
    return records_list   


def ranking_calculation(records_list):
    total_records = len(records_list)
    client_ips = [record["client_ip"] for record in records_list]
    counter_client_ips = Counter(client_ips)
    sorted_clients_ips = sorted(counter_client_ips.items(), key=lambda x: x[1], reverse=True)
    hosts = [record["name"] for record in records_list]
    counter_hosts = Counter(hosts)
    sorted_hosts = sorted(counter_hosts.items(), key=lambda x: x[1], reverse=True)
    client_ip_rank, host_rank = [], []
    for ip, count in islice(sorted_clients_ips, 5):
        client_ip_rank.append([ip, count, f"{format(count*100/total_records, '.2f')}%"])
    for host, count in islice(sorted_hosts, 5):
        host_rank.append([host, count, f"{format(count*100/total_records, '.2f')}%"])
    
    print('Total records: ', total_records)
    print("")
    print("Client IPs Rank")
    print(tabulate(client_ip_rank))
    print("")
    print("Host Rank")
    print(tabulate(host_rank))





def divide_queries_in_chunks(records_list, chunks_size=500):
    all_chunks, splitted_chunks = [], []
    for record in records_list:
        splitted_chunks.append(record)
        if len(splitted_chunks) == chunks_size:
            all_chunks.append(splitted_chunks)
            splitted_chunks = []
    if splitted_chunks: # En caso de que al final quede un lote con una cantidad menor de 500 records, igual agregarlo a la variable chunks
        all_chunks.append(splitted_chunks)

    return all_chunks








