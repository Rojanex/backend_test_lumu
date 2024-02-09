from datetime import datetime
from collections import Counter
from itertools import islice
from tabulate import tabulate
import requests, json, os, traceback
from dotenv import load_dotenv

load_dotenv()

def extract_info_from_queries(record):

    """
        Function that takes each record and filters key words, so it only has 
        the needed information. Returns a dictionary compatible with Lumu API for each record. 
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
    if splitted_chunks: # En caso de que al final quede un lote con una cantidad menor de 500 records, igual agregarlo a la lista chunks
        all_chunks.append(splitted_chunks)

    return all_chunks



def send_chunks_to_lumu_api(chunks):
    try:
        count = 0
        for chunk in chunks:
            json_chunks = json.dumps(chunk)
            base_url = os.environ.get('BASE_URL')
            collector_id = os.environ.get('COLLECTOR_ID')
            lumu_client_key = os.environ.get('LUMU_CLIENT_KEY')
            api_url = f"{base_url}/collectors/{collector_id}/dns/queries?key={lumu_client_key}"
            response = requests.post(
                url = api_url,
                data = json_chunks,
                headers= {'Content-Type': 'application/json'}
            )
            if response.status_code ==  200:
                print(f"Chunk #{chunks.index(chunk)+1} was sent successfully")
                count = count + 1 
            else:
                print(f"Request for chunk #{chunks.index(chunk)+1} failed with status code {response.status_code}")
        if count == len(chunks):
            print("------ All queries has been sent to Lumu API successfully ------")
            print("")
        else: 
            print(f"Sent queries: {count}")
            print(f"Queries remaining: {len(chunks)-count}")
            print("")
    except Exception as err:
        print(f'ERROR: {err}')
        print(traceback.format_exc())








