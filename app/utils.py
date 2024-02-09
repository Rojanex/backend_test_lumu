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

        Args:
            record (str): Query to be filtered
        Returns:
            api_record (dict): A dictionary with the information extracted compatible with Lumu API.
            Example:
                    {
                        'timestamp': '2022-07-07T16:34:13.010000', 
                        'name': 'pizzaseo.com', 
                        'client_ip': '111.90.159.121', 
                        'type': 'ANY'
                    }
    """
    
    split_words = [ "query:", "info:", "client", "query:", 'queries:' ]

    #Create a list with only the necessary information
    filtered_record = ' '.join(i for i in record.split() if i not in split_words).split() 
    api_record = { 
        "timestamp": datetime.strptime(filtered_record[0] + ' ' + filtered_record[1], \
                                       '%d-%b-%Y %H:%M:%S.%f').isoformat(), #Create a date object and transform it into a ISO format
        "name": filtered_record[5],
        "client_ip": filtered_record[3].split('#')[0], #Split with # to obtain port and ip
        "type": filtered_record[7]
    }
    return api_record



def read_file(query_file):

    """
        Read query file and return a list with all of the records in a format valid with Lumu API

        Args:
            query_file (str): String with filepath. Example: /Users/rojanex/Downloads/queries
        Returns:
            record_list (list): A list of dictionaries with valid format of Lumu API
    """
    records_list = []
    try:
        with open(query_file, 'r') as file:
            for line in file:
                record = extract_info_from_queries(line) #Each string line extract and format
                records_list.append(record)
        return records_list   
    except FileNotFoundError as file_err:
        print("The specify file was not found, please enter a valid file")
        return False


def ranking_calculation(records_list):
    """
        Calculates and counts the occurrences of the ip, prints a table with top 5 ranks
        of clients_ips and hosts

        Args:
            record_list (list): A list of dictionaries with valid format of Lumu API
        Returns:
            None
    """

    total_records = len(records_list) 

    #Create a list only with the client_ip value of the record list 
    client_ips = [record["client_ip"] for record in records_list]
    #Create a Counter object, to count how many times each ip appears in the list
    counter_client_ips = Counter(client_ips)
    
    #Repeat the process for hosts
    hosts = [record["name"] for record in records_list]
    counter_hosts = Counter(hosts)
   
   #Create new lists for table that includes the top 5 ranks, append to the new list [ip, count, percentage]
    client_ip_rank, host_rank = [], []
    for ip, count in counter_client_ips.most_common(5): #Use most_common() method of counter object to obtain only top 5
        client_ip_rank.append([ip, count, f"{format(count*100/total_records, '.2f')}%"])
    for ip, count in counter_hosts.most_common(5): #Use most_common() method of counter object to obtain only top 5
        host_rank.append([ip, count, f"{format(count*100/total_records, '.2f')}%"])
   

    #Printing ranking data
    print('Total records: ', total_records)
    print("")
    print("Client IPs Rank")
    print(tabulate(client_ip_rank))
    print("")
    print("Host Rank")
    print(tabulate(host_rank))



def divide_queries_in_chunks(records_list, chunks_size=500):
    """
        Creates a new list where each element is a list of 500 records (by default)

        Args:
            record_list (list): A list of dictionaries with valid format of Lumu API
            chunk_size (int): Integer with the size of each element in the new created list

        Returns:
            all_chunks (list): List where each element is a chunk
    """
    all_chunks, splitted_chunks = [], []
    for record in records_list:
        splitted_chunks.append(record)
        if len(splitted_chunks) == chunks_size:
            all_chunks.append(splitted_chunks)
            splitted_chunks = []
    if splitted_chunks: # In case in the final loop there is a chunk with less than 500 records, add it to the all_chunks list.
        all_chunks.append(splitted_chunks)
    return all_chunks



def send_chunks_to_lumu_api(chunks):
    """
        Sends each chunk to Lumu API as a json

        Args:
            chunks (list):  List where each element is a chunk with the data to be sent
        Raises:
            Exception: If there is an error during the request to the Lumu API
        Returns:
            None
    """
    try:
        count = 0
        for chunk in chunks:
            #Convert chunk data into a json
            json_chunks = json.dumps(chunk)
            #Obtains environment variables to build the url 
            base_url = os.environ.get('BASE_URL')
            collector_id = os.environ.get('COLLECTOR_ID')
            lumu_client_key = os.environ.get('LUMU_CLIENT_KEY')
            api_url = f"{base_url}/collectors/{collector_id}/dns/queries?key={lumu_client_key}"

            #Sends post to Lumu API with the json data, url builded and headers needed
            response = requests.post(
                url = api_url,
                data = json_chunks,
                headers= {'Content-Type': 'application/json'}
            )

            # Prints the success or failure of each request
            if response.status_code ==  200:
                print(f"Chunk #{chunks.index(chunk)+1} was sent successfully")
                count = count + 1 
            else:
                print(f"Request for chunk #{chunks.index(chunk)+1} failed with status code {response.status_code}")

        # In case all chunks are sent successfully, it prints a confirmation message
        if count == len(chunks):
            print("------ All queries has been sent to Lumu API successfully ------")
            print("")
        else: 
            # In case there is an error response in any chunk it reports the number of successful and remaining queries.
            print(f"Sent queries: {count}")
            print(f"Queries remaining: {len(chunks)-count}")
            print("")
    except Exception as err:
        print(f'ERROR: {err}')
        print(traceback.format_exc())