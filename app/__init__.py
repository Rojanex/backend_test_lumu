from app.utils import read_file, divide_queries_in_chunks, \
                    ranking_calculation, send_chunks_to_lumu_api, extract_info_from_queries

def parsed_app(query_file):

    records_list = read_file(query_file)
    if records_list is False:
        return False
    chunks = divide_queries_in_chunks(records_list)
    send_chunks_to_lumu_api(chunks)
    ranking_calculation(records_list)