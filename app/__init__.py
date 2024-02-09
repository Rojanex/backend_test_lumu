from app.utils import read_file, divide_queries_in_chunks, ranking_calculation, send_chunks_to_lumu_api

def parsed_app(query_file):
    records_list = read_file(query_file)
    chunks = divide_queries_in_chunks(records_list)
    send_chunks_to_lumu_api(chunks)
    ranking_calculation(records_list)