from app.utils import read_file, divide_queries_in_chunks, ranking_calculation

def parsed_app(query_file):

    records_list = read_file(query_file)
    divide_queries_in_chunks(records_list)
    ranking_calculation(records_list)

    return True