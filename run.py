
from app import parsed_app
import sys

if __name__ == '__main__':
    #In case the number of arguments is different from 2, print usage mode and close script 
    if len(sys.argv) != 2:
        print('Modo de uso: python run.py <query_file>') 
        sys.exit(1)

    query_file = sys.argv[1]
    response = parsed_app(query_file)
    if response is False:
        sys.exit(1)