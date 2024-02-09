
from app import parsed_app
import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Modo de uso: python run.py <query_file>') #En caso de que la cantidad de argumentos sea diferente de 2, imprimir modo de uso y cerrar script 
        sys.exit(1)

    query_file = sys.argv[1]
    response = parsed_app(query_file)
    if response is False:
        sys.exit(1)