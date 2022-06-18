from datetime import date
import requests
import os
import json

def download(url: str, directory: str) -> None:
    """Função que faz o download de um arquivo.

    Args:
        url (str): url do arquivo que vai ser baixado
        directory (str): pasta onde o arquivo será salvo.
    """

    file = requests.get(url)
    if file.status_code == requests.codes.OK:
        with open(directory, 'wb') as new_file:
            new_file.write(file.content)
        print(f'[OK] Download do arquivo realizado com sucesso. Salvo em {directory}')
    else:
        print(f'[ERRO] URL {url} não existe')


if __name__ == "__main__":
    
    # importar json com lista de links
    with open('../download_list.json') as json_file:
        url_dict = json.load(json_file)['links']
    
    for name, link in url_dict.items():
        folder = '../data/' + name
        url = link['url']
        file_name = link['filename']
        
        last_year = date.today().year
        first_year = last_year - 10        

        for year in range(first_year, last_year):
            # cria a pasta caso ela não exista
            os.mkdir(folder) if not os.path.exists(folder) else None
            
            directory = os.path.join(folder, f'{file_name}_{year}.zip')
            download(url.format(year), directory)
