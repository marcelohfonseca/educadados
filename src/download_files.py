import requests

def download(url, directory):

    URL = 'https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2023.zip'
    FOLDER = '../data'

    file = requests.get(URL)
    if file.status_code == requests.codes.OK:
        with open(FOLDER, 'wb') as new_file:
            new_file.write(file.content)
        print('Download do arquivo realizado com sucesso.')
    else:
        print(file.raise_for_status())


