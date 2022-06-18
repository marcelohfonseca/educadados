import pandas as pd
import os
import zipfile as zip

folder = '../data/censo-educacao-superior/'

files = os.listdir(folder)
df = pd.DataFrame()

for file in files:
    with zip.ZipFile(folder + file, 'r') as zip_file:
        for name in zip_file.namelist():
            if name.find('MICRODADOS_CADASTRO_CURSOS_') > 0:
                with zip_file.open(name) as csv_file:
                    df_tmp = pd.read_csv(csv_file, encoding='latin1', sep=';')
                    df = df.append(df_tmp)
            else:
                pass         
