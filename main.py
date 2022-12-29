import subprocess
import pandas as pd
import os 

raw_data_path = '/Users/piotrmierzejewski/Desktop/Uczelnia/Inżynierka/InformationSpreadKedro/informationspread/data/01_raw/chunk'
processed_data_path = '/Users/piotrmierzejewski/Desktop/Uczelnia/Inżynierka/InformationSpreadKedro/informationspread/data/02_intermediate/'
twitts = '/Users/piotrmierzejewski/Desktop/Uczelnia/Inżynierka/InformationSpreadKedro/informationspread/data/01_raw/joined.csv' 
kedro_output = processed_data_path + 'cleaned_data.csv'
processed = '_processed'
extension = '.csv'

def change_file_name(old_path, new_path):
    os.rename(old_path, new_path)
    
def run_kedro_pipeline():
    subprocess.run(["kedro", "run"])

start = 21
end = 114
for number in range(start, end, 1):
    next_chunk = raw_data_path + str(number) + extension
    change_file_name(next_chunk,twitts)
    run_kedro_pipeline()
    change_file_name(kedro_output,processed_data_path + "chunk" + str(number) + processed + extension)
    change_file_name(twitts, raw_data_path + str(number) + processed + extension)
# zmineń nazwę wejściowego na chunka na twitts

# odpal kedro
# zmień nazwe wyjścia
# zmień nazwę wejścia 

