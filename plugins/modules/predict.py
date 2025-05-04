# <YOUR_IMPORTS>
import pandas as pd
import dill
import json
import glob
import os
from pathlib import Path
from datetime import datetime


def predict():
    path = '/opt/airflow/data'
    # ...дальше путь внутри папки до модели
    with open(f'{path}/models/cars_pipe_202505041048.pkl', 'rb') as file:
        model = dill.load(file)

    predicted_df = pd.DataFrame(columns=['id', 'predict'])  #
    print('predicted_df -', predicted_df)

    def prediction(data_path):
        with open(data_path, 'r') as f:
            data = pd.DataFrame.from_dict([dict(json.load(f))])  # извлечем данные из файла в виде словаря в датафрейм
            print(data)
            y = model.predict(data)[0]  # найдем прогноз для этого датафрейма
            predicted_df.loc[len(predicted_df.index)] = [int(data.id),y]  # занесем в подготовленный датафрейм результат
    for file_name in os.listdir(f'{path}/test'):  # для файлов в папке test
        prediction(f'{path}/test/' + file_name)  # prediction для каждого файла
    # сохраним датафрейм в файл csv
    #predicted_df.to_csv(f'{path}/predictions/predictions_{datetime.now().strftime("%Y%m%d%H%M")}.csv')
    predic_filename = f'{path}/predictions/predictions_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    predicted_df.to_csv(predic_filename)

if __name__ == '__main__':
    predict()
