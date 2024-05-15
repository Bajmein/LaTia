import numpy as np
import pandas as pd
import re


class DataCleaner:
    fecha: list = []
    apertura: list = []
    maximo: list = []
    minimo: list = []
    cierre: list = []
    volumen: list = []
    spread: list = []

    def __init__(self, data: np.ndarray):
        self.data = str(data[0])

    def filtrar(self):
        patron = r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})"
        fecha = re.search(patron, self.data)
        DataCleaner.fecha.append(f'{fecha.groups(0)[0]} {fecha.groups(0)[1]}')
        self.data = self.data.strip(f'{fecha.groups(0)[0]} {fecha.groups(0)[1]}')
        patron = r"\d+\.\d+"
        resultados = re.findall(patron, self.data)

        if len(resultados) == 4:
            DataCleaner.apertura.append(resultados[0])
            DataCleaner.maximo.append(resultados[1])
            DataCleaner.minimo.append(resultados[2])
            DataCleaner.cierre.append(resultados[3])

        else:
            DataCleaner.apertura.append('0')
            DataCleaner.maximo.append('0')
            DataCleaner.minimo.append('0')
            DataCleaner.cierre.append('0')

    @staticmethod
    def segundo_filtro():
        for _ in range(len(DataCleaner.apertura)):
            DataCleaner.apertura[_] = str(DataCleaner.apertura[_])
            DataCleaner.maximo[_] = str(DataCleaner.maximo[_])
            DataCleaner.minimo[_] = str(DataCleaner.minimo[_])
            DataCleaner.cierre[_] = str(DataCleaner.cierre[_])

    @staticmethod
    def vaciar_listas():
        DataCleaner.fecha = []
        DataCleaner.apertura = []
        DataCleaner.maximo = []
        DataCleaner.minimo = []
        DataCleaner.cierre = []


if __name__ == '__main__':
    import os
    import time

    print('Actualizando datos...')
    inicio = time.time()

    try:
        for archivo in os.listdir('datos/datasets_por_filtrar/'):

            viejo_df = pd.read_csv(f'datos/datasets_por_filtrar/{archivo}')
            nuevo_df = pd.DataFrame()

            for i in range(len(viejo_df)):
                on = DataCleaner(viejo_df.values[i])
                on.filtrar()

            DataCleaner.segundo_filtro()

            longitud_minima = [
                len(DataCleaner.apertura), len(DataCleaner.maximo),
                len(DataCleaner.minimo), len(DataCleaner.cierre)
            ]

            nuevo_df['fecha'] = DataCleaner.fecha[:min(longitud_minima)]
            nuevo_df['apertura'] = DataCleaner.apertura[:min(longitud_minima)]
            nuevo_df['maximo'] = DataCleaner.maximo[:min(longitud_minima)]
            nuevo_df['minimo'] = DataCleaner.minimo[:min(longitud_minima)]
            nuevo_df['cierre'] = DataCleaner.cierre[:min(longitud_minima)]

            nuevo_df.to_csv(f'datos/datasets_filtrados/{archivo}')

            DataCleaner.vaciar_listas()

            print(f'- datos/datasets_por_filtrar/{archivo} actualizado.')

        final = time.time()
        tiempo_total = final - inicio

        print('\nFinalizado.')
        print(f'Tiempo total de ejecución: {tiempo_total:.2f} segundos')

    except KeyboardInterrupt:
        pass
