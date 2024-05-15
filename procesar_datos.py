import pandas as pd
import statistics
import os


class Procesamiento:
    datos: list = []

    def __init__(self, fecha, apertura, maximo, minimo, cierre, alts, pres):
        self.fecha = fecha
        self.par = None
        self.temporalidad = None
        self.tipo_hammer = None
        self.tipo_vela = None
        self.apertura = apertura
        self.maximo = maximo
        self.minimo = minimo
        self.cierre = cierre
        self.rango = maximo - minimo
        self.s1 = alts[0]
        self.s2 = alts[1]
        self.s3 = alts[2]
        self.s4 = alts[3]
        self.s5 = alts[4]
        self.p5 = pres[4]
        self.p4 = pres[3]
        self.p3 = pres[2]
        self.p2 = pres[1]
        self.p1 = pres[0]
        self.__mch_sup = None
        self.__cuerpo = None
        self.__mch_inf = None

    def __filtro_interno(self) -> None:
        """
            Método privado para aplicar filtros internos a los datos procesados.
        """
        # Determinar el tipo de vela (compra/venta)
        if self.apertura > self.cierre:
            self.tipo_vela: str = 'venta'

        if self.apertura < self.cierre:
            self.tipo_vela: str = 'compra'

        match self.tipo_vela:
            case 'venta':
                self.__mch_sup: float = self.maximo - self.apertura
                self.__cuerpo: float = self.apertura - self.cierre
                self.__mch_inf: float = self.cierre - self.minimo

            case 'compra':
                self.__mch_sup: float = self.maximo - self.cierre
                self.__cuerpo: float = self.cierre - self.apertura
                self.__mch_inf: float = self.apertura - self.minimo

        # Definir condiciones para los distintos tipos de hammer
        self.__mch_sup: list[bool] = [
            self.__mch_sup <= self.rango * 0.2,
            self.rango * 0.6 <= self.__mch_sup < self.rango
        ]

        self.__cuerpo: list[bool] = [
            self.rango * 0.15 <= self.__cuerpo < self.rango * 0.3
        ]

        self.__mch_inf: list[bool] = [
            self.rango * 0.6 <= self.__mch_inf < self.rango,
            self.__mch_inf <= self.rango * 0.2
        ]

        # Determinar si el patrón es un hammer normal o invertido
        if all([self.__mch_sup[0], self.__cuerpo[0], self.__mch_inf[0]]):  # Normal
            self.tipo_hammer: str = 'Hammer'

        if all([self.__mch_sup[1], self.__cuerpo[0], self.__mch_inf[1]]):  # Invertido
            self.tipo_hammer: str = 'Hammer invertido'

        # Si se identificó un hammer, almacenar los datos procesados
        if self.__dict__['tipo_hammer']:
            data: dict[str, any] = self.__dict__
            Procesamiento.datos.append(data)

    @staticmethod
    def __filtrar() -> None:
        """
            Método estático para filtrar los datos de los archivos en un directorio específico.
        """
        # Iterar sobre los archivos en el directorio 'datasets_por_filtrar/'
        for _, ruta in enumerate(os.listdir('datos/datasets_filtrados/')):
            ruta: str = 'datos/datasets_filtrados/' + ruta
            documento: pd.read_csv = pd.read_csv(ruta)
            data: list[list] = [list(row) for row in documento.iterrows()]
            print(f'{_ / len(os.listdir('datos/datasets_filtrados/')) * 100:.2f}%')
            # Procesar los datos y almacenar los resultados
            for i in range(len(data)):
                try:
                    fecha = list(data[i][1])[1]
                    actual: list = list(data[i][1])[2:]
                    alter: list[list] = [list(data[i + j][1])[2:] for j in range(1, 6)]
                    pres: list = []

                    for j in range(1, 6):
                        if i <= 4:
                            pres.append([0, 0, 0, 0])

                        else:
                            pres.append(list(data[i - j][1])[2:])

                    on: Procesamiento = Procesamiento(
                        fecha=fecha,
                        apertura=actual[0],
                        maximo=actual[1],
                        minimo=actual[2],
                        cierre=actual[3],
                        alts=alter,
                        pres=pres
                    )

                    on.par = ruta.strip('datos/datasets_filtrados/')[0:6]
                    on.temporalidad = ruta.strip('datos/datasets_filtrados/').strip('.csv')[-3:].strip('_')
                    on.__filtro_interno()

                except (IndexError, TypeError):
                    pass

    @staticmethod
    def main():
        import time

        tiempo_inicio = time.time()

        print('Procesando datos de datos/datasets_filtrados/...')

        Procesamiento.__filtrar()
        res = Procesamiento.datos

        campos: list[str] = [
            'fecha', 'par', 'temporalidad', 'tipo_hammer', 'tipo_vela',
            'apertura', 'maximo', 'minimo', 'cierre', 'media',

            'p1_apertura', 'p1_maximo', 'p1_minimo', 'p1_cierre',
            'p2_apertura', 'p2_maximo', 'p2_minimo', 'p2_cierre',
            'p3_apertura', 'p3_maximo', 'p3_minimo', 'p3_cierre',
            'p4_apertura', 'p4_maximo', 'p4_minimo', 'p4_cierre',
            'p5_apertura', 'p5_maximo', 'p5_minimo', 'p5_cierre',

            's1_apertura', 's1_maximo', 's1_minimo', 's1_cierre',
            's2_apertura', 's2_maximo', 's2_minimo', 's2_cierre',
            's3_apertura', 's3_maximo', 's3_minimo', 's3_cierre',
            's4_apertura', 's4_maximo', 's4_minimo', 's4_cierre',
            's5_apertura', 's5_maximo', 's5_minimo', 's5_cierre',
        ]

        df_final: pd.DataFrame = pd.DataFrame(columns=campos)

        print('100.00%')
        print('Cargando datos...')

        df_final['fecha'] = [elem['fecha'] for elem in res]
        df_final['par'] = [elem['par'] for elem in res]
        df_final['temporalidad'] = [elem['temporalidad'] for elem in res]
        df_final['tipo_hammer'] = [elem['tipo_hammer'] for elem in res]
        df_final['tipo_vela'] = [elem['tipo_vela'] for elem in res]
        df_final['apertura'] = [elem['apertura'] for elem in res]
        df_final['maximo'] = [elem['maximo'] for elem in res]
        df_final['minimo'] = [elem['minimo'] for elem in res]
        df_final['cierre'] = [elem['cierre'] for elem in res]

        df_final['media'] = [
            statistics.mean([elem['apertura'], elem['maximo'], elem['minimo'], elem['cierre']]) for elem in res
        ]

        df_final['p1_apertura'] = [elem['p1'][0] for elem in res]
        df_final['p2_apertura'] = [elem['p2'][0] for elem in res]
        df_final['p3_apertura'] = [elem['p3'][0] for elem in res]
        df_final['p4_apertura'] = [elem['p4'][0] for elem in res]
        df_final['p5_apertura'] = [elem['p5'][0] for elem in res]

        df_final['p1_maximo'] = [elem['p1'][1] for elem in res]
        df_final['p2_maximo'] = [elem['p2'][1] for elem in res]
        df_final['p3_maximo'] = [elem['p3'][1] for elem in res]
        df_final['p4_maximo'] = [elem['p4'][1] for elem in res]
        df_final['p5_maximo'] = [elem['p5'][1] for elem in res]

        df_final['p1_minimo'] = [elem['p1'][2] for elem in res]
        df_final['p2_minimo'] = [elem['p2'][2] for elem in res]
        df_final['p3_minimo'] = [elem['p3'][2] for elem in res]
        df_final['p4_minimo'] = [elem['p4'][2] for elem in res]
        df_final['p5_minimo'] = [elem['p5'][2] for elem in res]

        df_final['p1_cierre'] = [elem['p1'][3] for elem in res]
        df_final['p2_cierre'] = [elem['p2'][3] for elem in res]
        df_final['p3_cierre'] = [elem['p3'][3] for elem in res]
        df_final['p4_cierre'] = [elem['p4'][3] for elem in res]
        df_final['p5_cierre'] = [elem['p5'][3] for elem in res]

        df_final['s1_apertura'] = [elem['s1'][0] for elem in res]
        df_final['s2_apertura'] = [elem['s2'][0] for elem in res]
        df_final['s3_apertura'] = [elem['s3'][0] for elem in res]
        df_final['s4_apertura'] = [elem['s4'][0] for elem in res]
        df_final['s5_apertura'] = [elem['s5'][0] for elem in res]

        df_final['s1_maximo'] = [elem['s1'][1] for elem in res]
        df_final['s2_maximo'] = [elem['s2'][1] for elem in res]
        df_final['s3_maximo'] = [elem['s3'][1] for elem in res]
        df_final['s4_maximo'] = [elem['s4'][1] for elem in res]
        df_final['s5_maximo'] = [elem['s5'][1] for elem in res]

        df_final['s1_minimo'] = [elem['s1'][2] for elem in res]
        df_final['s2_minimo'] = [elem['s2'][2] for elem in res]
        df_final['s3_minimo'] = [elem['s3'][2] for elem in res]
        df_final['s4_minimo'] = [elem['s4'][2] for elem in res]
        df_final['s5_minimo'] = [elem['s5'][2] for elem in res]

        df_final['s1_cierre'] = [elem['s1'][3] for elem in res]
        df_final['s2_cierre'] = [elem['s2'][3] for elem in res]
        df_final['s3_cierre'] = [elem['s3'][3] for elem in res]
        df_final['s4_cierre'] = [elem['s4'][3] for elem in res]
        df_final['s5_cierre'] = [elem['s5'][3] for elem in res]

        df_final.to_csv('datos/resultado/forex_dataset.csv')

        tiempo_fin = time.time()
        tiempo_total = tiempo_fin - tiempo_inicio

        print()
        print('##############################################')
        print('\n* Carga de datos Finalizada *')
        print(f'Tiempo total de ejecución: {tiempo_total:.2f} segundos.\n')
        print('##############################################')


if __name__ == '__main__':
    Procesamiento.main()
