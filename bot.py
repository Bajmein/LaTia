import MetaTrader5 as mt5
import asyncio
from dataclasses import dataclass
from typing import ClassVar, Self
from datetime import datetime
import time
import os


@dataclass
class Bot:
    __hora_apagado: int
    __minuto_apagado: int
    __usuario: ClassVar[int | None] = 10002949113
    __passwd: ClassVar[str | None] = '+nBe7kMd'
    __server: ClassVar[str | None] = 'MetaQuotes-Demo'
    __lista_5m: ClassVar[list] = []
    __lista_15m: ClassVar[list] = []
    __lista_30m: ClassVar[list] = []
    __pares: ClassVar[dict] = {
        'EURUSD': {'5m': 0.00010, '15m': 0.00020, '30m': 0.00020},
        'GBPUSD': {'5m': 0.00010, '15m': 0.00020, '30m': 0.00020},
        'USDCAD': {'5m': 0.00010, '15m': 0.00020, '30m': 0.00020},
        'USDCHF': {'5m': 0.00010, '15m': 0.00020, '30m': 0.00020},
        'USDJPY': {'5m': 0.00660, '15m': 0.02000, '30m': 0.02000},
    }

    @staticmethod
    def __login() -> None:
        mt5.login(login=Bot.__usuario, password=Bot.__passwd, server=Bot.__server)

    @staticmethod
    def __abrir_posicion(accion: str, par: str, temporalidad: str) -> None:
        lot: float = 0.75
        deviation: int = 5
        price: float = 0.0
        tipo_orden: any = None
        take_profit: float | None = None

        match accion:
            case 'compra':
                price: float = mt5.symbol_info_tick(par).ask
                tipo_orden: None = mt5.ORDER_TYPE_BUY
                take_profit: float | None = price + Bot.__pares[par][temporalidad]

            case 'venta':
                price: float = mt5.symbol_info_tick(par).bid
                tipo_orden: None = mt5.ORDER_TYPE_SELL
                take_profit: float | None = price - Bot.__pares[par][temporalidad]

        request: dict[str, float | str | int | None] = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": par,
            "volume": lot,
            "type": tipo_orden,
            "price": price,
            "tp": take_profit,
            "deviation": deviation,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }

        result: any = mt5.order_send(request)

        resumen: str = f'Orden de {accion} realizada. Par: {par} '
        resumen += f'| temporalidad: {temporalidad} | tp: {round(take_profit, 5)} | lot: {lot}'

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f'orden de {accion} fallida, retcode={result.retcode}')

        else:
            print(resumen)
            match temporalidad:
                case '5m':
                    Bot.__lista_5m.append([par, result.order])

                case '15m':
                    Bot.__lista_15m.append([par, result.order])

                case '30m':
                    Bot.__lista_30m.append([par, result.order])

    @staticmethod
    def __cerrar_posicion(temporalidad: str) -> None:
        match temporalidad:
            case '5m':
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_5m]

            case '15m':
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_5m]
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_15m]

            case '30m':
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_5m]
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_15m]
                [mt5.Close(symbol=orden[0], ticket=orden[1]) for orden in Bot.__lista_30m]

    @staticmethod
    def __buscar_hammer(data: list, par: str, temporalidad: str) -> None:
        try:
            data: list = list(data[0])
            apertura: float = data[1]
            maximo: float = data[2]
            minimo: float = data[3]
            cierre: float = data[4]
            rango: float = maximo - minimo
            tipo_vela: str | None = None
            mch_sup: list[bool] | float | None = None
            cuerpo: list[bool] | float | None = None
            mch_inf: list[bool] | float | None = None

            if apertura > cierre:
                tipo_vela: str = 'venta'

            if apertura < cierre:
                tipo_vela: str = 'compra'

            assert tipo_vela is not None

            match tipo_vela:
                case 'venta':
                    mch_sup: float = maximo - apertura
                    cuerpo: float = apertura - cierre
                    mch_inf: float = cierre - minimo

                case 'compra':
                    mch_sup: float = maximo - cierre
                    cuerpo: float = cierre - apertura
                    mch_inf: float = apertura - minimo

            mch_sup: list[bool] = [mch_sup <= rango * 0.2, rango * 0.6 <= mch_sup < rango]
            cuerpo: list[bool] = [rango * 0.15 <= cuerpo < rango * 0.3]
            mch_inf: list[bool] = [rango * 0.6 <= mch_inf < rango, mch_inf <= rango * 0.2]

            if all([mch_sup[0], cuerpo[0], mch_inf[0], tipo_vela == 'compra']):  # Normal
                Bot.__abrir_posicion(accion='compra', par=par, temporalidad=temporalidad)

            if all([mch_sup[1], cuerpo[0], mch_inf[1], tipo_vela == 'venta']):  # Invertido
                Bot.__abrir_posicion(accion='venta', par=par, temporalidad=temporalidad)

        except AssertionError:
            pass

    @staticmethod
    async def __test_5m(par: str) -> None:
        res: list = mt5.copy_rates_from_pos(par, mt5.TIMEFRAME_M5, 1, 1)
        Bot.__buscar_hammer(data=res, par=par, temporalidad='5m')

    @staticmethod
    async def __test_15m(par: str) -> None:
        res: list = mt5.copy_rates_from_pos(par, mt5.TIMEFRAME_M15, 1, 1)
        Bot.__buscar_hammer(data=res, par=par, temporalidad='15m')

    @staticmethod
    async def __test_30m(par: str) -> None:
        res: list = mt5.copy_rates_from_pos(par, mt5.TIMEFRAME_M30, 1, 1)
        Bot.__buscar_hammer(data=res, par=par, temporalidad='30m')

    async def main(self: Self) -> None:
        self.__login()

        while True:
            hora_local: datetime = datetime.now()
            segundos_restantes: int = 60 - hora_local.second

            await asyncio.sleep(segundos_restantes)

            hora_local: datetime = datetime.now()
            minutos: int = hora_local.minute

            tasks: list = []

            if minutos % 5 == 0:
                self.__cerrar_posicion(temporalidad='5m')
                [tasks.append(asyncio.create_task(self.__test_5m(par))) for par in list(self.__pares.keys())]

            if minutos % 15 == 0:
                self.__cerrar_posicion(temporalidad='15m')
                [tasks.append(asyncio.create_task(self.__test_15m(par))) for par in list(self.__pares.keys())]

            if minutos % 30 == 0:
                self.__cerrar_posicion(temporalidad='30m')
                [tasks.append(asyncio.create_task(self.__test_30m(par))) for par in list(self.__pares.keys())]

            if hora_local.hour == self.__hora_apagado and hora_local.minute == self.__minuto_apagado:
                break

            await asyncio.gather(*tasks)


if __name__ == '__main__':
    inicio: None | float = None
    fin: None | float = None
    apagado_automatico: bool = False

    try:
        if not mt5.initialize():
            print('Error de conexión')

        else:
            inicio: float = time.time()
            on: Bot = Bot(13, 00)
            print('Bot iniciado.\n')
            asyncio.run(on.main())
            apagado_automatico: bool = True
            raise KeyboardInterrupt

    except KeyboardInterrupt:
        print('\nBot apagado.')
        mt5.shutdown()
        fin: float = time.time()
        total: float = fin - inicio
        print(f'\n- Tiempo total de ejecución: {total:.2f} segundos')

        if apagado_automatico:
            os.system('shutdown /s /f /t 0')
