import os

nombre_directorio_principal: str = 'datos'
subdirectorios: list[str] = ['datasets_filtrados', 'datasets_por_filtrar', 'reporte', 'resultado']
ruta_reporte: str = os.path.join(nombre_directorio_principal, 'reporte', 'reporte.txt')

if not os.path.exists(nombre_directorio_principal):
    os.makedirs(nombre_directorio_principal)

for subdir in subdirectorios:
    ruta_subdirectorio: str = os.path.join(nombre_directorio_principal, subdir)

    if not os.path.exists(ruta_subdirectorio):
        os.makedirs(ruta_subdirectorio)

if not os.path.exists(ruta_reporte):
    with open(ruta_reporte, 'w') as archivo:
        archivo.write('')
