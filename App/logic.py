import time
import sys
import os
import csv
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_limit=1000000
sys.setrecursionlimit(default_limit*10)

from DataStructures.List import array_list as al
from DataStructures.List.list_iterator import iterator
from DataStructures.Map import map_separate_chaining as sc

data_dir = os.path.dirname(os.path.realpath('__file__')) + '/Data/'

csv.field_size_limit(2147483647)

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    catalog = {'agricultural_records' : sc.new_map(1000000,2.5)}
    return catalog

# Funciones para la carga de datos
def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    star_time = get_time()
    records = catalog['agricultural_records']
    file_path = data_dir + filename
    with open(file_path, encoding='utf-8') as file:
        input_file = csv.DictReader(file)
        i = 1
        for row in input_file:
            row['year_collection'] = int(row['year_collection'])
            sc.put(records, str(i) + ',' + row['load_time'], row)
            i += 1
    records_retorno = al.merge_sort(sc.value_set(records), sort_criteria_1)
    end_time = get_time()
    time = delta_time(star_time, end_time)
    return round(time,3), sc.size(records), get_min_year(catalog), get_max_year(catalog), get_first_last_info(records_retorno, 'carga_datos')

def get_min_year(catalog):
    """
    Retorna el menor año de recolección encontrado en la lista de registros.
    """
    lowest = float('inf')
    for i in iterator(sc.value_set(catalog['agricultural_records'])):
        if i['year_collection'] < lowest:
            lowest = i['year_collection']
    return lowest

def get_max_year(catalog):
    """
    Retorna el mayor año de recolección encontrado en la lista de registros.
    """
    lowest = float('-inf')
    for i in iterator(sc.value_set(catalog['agricultural_records'])):
        if i['year_collection'] > lowest:
            lowest = i['year_collection']
    return lowest

def sort_criteria_1(element_1, element_2):
    is_sorted = False
    fecha_element_1 = datetime.strptime(element_1['load_time'], '%Y-%m-%d %H:%M:%S')
    fecha_element_2 = datetime.strptime(element_2['load_time'], '%Y-%m-%d %H:%M:%S')
    if fecha_element_1 > fecha_element_2:
        is_sorted = True
    if fecha_element_1 == fecha_element_2:
        if element_1['state_name'] <= element_2['state_name']:
            is_sorted = True
    return is_sorted


def extract_info(record, requerimiento):
    if requerimiento == "carga_datos":
        return {
            'year_collection': record['year_collection'],
            'load_time': record['load_time'],
            'state_name': record['state_name'],
            'source': record['source'],
            'unit_measurement': record['unit_measurement'],
            'value': record['value']
        }
    elif requerimiento == "req_1":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'source': record['source'],
            'freq_collection': record['freq_collection'],
            'state_name': record['state_name'],
            'commodity': record['commodity'],
            'unit_measurement': record['unit_measurement'],
            'value': record['value']
        }
    elif requerimiento ==  "req_2":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'source': record['source'],
            'freq_collection': record['freq_collection'],
            'state_name': record['state_name'],
            'commodity': record['commodity'],
            'unit_measurement': record['unit_measurement'],
            'value': record['value']
        }
    elif requerimiento == "req_3":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'source': record['source'],
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'freq_collection': record['freq_collection'],
            'commodity': record['commodity'],
            'unit_measurement': record['unit_measurement']
        }
    elif requerimiento == "req_4":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'source': record['source'],
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'freq_collection': record['freq_collection'],
            'state_name': record['state_name'],
            'unit_measurement': record['unit_measurement']
        }
    elif requerimiento == "req_5":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'source': record['source'],
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'freq_collection': record['freq_collection'],
            'state_name': record['state_name'],
            'unit_measurement': record['unit_measurement'],
            'commodity': record['commodity'],
        }
    elif requerimiento == "req_6":
        dt = datetime.strptime(record['load_time'], "%Y-%m-%d %H:%M:%S")
        return {
            'source': record['source'],
            'year_collection': record['year_collection'],
            'load_time': dt.strftime("%Y-%m-%d"),
            'freq_collection': record['freq_collection'],
            'state_name': record['state_name'],
            'unit_measurement': record['unit_measurement'],
            'commodity': record['commodity']
        }
    return record
            
def get_first_last_info(records_list, requerimiento):
    """
    Retorna una nueva lista (del mismo tipo que la original) que contiene los primeros 5 y 
    los últimos 5 registros con la siguiente información:
      - year_collection: Año de recolección.
      - load_time: Fecha de carga del registro.
      - state_name: Nombre del departamento.
      - source: Fuente/origen (ej. "CENSUS" o "SURVEY").
      - unit_measurement: Unidad de medición (ej. "HEAD", "$", etc.).
      - value: Valor de la medición.
    
    Si la lista original tiene 10 o menos registros, se retornan todos.    
    """
    total = al.size(records_list)
    new_list_ret = al.new_list()
            
    if total <= 20:
        for i in range(total):
            rec = al.get_element(records_list, i)
            al.add_last(new_list_ret, extract_info(rec, requerimiento))
    else:
        for i in range(5):
            rec = al.get_element(records_list, i)
            al.add_last(new_list_ret, extract_info(rec, requerimiento))
        for i in range(total - 5, total):
            rec = al.get_element(records_list, i)
            al.add_last(new_list_ret, extract_info(rec, requerimiento))
    return new_list_ret

# Funciones de consulta sobre el catálogo
def filtrar_por_año(records_lista, anio_inicial, anio_final):
    lista = al.new_list()
    for registro in iterator(records_lista):
        if anio_inicial <= registro['year_collection'] <= anio_final:
            al.add_last(lista, registro)
    return lista

def filtrar_por_fecha(records_lista, fecha_inicial, fecha_final):
    lista = al.new_list()
    fecha_i = datetime.strptime(fecha_inicial + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    fecha_f = datetime.strptime(fecha_final + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    for registro in iterator(records_lista):
        fecha_r = datetime.strptime(registro['load_time'], "%Y-%m-%d %H:%M:%S")
        if fecha_i <= fecha_r <= fecha_f:
            al.add_last(lista, registro)
    return lista

def filtrar_por_departamento(records_lista, departamento):
    lista = al.new_list()
    for registro in iterator(records_lista):
        if registro['state_name'] == departamento:
            al.add_last(lista, registro)
    return lista

def filtrar_por_producto(records_lista, producto):
    lista = al.new_list()
    for registro in iterator(records_lista):
        if registro['commodity'] == producto:
            al.add_last(lista, registro)
    return lista

def filtrar_por_categoria_estadistica(records_lista, categoria):
    lista = al.new_list()
    for registro in iterator(records_lista):
        if registro['statical_category'] == categoria:
            al.add_last(lista, registro)
    return lista


def req_1(catalog, anio):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    star_time = get_time()
    records = catalog["agricultural_records"]
    records_lista = sc.value_set(records)
    records_filtrado = filtrar_por_año(records_lista, anio, anio)
    maxima_fecha_carga = float('-inf')
    record_mas_reciente = None
    for i in iterator(records_filtrado):
        dt = datetime.strptime(i["load_time"], "%Y-%m-%d %H:%M:%S").timestamp()
        if maxima_fecha_carga < dt:
            maxima_fecha_carga = dt
            record_mas_reciente = i
    end_time = get_time()
    time = delta_time(star_time, end_time)
    return round(time, 3), al.size(records_filtrado), extract_info(record_mas_reciente, "req_1")

def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog, departamento, anio_inicial, anio_final):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start = get_time()
    records = catalog['agricultural_records']
    records_lista = sc.value_set(records)
    records_filtrado = filtrar_por_año(records_lista, anio_inicial, anio_final)
    records_filtrado = filtrar_por_departamento(records_filtrado, departamento)
    num_surveys = 0
    num_census = 0
    for record in iterator(records_filtrado):
        if record['source'] == 'SURVEY':
            num_surveys += 1
        elif record['source'] == 'CENSUS':
            num_census += 1
    records_retorno = al.merge_sort(records_filtrado, sort_criteria_1)
    end = get_time()
    elapsed = delta_time(start, end)
    return round(elapsed, 3), al.size(records_filtrado), num_surveys, num_census, get_first_last_info(records_retorno, "req_3")

def req_4_consultar_registros_por_producto_y_rango(catalog, tipo_producto, año_inicio, año_fin):
    start_time = get_time()
    registros_filtrados = filtrar_por_producto(catalog['agricultural_records'], tipo_producto)
    registros_filtrados = filtrar_por_año(registros_filtrados, año_inicio, año_fin)
    registros_filtrados = al.merge_sort(registros_filtrados, sort_criteria_1)
    if al.size(registros_filtrados) > 20:
        registros_filtrados = get_first_last_info(registros_filtrados, 'req_4')
    survey_count = 0
    census_count = 0
    for registro in iterator(registros_filtrados):
        if registro['source'] == 'SURVEY':
            survey_count += 1
        elif registro['source'] == 'CENSUS':
            census_count += 1
    end_time = get_time()
    tiempo_total = delta_time(start_time, end_time)
    return {
        'total_registros': al.size(registros_filtrados),
        'registros_por_fuente': {
            'SURVEY': survey_count,
            'CENSUS': census_count
        },
        'tiempo_total': tiempo_total
    }



def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


def req_7_analizar_ingresos_por_departamento(catalog, departamento, año_inicio, año_fin, orden):
    start_time = get_time()
    registros_filtrados = filtrar_por_departamento(catalog['agricultural_records'], departamento)
    registros_filtrados = filtrar_por_año(registros_filtrados, año_inicio, año_fin)
    lista_final = al.new_list()
    for registro in iterator(registros_filtrados):
        if 'unit_measurement' in registro and "$" in registro['unit_measurement']:
            if registro['value'] not in ["(D)", None]:
                al.add_last(lista_final, registro)
    mapa_ingresos = {}  
    for registro in iterator(lista_final):
        año = registro['year_collection']
        try:
            valor = float(registro['value']) if isinstance(registro['value'], str) and registro['value'].replace('.', '', 1).isdigit() else 0
        except:
            valor = 0  
        if año not in mapa_ingresos:
            mapa_ingresos[año] = {'ingreso_total': 0, 'cantidad_registros': 0}
        mapa_ingresos[año]['ingreso_total'] += valor
        mapa_ingresos[año]['cantidad_registros'] += 1
    ingresos_list = al.new_list()
    for año, data in mapa_ingresos.items():
        al.add_last(ingresos_list, {
            'year_collection': año,
            'ingreso_total': data['ingreso_total'],
            'cantidad_registros': data['cantidad_registros']
        })
    if orden == 'ascendente':
        ingresos_ordenados = al.merge_sort(ingresos_list, lambda x, y: 
            (x['ingreso_total'], -x['cantidad_registros']) < (y['ingreso_total'], -y['cantidad_registros'])
        )
    else:
        ingresos_ordenados = al.merge_sort(ingresos_list, lambda x, y: 
            (x['ingreso_total'], -x['cantidad_registros']) > (y['ingreso_total'], -y['cantidad_registros'])
        )
    total_registros = al.size(ingresos_ordenados)
    registros_filtrados_ordenados = get_first_last_info(ingresos_ordenados, None) if total_registros > 15 else ingresos_ordenados
    count_invalidos = 0
    count_survey = 0
    count_census = 0
    for registro in iterator(lista_final): 
        if registro.get('source') == 'SURVEY':
            count_survey += 1
        elif registro.get('source') == 'CENSUS':
            count_census += 1
        if registro['value'] in ["(D)", None] or 'ingreso_total' in registro and (registro['ingreso_total'] is None or registro['ingreso_total'] <= 0):
            count_invalidos += 1
    max_ingreso = max(iterator(registros_filtrados_ordenados), key=lambda r: r['ingreso_total'], default=None)
    min_ingreso = min(iterator(registros_filtrados_ordenados), key=lambda r: r['ingreso_total'], default=None)
    for registro in iterator(registros_filtrados_ordenados):
        if max_ingreso and registro['year_collection'] == max_ingreso['year_collection']:
            registro['indicación'] = "MAYOR"
        elif min_ingreso and registro['year_collection'] == min_ingreso['year_collection']:
            registro['indicación'] = "MENOR"
        else:
            registro['indicación'] = ""
    end_time = get_time()
    execution_time = delta_time(start_time, end_time)
    return {
        'Tiempo de ejecución (ms)': round(execution_time, 3),
        'Total de registros encontrados': total_registros,
        'Total de registros con fuente "SURVEY"': count_survey,
        'Total de registros con fuente "CENSUS"': count_census,
        'Total de registros inválidos con "$"': count_invalidos,
        'Ingresos por año': registros_filtrados_ordenados
    }


def req_8_analizar_tiempos_de_carga(catalog, N, orden):
    start_time = get_time() 
    registros_validos = al.new_list()
    for record in iterator(catalog['agricultural_records']):
        if record['unit_measurement'] != 'D': 
            al.add_last(registros_validos, record)
    departamentos = al.new_list()
    for record in iterator(registros_validos):
        dep_info = {
            'state_name': record['state_name'],
            'year_collection': record['year_collection'],
            'load_time': record['load_time'],
            'registros': 1,
            'min_time': float('inf'),
            'max_time': float('-inf'),
            'min_year': record['year_collection'],
            'max_year': record['year_collection'],
            'survey_count': 0,
            'census_count': 0
        }
        dep_found = al.is_present(departamentos, dep_info, al.default_function)
        if dep_found == -1:
            al.add_last(departamentos, dep_info)
        else:
            dep = al.get_element(departamentos, dep_found)
            dep['registros'] += 1
            dep['min_year'] = min(dep['min_year'], record['year_collection'])
            dep['max_year'] = max(dep['max_year'], record['year_collection'])
            tiempo_carga = record['year_collection'] - int(record['load_time'][:4])
            dep['min_time'] = min(dep['min_time'], tiempo_carga)
            dep['max_time'] = max(dep['max_time'], tiempo_carga)
            if record['source'] == 'SURVEY':
                dep['survey_count'] += 1
            if record['source'] == 'CENSUS':
                dep['census_count'] += 1
    for dep in iterator(departamentos):
        total_time = 0
        count = 0
        for record in iterator(registros_validos):
            if record['state_name'] == dep['state_name']:
                total_time += (record['year_collection'] - int(record['load_time'][:4])) 
                count += 1
        dep['promedio_tiempo'] = total_time / count if count > 0 else 0
    departamentos_ordenados = al.new_list()
    for dep in iterator(departamentos):
        al.add_last(departamentos_ordenados, dep)
    if orden == "ASCENDENTE":
        departamentos_ordenados = al.merge_sort(departamentos_ordenados, lambda a, b: a['promedio_tiempo'] < b['promedio_tiempo'])
    else:
        departamentos_ordenados = al.merge_sort(departamentos_ordenados, lambda a, b: a['promedio_tiempo'] > b['promedio_tiempo'])
    total_departamentos = al.size(departamentos_ordenados)
    if total_departamentos > 15:
        departamentos_ordenados = al.sub_list(departamentos_ordenados, 0, N)
        departamentos_ordenados = al.add_last(departamentos_ordenados, al.sub_list(departamentos_ordenados, total_departamentos - N, total_departamentos))
    count_departamentos = al.size(departamentos_ordenados)
    tiempo_total = sum([dep['promedio_tiempo'] for dep in iterator(departamentos_ordenados)])
    tiempo_promedio_total = tiempo_total / count_departamentos if count_departamentos > 0 else 0
    menor_año = min([dep['min_year'] for dep in iterator(departamentos_ordenados)], default=float('inf'))
    mayor_año = max([dep['max_year'] for dep in iterator(departamentos_ordenados)], default=float('-inf'))
    resultado = {
        'Tiempo de ejecución (ms)': round(delta_time(start_time, get_time()), 3),
        'Número total de departamentos': count_departamentos,
        'Tiempo promedio de carga': round(tiempo_promedio_total, 3),
        'Menor año de recopilación': menor_año,
        'Mayor año de recopilación': mayor_año,
        'Departamentos': [],
        'Total "SURVEY"': sum([dep['survey_count'] for dep in iterator(departamentos_ordenados)]),
        'Total "CENSUS"': sum([dep['census_count'] for dep in iterator(departamentos_ordenados)]),
    }
    for dep in iterator(departamentos_ordenados):
        dep_info = {
            'Departamento': dep['state_name'],
            'Promedio tiempo de carga': round(dep['promedio_tiempo'], 3),
            'Número de registros': dep['registros'],
            'Menor año de recopilación': dep['min_year'],
            'Mayor año de recopilación': dep['max_year'],
            'Menor tiempo de carga': dep['min_time'],
            'Mayor tiempo de carga': dep['max_time'],
            'Total "SURVEY"': dep['survey_count'],
            'Total "CENSUS"': dep['census_count']
        }
        resultado['Departamentos'].append(dep_info)
    return resultado



# Funciones para medir tiempos de ejecucion
def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
