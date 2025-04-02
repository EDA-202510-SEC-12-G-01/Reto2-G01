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
    condition = False
    fecha_element_1 = datetime.strptime(element_1['load_time'], '%Y-%m-%d %H:%M:%S')
    fecha_element_2 = datetime.strptime(element_2['load_time'], '%Y-%m-%d %H:%M:%S')
    if fecha_element_1 > fecha_element_2:
        condition = True
    if fecha_element_1 == fecha_element_2:
        if element_1['state_name'] < element_2['state_name']:
            condition = True
    return condition

def sort_criteria_2(element_1, element_2):
    condition = False
    if element_1['income'] > element_2['income']:
        condition = True
    elif element_1['income'] == element_2['income']:
        if element_1['year_collection'] > element_2['year_collection']:
            condition = True
    return condition

def sort_criteria_3(element_1, element_2):
    condition = False
    if element_1['income'] < element_2['income']:
        condition = True
    elif element_1['income'] == element_2['income']:
        if element_1['year_collection'] > element_2['year_collection']:
            condition = True
    return condition

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
    elif requerimiento == "req_7":
        return record
    return record
            
def get_first_last_info(records_list, requerimiento, num = 20):
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
            
    if total <= num:
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

def filtrar_por_unidad_de_medida(records):
    lista = al.new_list()
    for registro in iterator(records):
        if "$" in registro['unit_measurement']:
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
    star_time = get_time()
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
    end_time = get_time()
    time = delta_time(star_time, end_time)
    return round(time, 3), al.size(records_filtrado), num_surveys, num_census, get_first_last_info(records_retorno, "req_3")

def req_4(catalog, tipo_producto, anio_inicio, anio_fin):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    start_time = get_time()
    records = catalog['agricultural_records']
    records_lista = sc.value_set(records)
    records_filtrado = filtrar_por_producto(records_lista, tipo_producto)
    records_filtrado = filtrar_por_año(records_filtrado, anio_inicio, anio_fin)
    num_surveys = 0
    num_census = 0
    for registro in iterator(records_filtrado):
        if registro['source'] == 'SURVEY':
            survey_count += 1
        elif registro['source'] == 'CENSUS':
            census_count += 1
    records_retorno = al.quick_sort(records_filtrado, sort_criteria_1)    
    end_time = get_time()
    tiempo_total = delta_time(start_time, end_time)
    return round(tiempo_total, 3), al.size(records_filtrado), num_surveys, num_census, get_first_last_info(records_retorno, "req_4")

def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog, departamento_interes, fecha_inicio, fecha_fin):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    star_time = get_time()
    records = catalog['agricultural_records']
    records_lista = sc.value_set(records)
    records_filtrado = filtrar_por_departamento(records_lista, departamento_interes)
    records_filtrado = filtrar_por_fecha(records_filtrado, fecha_inicio, fecha_fin)
    num_surveys = 0
    num_census = 0
    for record in iterator(records_filtrado):
        if record['source'] == 'SURVEY':
            num_surveys += 1
        elif record['source'] == 'CENSUS':
            num_census += 1
    records_retorno = al.merge_sort(records_filtrado, sort_criteria_1)
    end_time = get_time()
    time = delta_time(star_time, end_time)
    return round(time, 3), al.size(records_filtrado), num_surveys, num_census, get_first_last_info(records_retorno, "req_6")


def req_7(catalog, departamento, anio_inicial, anio_final, tipo_ordenamiento):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    star_time = get_time()
    records = catalog['agricultural_records']
    records_lista = sc.value_set(records)
    records_filtrado = filtrar_por_año(records_lista, anio_inicial, anio_final)
    records_filtrado = filtrar_por_departamento(records_filtrado, departamento)
    records_filtrado = filtrar_por_unidad_de_medida(records_filtrado)
    """
    Registros filtrados por un intervalo de años y por departamento
    
    Además, se filtran los registros que en su unidad de medida contienen el símbolo "$"
    (Que se indica en el enunciado que son los unicos que se deben considerar)
    
    Además, se consideran validos los value que usa "," como separador de miles y "." como separador decimal.
    Solo se considera inválido los value con caracteres string que no se pueden convertir a float.sss
    """
    records_retorno = al.new_list()
    for i in range(anio_final - anio_inicial + 1):
        dictionario = {
            'year_collection': anio_inicial + i,
            'period_type': "N/A",
            'income': 0,
            'count': 0,
            'invalid': 0,
            'survey': 0,
            'census': 0,
        }
        al.add_last(records_retorno, dictionario) 
    for record in iterator(records_filtrado):
        for i in range(anio_final - anio_inicial + 1):
            if record['year_collection'] == anio_inicial + i:
                al.get_element(records_retorno, i)['count'] += 1
                if record['source'] == 'SURVEY':
                    al.get_element(records_retorno, i)['survey'] += 1
                elif record['source'] == 'CENSUS':
                    al.get_element(records_retorno, i)['census'] += 1
                try:
                    al.get_element(records_retorno, i)['income'] += float(record['value'].replace(",", "").strip())
                except:
                    al.get_element(records_retorno, i)['invalid'] += 1              
    if tipo_ordenamiento == 'ASCENDENTE':
        records_retorno = al.merge_sort(records_retorno, sort_criteria_3)
        al.first_element(records_retorno)['period_type'] = "MENOR"
        al.last_element(records_retorno)['period_type'] = "MAYOR"
    elif tipo_ordenamiento == 'DESCENDENTE':
        records_retorno = al.merge_sort(records_retorno, sort_criteria_2)    
        al.first_element(records_retorno)['period_type'] = "MAYOR"
        al.last_element(records_retorno)['period_type'] = "MENOR"
    end_time = get_time()
    time = delta_time(star_time, end_time)
    return round(time, 3), al.size(records_filtrado), get_first_last_info(records_retorno, "req_7", 15)

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
