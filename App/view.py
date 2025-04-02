import sys
import os
import tabulate as tb

default_limit=100000
sys.setrecursionlimit(default_limit*10)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DataStructures.List.list_iterator import iterator
from App import logic
from DataStructures.List import array_list as al

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    return logic.new_logic()

def print_menu():
    print("Bienvenido")
    print("1- Cargar información")
    print("2- Ejecutar Requerimiento 1")
    print("3- Ejecutar Requerimiento 2")
    print("4- Ejecutar Requerimiento 3")
    print("5- Ejecutar Requerimiento 4")
    print("6- Ejecutar Requerimiento 5")
    print("7- Ejecutar Requerimiento 6")
    print("8- Ejecutar Requerimiento 7")
    print("9- Ejecutar Requerimiento 8 (Bono)")
    print("0- Salir")

def load_data(control, archivo):
    """
    Carga los datos
    """
    #TODO: Realizar la carga de datos
    time, size, menor_anio, mayor_anio, records_recortado = logic.load_data(control, archivo)
    print()
    print("INICIANDO LA CARGA DE DATOS")
    print("========================================================================================================")
    print("SE CARGARON LOS DATOS CORRECTAMENTE")
    print("Tiempo de ejecución en ms: ", time)
    print("Cantidad de registros cargados: ", size)
    print("Año más antiguo de recolección de registros: ", menor_anio)
    print("Año más reciente de recolección de registros: ", mayor_anio)
    print(tb.tabulate(iterator(records_recortado), headers= 'keys' , tablefmt= "fancy_grid"))
    print()

def print_req_1(control):
    departamento = input("Ingrese el departamento a buscar: ")
    time, size, record_formateado = logic.req_2(control, departamento)
    print()
    print("========================================================================================================")
    print("Tiempo de ejecución en ms: ", time)
    print(f"Cantidad de registros encontrados en el departamento {departamento}: ", size)
    print("ULTIMO REGISTRO RECOPILADO")
    print(tb.tabulate([record_formateado], headers= 'keys' , tablefmt= "fancy_grid"))
    print()


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    departamento = input("Ingrese el departamento a buscar: ")
    anio_i = int(input("Ingrese el año inicial del periodo a consultar: "))
    anio_f = int(input("Ingrese el año final del periodo a consultar: "))
    time, size, num_surveys, num_census, records_return = logic.req_3(control, departamento, anio_i, anio_f)
    print()
    print("========================================================================================================")
    print("Tiempo de ejecución en ms: ", time)
    print(f"Cantidad de registros encontrados en el departamento {departamento} entre los años {anio_i} y {anio_f}: ", size)
    print(f"Cantidad de registros de tipo 'SURVEY' encontrados en el departamento {departamento} entre los años {anio_i} y {anio_f}: ", num_surveys)
    print(f"Cantidad de registros de tipo 'CENSUS' encontrados en el departamento {departamento} entre los años {anio_i} y {anio_f}: ", num_census)
    print("RESUMEN DE REGISTROS ENCONTRADOS")
    print(tb.tabulate(iterator(records_return), headers= 'keys' , tablefmt= "fancy_grid"))
    print()


def print_req_4(control):
    producto = input("Ingrese el producto a buscar: ")
    anio_i = int(input("Ingrese el año inicial del periodo a consultar: "))
    anio_f = int(input("Ingrese el año final del periodo a consultar: "))
    time, size, num_surveys, num_census, records_return = logic.req_4(control, producto, anio_i, anio_f)
    print()
    print("========================================================================================================")
    print("Tiempo de ejecución en ms: ", time)
    print(f"Cantidad de registros encontrados en el producto {producto} entre los años {anio_i} y {anio_f}: ", size)
    print(f"Cantidad de registros de tipo 'SURVEY' encontrados en el producto {producto} entre los años {anio_i} y {anio_f}: ", num_surveys)
    print(f"Cantidad de registros de tipo 'CENSUS' encontrados en el producto {producto} entre los años {anio_i} y {anio_f}: ", num_census)
    print("RESUMEN DE REGISTROS ENCONTRADOS")
    print(tb.tabulate(iterator(records_return), headers= 'keys' , tablefmt= "fancy_grid"))
    print()



def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    departamento = input("Ingrese el departamento a buscar: ")
    fecha_i = input('Ingrese la fecha inicial del periodo a consultar (con formato "%Y-%m-%d"): ')
    fecha_f = input('Ingrese la fecha final del periodo a consultar (con formato "%Y-%m-%d"): ')
    time, size, num_surveys, num_census, records_return = logic.req_6(control, departamento, fecha_i, fecha_f)
    print()
    print("========================================================================================================")
    print("Tiempo de ejecución en ms: ", time)
    print(f"Cantidad de registros encontrados en el departamento {departamento} entre las fechas {fecha_i} y {fecha_f}: ", size)
    print(f"Cantidad de registros de tipo 'SURVEY' encontrados en el departamento {departamento} entre las fechas {fecha_i} y {fecha_f}: ", num_surveys)
    print(f"Cantidad de registros de tipo 'CENSUS' encontrados en el departamento {departamento} entre las fechas {fecha_i} y {fecha_f}: ", num_census)
    print("RESUMEN DE REGISTROS ENCONTRADOS")
    print(tb.tabulate(iterator(records_return), headers= 'keys' , tablefmt= "fancy_grid"))
    print()

def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    departamento = input("Ingrese el departamento a buscar: ")
    anio_i = int(input("Ingrese el año inicial del periodo a consultar: "))
    anio_f = int(input("Ingrese el año final del periodo a consultar: "))
    tipo_ordenamiento = input('Ingrese el tipo de ordenamiento ("ASCENDENTE" O "DESCENDENTE"): ')
    time, size, records = logic.req_7(control, departamento, anio_i, anio_f, tipo_ordenamiento)
    print()
    print("========================================================================================================")
    print("Tiempo de ejecución en ms: ", time)
    print(f"Cantidad de registros encontrados en el departamento {departamento} entre los años {anio_i} y {anio_f}: ", size)
    print("RESUMEN DE REGISTROS ENCONTRADOS")
    print(tb.tabulate(iterator(records), headers= 'keys' , tablefmt= "fancy_grid"))
    print()

def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    pass


# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        
        if int(inputs) == 1:
            print("Cargando información de los archivos ....\n")
            archivo = seleccionar_archivo()
            data = load_data(control, archivo)
        elif int(inputs) == 2:
            print_req_1(control)

        elif int(inputs) == 3:
            print_req_2(control)

        elif int(inputs) == 4:
            print_req_3(control)

        elif int(inputs) == 5:
            print_req_4(control)

        elif int(inputs) == 6:
            print_req_5(control)

        elif int(inputs) == 7:
            print_req_6(control)

        elif int(inputs) == 8:
            print_req_7(control)

        elif int(inputs) == 9:
            print_req_8(control)

        elif int(inputs) == 0:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)

def seleccionar_archivo():
    print("Escoja el archivo a cargar")
    print("1- agricultural-20.csv")
    print("2- agricultural-40.csv")
    print("3- agricultural-60.csv")
    print("4- agricultural-80.csv")
    print("5- agricultural-100.csv")
    inputs = input('Seleccione una opción para continuar\n')
    if int(inputs) == 1:
        return "/agricultural-20.csv"
    elif int(inputs) == 2:
        return "/agricultural-40.csv"
    elif int(inputs) == 3:
        return "/agricultural-60.csv"
    elif int(inputs) == 4:
        return "/agricultural-80.csv"
    elif int(inputs) == 5:
        return "/agricultural-100.csv"
    else:
        print("Opción errónea, vuelva a elegir.\n")
        seleccionar_archivo()