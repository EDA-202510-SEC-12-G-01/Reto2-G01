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


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    pass

def print_req_1(catalog, anio):
    tiempo, numero_registros, info = logic.req_1(catalog, anio)
    print(f"Requerimiento 1 - Año: {anio}")
    print(f"Tiempo de ejecución: {tiempo} segundos")
    print(f"Número de registros filtrados: {numero_registros}")
    print("Información del registro más reciente:")
    print(info)


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
    pass


def print_req_4(catalog, tipo_producto, año_inicio, año_fin):
    resultado = logic.req_4_consultar_registros_por_producto_y_rango(catalog, tipo_producto, año_inicio, año_fin)
    print(f"Información sobre los registros filtrados por producto '{tipo_producto}' y rango de años {año_inicio}-{año_fin}:")
    print(f"Total de registros: {resultado['total_registros']}")
    print("Registros por fuente:")
    for fuente, count in resultado['registros_por_fuente'].items():
        print(f"  {fuente}: {count}")
    print(f"Tiempo total de procesamiento: {resultado['tiempo_total']} segundos")
    print("\nPrimeros y últimos registros:")
    if 'registros' in resultado:
        print("\nPrimeros 10 registros:")
        for i, registro in enumerate(resultado['registros'][:10]):
            print(f"{i+1}. {registro}")
        print("\nÚltimos 10 registros:")
        for i, registro in enumerate(resultado['registros'][-10:]):
            print(f"{i+1}. {registro}")



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
    pass


def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    pass


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
            anio = int(input ("Ingrese el año de interes: \n"))
            print_req_1(control,anio)

        elif int(inputs) == 3:
            print_req_2(control)

        elif int(inputs) == 4:
            print_req_3(control)

        elif int(inputs) == 5:
            tipo_producto = (input ("Ingrese el tipo de producto: \n"))
            año_inicio = int(input ("Ingrese el año de inicio: \n"))
            año_fin = int(input ("Ingrese el año de final: \n"))
            print_req_4(control, tipo_producto, año_inicio, año_fin)


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