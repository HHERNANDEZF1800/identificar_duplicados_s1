#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detector de registros duplicados en archivos JSON
Este script identifica registros duplicados basados en nombre, primerApellido y segundoApellido
en archivos JSON distribuidos en una estructura de directorios.
"""

import os
import json
import pandas as pd
import argparse
from collections import defaultdict
from pathlib import Path
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Procesa los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description='Detector de registros duplicados en archivos JSON.')
    parser.add_argument('origen', help='Ruta del directorio padre que contiene los subdirectorios con archivos JSON.')
    parser.add_argument('destino', help='Ruta donde se guardará el archivo Excel con los duplicados.')
    return parser.parse_args()

def procesar_archivo_json(ruta_archivo, datos_personas):
    """Procesa un archivo JSON y extrae la información de las personas."""
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            contenido = json.load(f)
            
        # El archivo puede contener un objeto o un array de objetos
        if isinstance(contenido, dict):
            contenido = [contenido]
            
        for registro in contenido:
            try:
                # Navegar a través de la estructura anidada para obtener datos de la persona
                if 'declaracion' in registro and 'situacionPatrimonial' in registro['declaracion']:
                    datos_generales = registro['declaracion']['situacionPatrimonial'].get('datosGenerales', {})
                    
                    if datos_generales:
                        nombre = datos_generales.get('nombre', '')
                        primer_apellido = datos_generales.get('primerApellido', '')
                        segundo_apellido = datos_generales.get('segundoApellido', '')
                        
                        # Si tenemos al menos nombre y primer apellido
                        if nombre and primer_apellido:
                            # Crear clave única para la persona
                            persona_key = f"{nombre}|{primer_apellido}|{segundo_apellido}"
                            
                            # Obtener información adicional para enriquecer el reporte
                            id_registro = registro.get('id', 'Sin ID')
                            metadata = registro.get('metadata', {})
                            institucion = metadata.get('institucion', 'Sin institución')
                            fecha_actualizacion = metadata.get('actualizacion', 'Sin fecha')
                            
                            # Guardar información de la persona
                            datos_personas[persona_key].append({
                                'id': id_registro,
                                'nombre': nombre,
                                'primerApellido': primer_apellido,
                                'segundoApellido': segundo_apellido,
                                'institucion': institucion,
                                'fechaActualizacion': fecha_actualizacion,
                                'rutaArchivo': str(ruta_archivo)
                            })
            except Exception as e:
                logger.error(f"Error al procesar registro en {ruta_archivo}: {e}")
                
    except Exception as e:
        logger.error(f"Error al procesar archivo {ruta_archivo}: {e}")

def explorar_directorios(dir_origen):
    """Explora recursivamente los directorios y procesa los archivos JSON."""
    datos_personas = defaultdict(list)
    archivos_procesados = 0
    
    logger.info(f"Comenzando exploración de: {dir_origen}")
    
    try:
        # Recorrer todos los subdirectorios
        for ruta_actual, subdirs, archivos in os.walk(dir_origen):
            for archivo in archivos:
                if archivo.lower().endswith('.json'):
                    ruta_completa = os.path.join(ruta_actual, archivo)
                    procesar_archivo_json(ruta_completa, datos_personas)
                    archivos_procesados += 1
                    
                    # Mostrar progreso cada 100 archivos
                    if archivos_procesados % 100 == 0:
                        logger.info(f"Procesados {archivos_procesados} archivos JSON...")
    except Exception as e:
        logger.error(f"Error al explorar directorios: {e}")
        
    logger.info(f"Procesamiento completado. Total archivos JSON procesados: {archivos_procesados}")
    
    return datos_personas

def generar_informe_duplicados(datos_personas, ruta_destino):
    """Genera un informe Excel con los registros duplicados."""
    try:
        # Filtrar solo los registros duplicados (aparecen más de una vez)
        duplicados = {k: v for k, v in datos_personas.items() if len(v) > 1}
        
        if not duplicados:
            logger.info("No se encontraron registros duplicados.")
            return False
        
        # Preparar datos para el DataFrame
        registros = []
        for persona_key, ocurrencias in duplicados.items():
            nombre, primer_apellido, segundo_apellido = persona_key.split('|')
            cantidad = len(ocurrencias)
            
            # Extraer información de cada ocurrencia
            for i, ocurrencia in enumerate(ocurrencias, 1):
                registros.append({
                    'nombre': nombre,
                    'primerApellido': primer_apellido,
                    'segundoApellido': segundo_apellido,
                    'cantidadOcurrencias': cantidad,
                    'ocurrenciaNumero': i,
                    'id': ocurrencia['id'],
                    'institucion': ocurrencia['institucion'],
                    'fechaActualizacion': ocurrencia['fechaActualizacion'],
                    'rutaArchivo': ocurrencia['rutaArchivo']
                })
        
        # Crear DataFrame
        df = pd.DataFrame(registros)
        
        # Asegurar que el directorio de destino existe
        os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
        
        # Guardar como Excel
        df.to_excel(ruta_destino, index=False, sheet_name='Registros Duplicados')
        
        logger.info(f"Informe de duplicados generado exitosamente en: {ruta_destino}")
        logger.info(f"Se encontraron {len(duplicados)} registros duplicados con un total de {len(registros)} ocurrencias.")
        
        return True
    except Exception as e:
        logger.error(f"Error al generar informe: {e}")
        return False

def main():
    """Función principal del script."""
    args = parse_arguments()
    
    # Verificar que el directorio origen existe
    if not os.path.isdir(args.origen):
        logger.error(f"El directorio origen no existe: {args.origen}")
        return 1
    
    # Preparar ruta de destino
    ruta_destino = args.destino
    if os.path.isdir(ruta_destino):
        ruta_destino = os.path.join(ruta_destino, "duplicados.xlsx")
        
    logger.info(f"Directorio origen: {args.origen}")
    logger.info(f"Archivo destino: {ruta_destino}")
    
    # Procesar archivos y generar informe
    datos_personas = explorar_directorios(args.origen)
    generar_informe_duplicados(datos_personas, ruta_destino)
    
    return 0

if __name__ == "__main__":
    exit(main())