#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detector de registros duplicados en archivos JSON
Este script identifica registros duplicados basados en nombre, primerApellido y segundoApellido
en archivos JSON por cada directorio de entidad.
"""

import os
import json
import pandas as pd
import argparse
from collections import defaultdict
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Procesa los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description='Detector de registros duplicados en archivos JSON por directorio.')
    parser.add_argument('origen', help='Ruta del directorio padre que contiene los subdirectorios con archivos JSON.')
    parser.add_argument('destino', help='Ruta donde se guardará el archivo Excel con los duplicados.')
    return parser.parse_args()

def extraer_datos_persona(registro):
    """Extrae los datos de una persona desde un registro JSON."""
    try:
        if 'declaracion' in registro and 'situacionPatrimonial' in registro['declaracion']:
            datos_generales = registro['declaracion']['situacionPatrimonial'].get('datosGenerales', {})
            
            if datos_generales:
                nombre = datos_generales.get('nombre', '')
                primer_apellido = datos_generales.get('primerApellido', '')
                segundo_apellido = datos_generales.get('segundoApellido', '')
                
                # Obtener información adicional para enriquecer el reporte
                id_registro = registro.get('id', 'Sin ID')
                metadata = registro.get('metadata', {})
                institucion = metadata.get('institucion', 'Sin institución')
                fecha_actualizacion = metadata.get('actualizacion', 'Sin fecha')
                tipo_declaracion = metadata.get('tipo', 'Sin tipo')
                
                return {
                    'id': id_registro,
                    'nombre': nombre,
                    'primerApellido': primer_apellido,
                    'segundoApellido': segundo_apellido,
                    'nombreCompleto': f"{nombre} {primer_apellido} {segundo_apellido}",
                    'institucion': institucion,
                    'fechaActualizacion': fecha_actualizacion,
                    'tipoDeclaracion': tipo_declaracion
                }
    except Exception as e:
        logger.error(f"Error al extraer datos de persona: {e}")
    
    return None

def leer_archivos_json_directorio(directorio):
    """Lee todos los archivos JSON en un directorio y extrae los datos de personas."""
    registros = []
    archivos_procesados = 0
    
    try:
        # Listar solo los archivos JSON en este directorio específico (no recursivo)
        archivos_json = [f for f in os.listdir(directorio) if f.lower().endswith('.json')]
        
        if not archivos_json:
            logger.info(f"No se encontraron archivos JSON en el directorio: {directorio}")
            return registros, archivos_procesados
        
        logger.info(f"Procesando {len(archivos_json)} archivos JSON en: {directorio}")
        
        for archivo in archivos_json:
            ruta_completa = os.path.join(directorio, archivo)
            try:
                with open(ruta_completa, 'r', encoding='utf-8') as f:
                    contenido = json.load(f)
                
                # El archivo puede contener un objeto o un array de objetos
                if isinstance(contenido, dict):
                    contenido = [contenido]
                
                for registro in contenido:
                    datos_persona = extraer_datos_persona(registro)
                    if datos_persona:
                        datos_persona['rutaArchivo'] = ruta_completa
                        registros.append(datos_persona)
                
                archivos_procesados += 1
                
                # Mostrar progreso cada 10 archivos
                if archivos_procesados % 10 == 0:
                    logger.info(f"Procesados {archivos_procesados}/{len(archivos_json)} archivos en {directorio}")
                    
            except Exception as e:
                logger.error(f"Error al procesar archivo {ruta_completa}: {e}")
    
    except Exception as e:
        logger.error(f"Error al leer directorio {directorio}: {e}")
    
    return registros, archivos_procesados

def analizar_duplicados(df):
    """Analiza duplicados en un DataFrame y devuelve los resultados."""
    if df.empty:
        return pd.DataFrame(), 0
    
    # Contar duplicados basados en nombre completo
    duplicados = df[df.duplicated(['nombreCompleto'], keep=False)]
    
    if duplicados.empty:
        return pd.DataFrame(), 0
    
    # Añadir columna de conteo
    conteo = df['nombreCompleto'].value_counts().reset_index()
    conteo.columns = ['nombreCompleto', 'cantidadOcurrencias']
    
    # Unir con el DataFrame original para tener el conteo en cada registro
    duplicados = duplicados.merge(conteo, on='nombreCompleto', how='left')
    
    # Ordenar por cantidad de ocurrencias (descendente) y nombre (ascendente)
    duplicados = duplicados.sort_values(by=['cantidadOcurrencias', 'nombreCompleto'], ascending=[False, True])
    
    return duplicados, len(duplicados['nombreCompleto'].unique())

def guardar_excel(df, ruta_destino, nombre_directorio):
    """Guarda un DataFrame en un archivo Excel existente, creando una nueva hoja."""
    try:
        # Verificar si el archivo ya existe
        if os.path.exists(ruta_destino):
            # Leer el archivo existente
            with pd.ExcelWriter(ruta_destino, engine='openpyxl', mode='a') as writer:
                # Limitar el nombre de la hoja a 31 caracteres (límite de Excel)
                nombre_hoja = nombre_directorio[:31]
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        else:
            # Crear el archivo si no existe
            os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
            with pd.ExcelWriter(ruta_destino, engine='openpyxl') as writer:
                nombre_hoja = nombre_directorio[:31]
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        
        return True
    except Exception as e:
        logger.error(f"Error al guardar Excel para {nombre_directorio}: {e}")
        return False

def crear_hoja_resumen(ruta_destino, resultados_directorios):
    """Crea una hoja de resumen en el archivo Excel."""
    try:
        resumen = pd.DataFrame(resultados_directorios)
        
        # Ordenar por cantidad de duplicados (descendente)
        resumen = resumen.sort_values('cantidadDuplicados', ascending=False)
        
        with pd.ExcelWriter(ruta_destino, engine='openpyxl', mode='a') as writer:
            resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        logger.info(f"Hoja de resumen creada exitosamente en: {ruta_destino}")
        return True
    except Exception as e:
        logger.error(f"Error al crear hoja de resumen: {e}")
        return False

def procesar_directorio_raiz(dir_origen, ruta_destino):
    """Procesa todos los subdirectorios del directorio raíz."""
    # Verificar que el directorio origen existe
    if not os.path.isdir(dir_origen):
        logger.error(f"El directorio origen no existe: {dir_origen}")
        return False
    
    # Si existe, eliminar archivo Excel anterior
    if os.path.exists(ruta_destino):
        try:
            os.remove(ruta_destino)
            logger.info(f"Archivo anterior eliminado: {ruta_destino}")
        except Exception as e:
            logger.error(f"No se pudo eliminar el archivo existente: {e}")
            return False
    
    total_archivos = 0
    total_directorios = 0
    total_registros = 0
    total_duplicados = 0
    resultados_directorios = []
    
    # Listar todos los subdirectorios inmediatos (no recursivo)
    try:
        subdirectorios = [d for d in os.listdir(dir_origen) if os.path.isdir(os.path.join(dir_origen, d))]
        logger.info(f"Se encontraron {len(subdirectorios)} subdirectorios en {dir_origen}")
        
        # Procesar cada subdirectorio por separado
        for i, subdir in enumerate(subdirectorios, 1):
            ruta_subdir = os.path.join(dir_origen, subdir)
            logger.info(f"Procesando directorio {i}/{len(subdirectorios)}: {subdir}")
            
            # Leer todos los archivos JSON en este subdirectorio
            registros, archivos_procesados = leer_archivos_json_directorio(ruta_subdir)
            
            # Actualizar contadores
            total_archivos += archivos_procesados
            if archivos_procesados > 0:
                total_directorios += 1
            
            # Si no hay registros, continuar con el siguiente directorio
            if not registros:
                logger.info(f"No se encontraron registros válidos en {subdir}")
                resultados_directorios.append({
                    'directorio': subdir,
                    'archivos': archivos_procesados,
                    'registros': 0,
                    'cantidadDuplicados': 0,
                    'nombresDuplicados': 0
                })
                continue
            
            # Crear DataFrame con los registros
            df = pd.DataFrame(registros)
            total_registros += len(df)
            
            # Analizar duplicados
            duplicados_df, num_nombres_duplicados = analizar_duplicados(df)
            
            # Guardar duplicados en Excel si existen
            if not duplicados_df.empty:
                guardar_excel(duplicados_df, ruta_destino, subdir)
                total_duplicados += len(duplicados_df)
                logger.info(f"Se encontraron {len(duplicados_df)} registros duplicados ({num_nombres_duplicados} nombres únicos) en {subdir}")
            else:
                logger.info(f"No se encontraron duplicados en {subdir}")
            
            # Guardar resultados para el resumen
            resultados_directorios.append({
                'directorio': subdir,
                'archivos': archivos_procesados,
                'registros': len(df),
                'cantidadDuplicados': len(duplicados_df),
                'nombresDuplicados': num_nombres_duplicados
            })
            
            # Liberar memoria
            del df
            if 'duplicados_df' in locals():
                del duplicados_df
        
        # Crear hoja de resumen
        if resultados_directorios:
            crear_hoja_resumen(ruta_destino, resultados_directorios)
        
        logger.info(f"Procesamiento completado. Total directorios: {total_directorios}, archivos: {total_archivos}, registros: {total_registros}, duplicados: {total_duplicados}")
        return True
        
    except Exception as e:
        logger.error(f"Error al procesar directorio raíz: {e}")
        return False

def main():
    """Función principal del script."""
    # Registrar tiempo de inicio
    tiempo_inicio = datetime.now()
    
    # Procesar argumentos
    args = parse_arguments()
    
    # Preparar ruta de destino
    ruta_destino = args.destino
    if os.path.isdir(ruta_destino):
        ruta_destino = os.path.join(ruta_destino, f"duplicados_{tiempo_inicio.strftime('%Y%m%d_%H%M%S')}.xlsx")
    
    logger.info(f"Directorio origen: {args.origen}")
    logger.info(f"Archivo destino: {ruta_destino}")
    
    # Procesar directorio raíz
    procesar_directorio_raiz(args.origen, ruta_destino)
    
    # Calcular tiempo total
    tiempo_total = datetime.now() - tiempo_inicio
    logger.info(f"Tiempo total de ejecución: {tiempo_total}")
    
    return 0

if __name__ == "__main__":
    exit(main())