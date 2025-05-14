#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detector de registros duplicados en archivos JSON
Este script identifica registros duplicados basados en nombre, primerApellido y segundoApellido
en archivos JSON por cada directorio de entidad y guarda resultados en archivos CSV separados.
"""

import os
import sys
import json
import pandas as pd
import argparse
from collections import defaultdict
import logging
from datetime import datetime
import tempfile
import platform
import getpass
import subprocess
import csv

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
    parser.add_argument('destino', help='Ruta del directorio donde se guardarán los archivos CSV de duplicados.')
    parser.add_argument('--temporal', action='store_true', help='Usar directorio temporal para guardar los archivos')
    parser.add_argument('--debug', action='store_true', help='Mostrar información detallada de depuración')
    return parser.parse_args()

def mostrar_info_sistema():
    """Muestra información del sistema para diagnóstico."""
    info = {
        "Sistema operativo": platform.system(),
        "Versión": platform.version(),
        "Arquitectura": platform.architecture(),
        "Usuario": getpass.getuser(),
        "Directorio actual": os.getcwd(),
        "Python versión": sys.version,
        "Pandas versión": pd.__version__
    }
    
    logger.info("Información del sistema:")
    for clave, valor in info.items():
        logger.info(f"  {clave}: {valor}")

def verificar_permisos_directorio(ruta):
    """Verifica los permisos de un directorio e intenta arreglarlos."""
    # Normalizar la ruta (para evitar problemas con / o \)
    ruta = os.path.normpath(ruta)
    
    logger.info(f"Verificando permisos para: {ruta}")
    
    # Si la ruta no existe, intentar crearla
    if not os.path.exists(ruta):
        try:
            os.makedirs(ruta, exist_ok=True, mode=0o777)
            logger.info(f"Directorio creado: {ruta}")
        except Exception as e:
            logger.error(f"No se pudo crear el directorio: {e}")
            return False
    
    # Verificar si es un directorio
    if not os.path.isdir(ruta):
        logger.error(f"La ruta no es un directorio: {ruta}")
        return False
    
    # Verificar permisos de escritura
    if not os.access(ruta, os.W_OK):
        logger.error(f"No tienes permisos de escritura en: {ruta}")
        
        # Intentar cambiar permisos (solo en sistemas Unix)
        if platform.system() != "Windows":
            try:
                logger.info(f"Intentando cambiar permisos a 777 para: {ruta}")
                subprocess.run(['chmod', '777', ruta], check=True)
                logger.info("Permisos cambiados exitosamente")
                
                # Verificar de nuevo después del cambio
                if os.access(ruta, os.W_OK):
                    logger.info("Ahora se tienen permisos de escritura")
                    return True
                else:
                    logger.error("Aún no se tienen permisos de escritura después de cambiar a 777")
                    return False
            except Exception as e:
                logger.error(f"No se pudieron cambiar los permisos: {e}")
                return False
        return False
    
    # Intentar crear un archivo temporal para verificar realmente los permisos
    try:
        test_file_path = os.path.join(ruta, f"test_permisos_{datetime.now().strftime('%H%M%S')}.tmp")
        with open(test_file_path, 'w') as f:
            f.write("test")
        os.remove(test_file_path)
        logger.info(f"Prueba de escritura exitosa en: {ruta}")
        return True
    except Exception as e:
        logger.error(f"No se pudo escribir archivo de prueba en el directorio: {e}")
        return False

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

def guardar_csv(df, directorio_destino, nombre_directorio, usar_temp=False):
    """Guarda un DataFrame en un archivo CSV."""
    # Si se solicita usar directorio temporal, cambiar la ruta
    if usar_temp:
        dir_temp = tempfile.gettempdir()
        directorio_destino = dir_temp
        logger.info(f"Usando directorio temporal: {dir_temp}")
    
    try:
        # Crear directorio si no existe
        if not os.path.exists(directorio_destino):
            try:
                os.makedirs(directorio_destino, exist_ok=True)
                logger.info(f"Directorio creado: {directorio_destino}")
            except Exception as e:
                logger.error(f"No se pudo crear el directorio: {e}")
                
                # Intentar usar directorio temporal si no se pudo crear el directorio
                if not usar_temp:
                    dir_temp = tempfile.gettempdir()
                    directorio_destino = dir_temp
                    logger.info(f"Usando directorio temporal: {dir_temp}")
                else:
                    return False
        
        # Crear nombre de archivo
        # Reemplazar caracteres problemáticos en el nombre del directorio
        nombre_archivo_seguro = nombre_directorio.replace('/', '_').replace('\\', '_').replace(' ', '_')
        nombre_archivo = f"duplicados_{nombre_archivo_seguro}.csv"
        ruta_archivo = os.path.join(directorio_destino, nombre_archivo)
        
        # Guardar DataFrame como CSV
        try:
            df.to_csv(ruta_archivo, index=False, encoding='utf-8')
            logger.info(f"Archivo CSV guardado: {ruta_archivo}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar CSV: {e}")
            
            # Si hay error y no estamos usando directorio temporal, intentar con directorio temporal
            if not usar_temp and "Permission denied" in str(e):
                dir_temp = tempfile.gettempdir()
                ruta_temp = os.path.join(dir_temp, nombre_archivo)
                logger.info(f"Intentando guardar en ubicación temporal: {ruta_temp}")
                
                try:
                    df.to_csv(ruta_temp, index=False, encoding='utf-8')
                    logger.info(f"Archivo guardado en ubicación temporal: {ruta_temp}")
                    return True
                except Exception as temp_error:
                    logger.error(f"Error al guardar en ubicación temporal: {temp_error}")
                    return False
            
            return False
        
    except Exception as e:
        logger.error(f"Error general al guardar CSV para {nombre_directorio}: {e}")
        return False

def guardar_resumen(resumen_df, directorio_destino, usar_temp=False):
    """Guarda un DataFrame de resumen en un archivo CSV."""
    # Si se solicita usar directorio temporal, cambiar la ruta
    if usar_temp:
        dir_temp = tempfile.gettempdir()
        directorio_destino = dir_temp
    
    try:
        # Crear nombre de archivo
        nombre_archivo = f"resumen_duplicados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        ruta_archivo = os.path.join(directorio_destino, nombre_archivo)
        
        # Guardar DataFrame como CSV
        try:
            resumen_df.to_csv(ruta_archivo, index=False, encoding='utf-8')
            logger.info(f"Archivo de resumen guardado: {ruta_archivo}")
            return True
        except Exception as e:
            logger.error(f"Error al guardar resumen: {e}")
            
            # Si hay error y no estamos usando directorio temporal, intentar con directorio temporal
            if not usar_temp:
                dir_temp = tempfile.gettempdir()
                ruta_temp = os.path.join(dir_temp, nombre_archivo)
                logger.info(f"Intentando guardar resumen en ubicación temporal: {ruta_temp}")
                
                try:
                    resumen_df.to_csv(ruta_temp, index=False, encoding='utf-8')
                    logger.info(f"Resumen guardado en ubicación temporal: {ruta_temp}")
                    return True
                except Exception as temp_error:
                    logger.error(f"Error al guardar resumen en ubicación temporal: {temp_error}")
                    return False
            
            return False
        
    except Exception as e:
        logger.error(f"Error general al guardar resumen: {e}")
        return False

def procesar_directorio_raiz(dir_origen, dir_destino, usar_temp=False):
    """Procesa todos los subdirectorios del directorio raíz."""
    # Verificar que el directorio origen existe
    if not os.path.isdir(dir_origen):
        logger.error(f"El directorio origen no existe: {dir_origen}")
        return False
    
    # Si se solicita usar directorio temporal, cambiar la ruta de destino
    if usar_temp:
        dir_temp = tempfile.gettempdir()
        dir_destino = dir_temp
        logger.info(f"Usando directorio temporal para guardar resultados: {dir_temp}")
    else:
        dir_destino = dir_destino
    
    # Verificar permisos en directorio de destino (si no estamos usando directorio temporal)
    if not usar_temp:
        if not verificar_permisos_directorio(dir_destino):
            logger.warning("No se pudieron verificar/arreglar los permisos en el directorio de destino")
            logger.info("Usando directorio temporal como alternativa...")
            usar_temp = True
            dir_temp = tempfile.gettempdir()
            dir_destino = dir_temp
            logger.info(f"Nuevo directorio de destino (temporal): {dir_destino}")
    
    total_archivos = 0
    total_directorios = 0
    total_registros = 0
    total_duplicados = 0
    resultados_directorios = []
    
    # Listar todos los subdirectorios inmediatos (no recursivo)
    try:
        subdirectorios = [d for d in os.listdir(dir_origen) if os.path.isdir(os.path.join(dir_origen, d))]
        
        if not subdirectorios:
            logger.warning(f"No se encontraron subdirectorios en {dir_origen}")
            logger.info("El script está diseñado para procesar archivos JSON dentro de subdirectorios")
            logger.info("La estructura debe ser: directorio_raíz/entidad1/*.json, directorio_raíz/entidad2/*.json, etc.")
            return False
            
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
            
            # Guardar duplicados en CSV si existen
            if not duplicados_df.empty:
                resultado = guardar_csv(duplicados_df, dir_destino, subdir, usar_temp)
                if resultado:
                    total_duplicados += len(duplicados_df)
                    logger.info(f"Se encontraron {len(duplicados_df)} registros duplicados ({num_nombres_duplicados} nombres únicos) en {subdir}")
                else:
                    logger.warning(f"No se pudieron guardar los duplicados de {subdir}")
            else:
                logger.info(f"No se encontraron duplicados en {subdir}")
            
            # Guardar resultados para el resumen
            resultados_directorios.append({
                'directorio': subdir,
                'archivos': archivos_procesados,
                'registros': len(df),
                'cantidadDuplicados': len(duplicados_df) if 'duplicados_df' in locals() and not duplicados_df.empty else 0,
                'nombresDuplicados': num_nombres_duplicados if 'num_nombres_duplicados' in locals() else 0
            })
            
            # Liberar memoria
            del df
            if 'duplicados_df' in locals() and not duplicados_df.empty:
                del duplicados_df
        
        # Crear archivo de resumen
        if resultados_directorios:
            resumen_df = pd.DataFrame(resultados_directorios)
            # Ordenar por cantidad de duplicados (descendente)
            resumen_df = resumen_df.sort_values('cantidadDuplicados', ascending=False)
            guardar_resumen(resumen_df, dir_destino, usar_temp)
        
        logger.info(f"Procesamiento completado. Total directorios: {total_directorios}, archivos: {total_archivos}, registros: {total_registros}, duplicados: {total_duplicados}")
        
        # Mostrar ruta final donde se guardaron los archivos
        if usar_temp:
            logger.info(f"IMPORTANTE: Debido a problemas de permisos, los archivos se guardaron en: {dir_destino}")
            logger.info(f"Copia estos archivos a tu ubicación deseada manualmente.")
        else:
            logger.info(f"Archivos CSV guardados en: {dir_destino}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error al procesar directorio raíz: {e}")
        if "Permission denied" in str(e):
            logger.info("RECOMENDACIÓN: Ejecuta el script con permisos de administrador o cambia la ruta")
        return False

def main():
    """Función principal del script."""
    # Registrar tiempo de inicio
    tiempo_inicio = datetime.now()
    
    # Procesar argumentos
    args = parse_arguments()
    
    # Establecer nivel de log
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Modo debug activado")
    
    # Mostrar información del sistema
    mostrar_info_sistema()
    
    # Normalizar rutas
    args.origen = os.path.normpath(args.origen)
    args.destino = os.path.normpath(args.destino)
    
    logger.info(f"Directorio origen: {args.origen}")
    logger.info(f"Directorio destino: {args.destino}")
    
    try:
        # Procesar directorio raíz
        resultado = procesar_directorio_raiz(args.origen, args.destino, args.temporal)
        
        # Calcular tiempo total
        tiempo_total = datetime.now() - tiempo_inicio
        
        if resultado:
            logger.info(f"Tiempo total de ejecución: {tiempo_total}")
            logger.info(f"✅ Proceso completado exitosamente.")
        else:
            logger.error(f"❌ El proceso falló después de {tiempo_total}.")
            logger.info("\nSOLUCIONES A PROBLEMAS COMUNES:")
            logger.info("1. Intenta especificar una ruta relativa en lugar de absoluta:")
            logger.info("   python detector_duplicados.py ./origen ./resultado")
            logger.info("2. Usa la opción --temporal para guardar en un directorio temporal:")
            logger.info("   python detector_duplicados.py /ruta/origen /ruta/destino --temporal")
            logger.info("3. Especifica una ruta completa en tu directorio de usuario:")
            if platform.system() == "Windows":
                home = os.path.expanduser("~")
                logger.info(f"   python detector_duplicados.py /ruta/origen {home}\\Documents")
            else:
                home = os.path.expanduser("~")
                logger.info(f"   python detector_duplicados.py /ruta/origen {home}/Documents")
            logger.info("4. Ejecuta el script con privilegios elevados (administrador/sudo)")
            return 1
        
    except Exception as e:
        logger.error(f"Error no controlado: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())