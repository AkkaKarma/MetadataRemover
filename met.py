#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
import argparse
from PIL import Image
from PIL.ExifTags import TAGS
import requests
import schedule
import shutil
import json
import tempfile
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from fractions import Fraction

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("metadata_cleaner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def custom_json_serializer(obj):
    """Función personalizada para serializar objetos no serializables por defecto en JSON"""
    if isinstance(obj, (int, float, str)):
        return obj
    elif isinstance(obj, dict):
        return {k: custom_json_serializer(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [custom_json_serializer(i) for i in obj]
    elif isinstance(obj, tuple):
        return tuple(custom_json_serializer(i) for i in obj)
    elif isinstance(obj, Fraction):
        return float(obj)
    else:
        return str(obj)

class MetadataCleaner:
    def __init__(self, folder_path, telegram_token, chat_id, interval=60):
        """
        Inicializa el limpiador de metadatos
        
        Args:
            folder_path (str): Ruta a la carpeta a monitorear
            telegram_token (str): Token del bot de Telegram
            chat_id (str): ID del chat donde enviar los mensajes
            interval (int): Intervalo de escaneo en segundos cuando no se usa watchdog
        """
        self.folder_path = os.path.abspath(folder_path)
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        self.interval = interval
        self.telegram_api_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        
        # Crear carpeta temporal si no existe
        self.temp_folder = os.path.join(self.folder_path, "_temp_metadata_cleaner")
        os.makedirs(self.temp_folder, exist_ok=True)
        
        # Verificar las herramientas externas necesarias
        self._check_required_tools()
        
        logger.info(f"Iniciando monitoreo en: {self.folder_path}")
        self.send_telegram_message("🟢 Monitor de metadatos iniciado")

    def _check_required_tools(self):
        """Verifica si las herramientas externas necesarias están instaladas"""
        tools_status = {}
        
        # Verificar exiftool (para múltiples formatos)
        try:
            subprocess.run(['exiftool', '-ver'], capture_output=True, text=True, check=False)
            tools_status['exiftool'] = True
        except (FileNotFoundError, subprocess.SubprocessError):
            tools_status['exiftool'] = False
            logger.warning("ExifTool no encontrado. La limpieza de metadatos será limitada.")
        
        # Verificar qpdf (para PDFs)
        try:
            subprocess.run(['qpdf', '--version'], capture_output=True, text=True, check=False)
            tools_status['qpdf'] = True
        except (FileNotFoundError, subprocess.SubprocessError):
            tools_status['qpdf'] = False
            logger.warning("QPDF no encontrado. La limpieza de metadatos de PDF será limitada.")
        
        self.available_tools = tools_status

    def send_telegram_message(self, message):
        """Envía mensaje a Telegram"""
        try:
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(self.telegram_api_url, data=payload)
            if response.status_code != 200:
                logger.error(f"Error al enviar mensaje a Telegram: {response.text}")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error en Telegram: {str(e)}")
            return False

    def get_metadata_with_exiftool(self, file_path):
        """
        Obtiene los metadatos usando ExifTool
        
        Args:
            file_path (str): Ruta al archivo
            
        Returns:
            dict: Diccionario con los metadatos
        """
        if not self.available_tools.get('exiftool', False):
            return {"error": "ExifTool no disponible"}
            
        try:
            result = subprocess.run(
                ['exiftool', '-json', file_path], 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            if result.returncode != 0:
                logger.error(f"Error al obtener metadatos con ExifTool: {result.stderr}")
                return {}
                
            metadata = json.loads(result.stdout)
            return metadata[0] if metadata else {}
        except Exception as e:
            logger.error(f"Error al usar ExifTool: {str(e)}")
            return {}

    def has_metadata(self, file_path):
        """
        Comprueba si un archivo tiene metadatos
        
        Args:
            file_path (str): Ruta al archivo
        
        Returns:
            tuple: (bool tiene_metadatos, dict metadatos)
        """
        try:
            extension = os.path.splitext(file_path)[1].lower()
            
            # Usar ExifTool si está disponible para todos los tipos de archivos
            if self.available_tools.get('exiftool', False):
                metadata = self.get_metadata_with_exiftool(file_path)
                # Filtrar metadatos básicos que no son sensibles
                filtered_metadata = {k: v for k, v in metadata.items() 
                                   if k not in ['FileSize', 'FileName', 'FileType', 'MIMEType', 'ExifToolVersion']}
                return bool(filtered_metadata), filtered_metadata
            
            # Métodos alternativos si ExifTool no está disponible
            # Imágenes (JPG, PNG, TIFF, etc.)
            if extension in ['.jpg', '.jpeg', '.png', '.tiff', '.tif', '.webp']:
                try:
                    with Image.open(file_path) as img:
                        exif_data = {}
                        if hasattr(img, '_getexif') and img._getexif():
                            for tag_id, value in img._getexif().items():
                                tag = TAGS.get(tag_id, tag_id)
                                exif_data[tag] = value
                        
                        # Comprobar otros metadatos (IPTC, XMP)
                        if hasattr(img, 'info') and img.info:
                            for key, value in img.info.items():
                                if key not in ['dpi', 'jfif', 'jfif_version', 'jfif_unit', 'jfif_density']:
                                    exif_data[key] = str(value)
                        
                        return bool(exif_data), exif_data
                except Exception as e:
                    logger.error(f"Error al leer metadatos de imagen {file_path}: {str(e)}")
                    return False, {}
            
            # Documentos PDF, DOCX, etc. - Simplemente indicamos que podrían tener metadatos
            elif extension in ['.pdf', '.docx', '.doc', '.pptx', '.xlsx', '.xls']:
                return True, {"warning": "Archivo potencialmente con metadatos"}
            
            # Archivos multimedia
            elif extension in ['.mp3', '.mp4', '.avi', '.mov', '.wav']:
                return True, {"warning": "Archivo multimedia potencialmente con metadatos"}
                
            return False, {}
        except Exception as e:
            logger.error(f"Error al verificar metadatos de {file_path}: {str(e)}")
            return False, {}

    def clean_metadata(self, file_path):
        """
        Limpia los metadatos de un archivo
        
        Args:
            file_path (str): Ruta al archivo
        
        Returns:
            bool: True si se limpiaron los metadatos, False en caso contrario
        """
        try:
            extension = os.path.splitext(file_path)[1].lower()
            filename = os.path.basename(file_path)
            temp_file = os.path.join(self.temp_folder, filename)
            
            # Usar ExifTool si está disponible (método más efectivo para la mayoría de archivos)
            if self.available_tools.get('exiftool', False):
                try:
                    # Crear una copia limpia con todos los metadatos eliminados
                    result = subprocess.run(
                        ['exiftool', '-all=', '-o', temp_file, file_path],
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    
                    if result.returncode != 0:
                        logger.error(f"Error al limpiar metadatos con ExifTool: {result.stderr}")
                        # Si falla con ExifTool, intentamos con métodos específicos
                    else:
                        # Reemplazar el archivo original
                        shutil.move(temp_file, file_path)
                        return True
                except Exception as e:
                    logger.error(f"Error con ExifTool: {str(e)}")
                    # Continuar con métodos alternativos
            
            # Métodos específicos por tipo de archivo si ExifTool falló o no está disponible
            
            # PDFs
            if extension == '.pdf':
                if self.available_tools.get('qpdf', False):
                    try:
                        # Usar QPDF para crear una versión limpia
                        result = subprocess.run(
                            ['qpdf', '--linearize', '--replace-input', file_path],
                            capture_output=True,
                            text=True,
                            check=False
                        )
                        
                        if result.returncode == 0:
                            return True
                        else:
                            logger.error(f"Error al limpiar PDF con QPDF: {result.stderr}")
                    except Exception as e:
                        logger.error(f"Error con QPDF: {str(e)}")
                        
                return False  # No se pudo limpiar
                
            # Imágenes
            elif extension in ['.jpg', '.jpeg', '.png', '.tiff', '.tif', '.webp']:
                try:
                    with Image.open(file_path) as img:
                        # Crear nueva imagen sin metadatos
                        img_without_exif = Image.new(img.mode, img.size)
                        img_without_exif.putdata(list(img.getdata()))
                        img_without_exif.save(temp_file)
                    
                    # Reemplazar el archivo original
                    shutil.move(temp_file, file_path)
                    return True
                except Exception as e:
                    logger.error(f"Error al limpiar metadatos de imagen {file_path}: {str(e)}")
                    return False
            
            # Documentos Office (DOCX, DOC, etc.)
            elif extension in ['.docx', '.xlsx', '.pptx']:
                logger.warning(f"Limpieza manual para archivos Office {extension}")
                return False
                
            # Archivos multimedia
            elif extension in ['.mp3', '.mp4', '.avi', '.mov', '.wav']:
                logger.warning(f"Limpieza de metadatos para archivos multimedia {extension} no implementada")
                return False
                
            logger.warning(f"No se implementó limpieza para el tipo de archivo: {extension}")
            return False
                
        except Exception as e:
            logger.error(f"Error al limpiar metadatos de {file_path}: {str(e)}")
            return False

    def scan_folder(self):
        """Escanea la carpeta en busca de archivos con metadatos"""
        logger.info(f"Escaneando carpeta: {self.folder_path}")
        files_with_metadata = 0

        for root, _, files in os.walk(self.folder_path):
            # Saltar la carpeta temporal
            if self.temp_folder in root:
                continue
                
            for file in files:
                file_path = os.path.join(root, file)
                has_meta, metadata = self.has_metadata(file_path)
                
                if has_meta:
                    files_with_metadata += 1
                    relative_path = os.path.relpath(file_path, self.folder_path)
                    metadata_str = json.dumps(custom_json_serializer(metadata), indent=2) if isinstance(metadata, dict) else str(metadata)
                    
                    # Limitar longitud del mensaje
                    if len(metadata_str) > 500:
                        metadata_str = metadata_str[:500] + "... [truncado]"
                    
                    message = (
                        f"🔍 <b>Archivo con metadatos detectado:</b>\n"
                        f"📁 {relative_path}\n"
                        f"📊 <pre>{metadata_str}</pre>"
                    )
                    
                    self.send_telegram_message(message)
                    logger.info(f"Encontrados metadatos en: {file_path}")
                    
                    # Limpiar metadatos
                    if self.clean_metadata(file_path):
                        self.send_telegram_message(f"✅ Metadatos eliminados de: {relative_path}")
                        logger.info(f"Metadatos eliminados de: {file_path}")
                    else:
                        self.send_telegram_message(f"❌ Error al eliminar metadatos de: {relative_path}")
                        logger.error(f"Error al eliminar metadatos de: {file_path}")
        
        if files_with_metadata == 0:
            logger.info("No se encontraron archivos con metadatos")
        else:
            logger.info(f"Se encontraron {files_with_metadata} archivos con metadatos")
        
        return files_with_metadata

    def run_continuous(self):
        """Ejecuta el escáner en modo continuo con schedule"""
        self.scan_folder()  # Escaneo inicial
        
        # Programar escaneos periódicos
        schedule.every(self.interval).seconds.do(self.scan_folder)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Deteniendo monitor de metadatos...")
            self.send_telegram_message("🔴 Monitor de metadatos detenido")
            # Limpiar carpeta temporal
            shutil.rmtree(self.temp_folder, ignore_errors=True)

    def run_watchdog(self):
        """Ejecuta el monitor usando watchdog para eventos de sistema de archivos"""
        # Definir manejador de eventos
        event_handler = MetadataEventHandler(self)
        
        # Configurar observador
        observer = Observer()
        observer.schedule(event_handler, self.folder_path, recursive=True)
        
        # Iniciar observador
        observer.start()
        
        # Escaneo inicial
        self.scan_folder()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            logger.info("Deteniendo monitor de metadatos...")
            self.send_telegram_message("🔴 Monitor de metadatos detenido")
            # Limpiar carpeta temporal
            shutil.rmtree(self.temp_folder, ignore_errors=True)
        
        observer.join()


class MetadataEventHandler(FileSystemEventHandler):
    """Manejador de eventos del sistema de archivos para watchdog"""
    
    def __init__(self, metadata_cleaner):
        self.cleaner = metadata_cleaner
        self.temp_folder = metadata_cleaner.temp_folder
        # Agregar un conjunto para rastrear archivos procesados recientemente para evitar duplicados
        self.recently_processed = set()

    def on_created(self, event):
        """Evento cuando se crea un nuevo archivo"""
        if event.is_directory or self.temp_folder in event.src_path:
            return
            
        # Evitar procesamiento duplicado
        if event.src_path in self.recently_processed:
            return
            
        self.recently_processed.add(event.src_path)
        # Eliminar del conjunto después de 5 segundos para evitar duplicados pero permitir cambios reales
        time.sleep(1)  # Esperar un momento para que el archivo termine de escribirse
        
        try:
            if os.path.exists(event.src_path):
                logger.info(f"Nuevo archivo detectado: {event.src_path}")
                has_meta, metadata = self.cleaner.has_metadata(event.src_path)
                
                if has_meta:
                    relative_path = os.path.relpath(event.src_path, self.cleaner.folder_path)
                    # Usar el serializador personalizado
                    metadata_str = json.dumps(custom_json_serializer(metadata), indent=2) if isinstance(metadata, dict) else str(metadata)
                    
                    # Limitar longitud
                    if len(metadata_str) > 500:
                        metadata_str = metadata_str[:500] + "... [truncado]"
                    
                    message = (
                        f"🆕 <b>Nuevo archivo con metadatos:</b>\n"
                        f"📁 {relative_path}\n"
                        f"📊 <pre>{metadata_str}</pre>"
                    )
                    
                    self.cleaner.send_telegram_message(message)
                    
                    # Limpiar metadatos
                    if self.cleaner.clean_metadata(event.src_path):
                        self.cleaner.send_telegram_message(f"✅ Metadatos eliminados de: {relative_path}")
                        logger.info(f"Metadatos eliminados de: {event.src_path}")
                    else:
                        self.cleaner.send_telegram_message(f"❌ Error al eliminar metadatos de: {relative_path}")
                        logger.error(f"Error al eliminar metadatos de: {event.src_path}")
        except Exception as e:
            logger.error(f"Error al procesar archivo creado {event.src_path}: {str(e)}")
        finally:
            # Limpiar de la lista de procesados después de un tiempo
            time.sleep(4)
            if event.src_path in self.recently_processed:
                self.recently_processed.remove(event.src_path)

    def on_modified(self, event):
        """Evento cuando se modifica un archivo"""
        if event.is_directory or self.temp_folder in event.src_path:
            return
            
        # Evitar procesamiento duplicado
        if event.src_path in self.recently_processed:
            return
            
        self.recently_processed.add(event.src_path)
        
        try:
            # Procesar solo algunas modificaciones para evitar duplicados
            # (los sistemas de archivos a veces generan múltiples eventos)
            file_ext = os.path.splitext(event.src_path)[1].lower()
            relevant_extensions = ('.jpg', '.jpeg', '.png', '.pdf', '.docx', '.doc', 
                                  '.pptx', '.xlsx', '.xls', '.mp3', '.mp4', '.avi')
            
            if os.path.exists(event.src_path) and file_ext in relevant_extensions:
                logger.info(f"Archivo modificado: {event.src_path}")
                has_meta, metadata = self.cleaner.has_metadata(event.src_path)
                
                if has_meta:
                    relative_path = os.path.relpath(event.src_path, self.cleaner.folder_path)
                    # Usar el serializador personalizado
                    metadata_str = json.dumps(custom_json_serializer(metadata), indent=2) if isinstance(metadata, dict) else str(metadata)
                    
                    # Limitar longitud
                    if len(metadata_str) > 500:
                        metadata_str = metadata_str[:500] + "... [truncado]"
                    
                    message = (
                        f"🔄 <b>Archivo modificado con metadatos:</b>\n"
                        f"📁 {relative_path}\n"
                        f"📊 <pre>{metadata_str}</pre>"
                    )
                    
                    self.cleaner.send_telegram_message(message)
                    
                    # Limpiar metadatos
                    if self.cleaner.clean_metadata(event.src_path):
                        self.cleaner.send_telegram_message(f"✅ Metadatos eliminados de: {relative_path}")
                        logger.info(f"Metadatos eliminados de: {event.src_path}")
                    else:
                        self.cleaner.send_telegram_message(f"❌ Error al eliminar metadatos de: {relative_path}")
                        logger.error(f"Error al eliminar metadatos de: {event.src_path}")
        except Exception as e:
            logger.error(f"Error al procesar archivo modificado {event.src_path}: {str(e)}")
        finally:
            # Limpiar de la lista de procesados después de un tiempo
            time.sleep(4)
            if event.src_path in self.recently_processed:
                self.recently_processed.remove(event.src_path)


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Monitor y limpiador de metadatos de archivos')
    parser.add_argument('folder', help='Carpeta a monitorear')
    parser.add_argument('--token', required=True, help='Token del bot de Telegram')
    parser.add_argument('--chat', required=True, help='ID del chat de Telegram')
    parser.add_argument('--interval', type=int, default=60, help='Intervalo de escaneo en segundos (modo schedule)')
    parser.add_argument('--mode', choices=['schedule', 'watchdog'], default='watchdog', 
                        help='Modo de funcionamiento: schedule (escaneo periódico) o watchdog (monitoreo de eventos)')
    
    args = parser.parse_args()
    
    # Validar que la carpeta existe
    if not os.path.isdir(args.folder):
        logger.error(f"La carpeta {args.folder} no existe")
        return
    
    # Crear instancia del limpiador
    cleaner = MetadataCleaner(
        folder_path=args.folder,
        telegram_token=args.token,
        chat_id=args.chat,
        interval=args.interval
    )
    
    # Ejecutar en el modo seleccionado
    if args.mode == 'schedule':
        logger.info(f"Iniciando en modo schedule (intervalo: {args.interval} segundos)")
        cleaner.run_continuous()
    else:
        logger.info("Iniciando en modo watchdog")
        cleaner.run_watchdog()


if __name__ == "__main__":
    main()