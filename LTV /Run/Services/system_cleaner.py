# ============================================================================
# FILE: Run/Services/system_cleaner.py
# NUEVO - LIMPIEZA DEL SISTEMA
# ============================================================================
"""
Sistema de limpieza de archivos temporales y caché.
"""

import shutil
import os
from pathlib import Path
from typing import List, Tuple


class SystemCleaner:
    """Limpia archivos temporales y caché del sistema."""
    
    # Carpetas seguras para limpiar
    CLEANABLE_DIRS = ['logs', 'secure', 'cache', '__pycache__']
    
    # Archivos seguros para limpiar
    CLEANABLE_FILES = ['*.pyc', '*.pyo', '*.tmp', '*.log']
    
    # Carpetas que NO se deben tocar
    PROTECTED_DIRS = ['Data_LTV_GT', 'Data_LTV_CR', 'inputs', 'config']
    
    @classmethod
    def get_project_root(cls) -> Path:
        """Retorna la raíz del proyecto."""
        return Path(__file__).parent.parent.parent
    
    @classmethod
    def find_cleanable_items(cls) -> List[Tuple[str, Path]]:
        """
        Encuentra elementos limpiables en el proyecto.
        Retorna lista de (tipo, path) donde tipo es 'dir' o 'file'.
        """
        root = cls.get_project_root()
        items = []
        
        # Buscar carpetas limpiables
        for clean_dir in cls.CLEANABLE_DIRS:
            for path in root.rglob(clean_dir):
                # Verificar que no sea una carpeta protegida
                is_protected = any(protected in str(path) for protected in cls.PROTECTED_DIRS)
                if not is_protected and path.is_dir():
                    items.append(('dir', path))
        
        # Buscar archivos limpiables (solo en directorios seguros)
        for pattern in cls.CLEANABLE_FILES:
            for path in root.rglob(pattern):
                # Solo limpiar si está en una carpeta limpiable
                parent = path.parent
                is_in_cleanable = any(clean_dir in str(parent) for clean_dir in cls.CLEANABLE_DIRS)
                if is_in_cleanable and path.is_file():
                    items.append(('file', path))
        
        return items
    
    @classmethod
    def get_size_summary(cls) -> Tuple[int, str]:
        """Calcula el tamaño total de elementos limpiables."""
        items = cls.find_cleanable_items()
        total_size = 0
        
        for typ, path in items:
            if typ == 'dir':
                for root, dirs, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            total_size += os.path.getsize(file_path)
                        except OSError:
                            pass
            else:  # file
                try:
                    total_size += path.stat().st_size
                except OSError:
                    pass
        
        # Formatear tamaño
        if total_size < 1024:
            size_str = f"{total_size} B"
        elif total_size < 1024 * 1024:
            size_str = f"{total_size / 1024:.2f} KB"
        else:
            size_str = f"{total_size / (1024 * 1024):.2f} MB"
        
        return total_size, size_str
    
    @classmethod
    def preview_cleanable(cls) -> List[str]:
        """Retorna lista de rutas a limpiar (para previsualización)."""
        items = cls.find_cleanable_items()
        return [str(path) for typ, path in items]
    
    @classmethod
    def clean(cls, dry_run: bool = False) -> Tuple[int, int, List[str]]:
        """
        Limpia archivos temporales.
        
        Args:
            dry_run: Si True, solo simula sin borrar.
        
        Returns:
            (deleted_count, freed_bytes, errors)
        """
        items = cls.find_cleanable_items()
        deleted = 0
        freed = 0
        errors = []
        
        for typ, path in items:
            try:
                if typ == 'dir':
                    # Calcular tamaño antes de borrar
                    size = 0
                    for root, dirs, files in os.walk(path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            try:
                                size += os.path.getsize(file_path)
                            except OSError:
                                pass
                    
                    if not dry_run:
                        shutil.rmtree(path, ignore_errors=True)
                        # Recrear directorio vacío (opcional)
                        path.mkdir(parents=True, exist_ok=True)
                    
                    freed += size
                    deleted += 1
                    
                else:  # file
                    size = path.stat().st_size if path.exists() else 0
                    if not dry_run and path.exists():
                        path.unlink()
                    freed += size
                    deleted += 1
                    
            except Exception as e:
                errors.append(f"Error limpiando {path}: {e}")
        
        return deleted, freed, errors
    
    @classmethod
    def clean_interactive(cls):
        """Modo interactivo para limpieza."""
        print("\n" + "=" * 60)
        print("   LIMPIEZA DEL SISTEMA".center(60))
        print("=" * 60)
        
        # Verificar elementos limpiables
        items = cls.find_cleanable_items()
        if not items:
            print("✅ No se encontraron elementos limpiables.")
            return True
        
        total_size, size_str = cls.get_size_summary()
        
        print(f"\n📊 Se encontraron {len(items)} elementos para limpiar:")
        print(f"   Espacio ocupado: {size_str}")
        
        print("\n📋 Vista previa:")
        preview = cls.preview_cleanable()[:15]
        for p in preview:
            # Mostrar solo nombre de carpeta/archivo, no ruta completa
            name = Path(p).name
            parent = Path(p).parent.name
            print(f"   📁 {parent}/{name}")
        
        if len(items) > 15:
            print(f"   ... y {len(items) - 15} más")
        
        print("\n⚠️ Esta acción eliminará:")
        print("   • Logs del sistema")
        print("   • Archivos de caché")
        print("   • Archivos temporales")
        print("   • Carpetas secure/backups (backups viejos)")
        print("\n✅ NO se eliminarán:")
        print("   • Credenciales")
        print("   • Datos de entrada (inputs)")
        print("   • Resultados de análisis")
        
        confirm = input("\n👉 ¿Eliminar estos archivos? (s/n): ").strip().lower()
        if confirm not in ['s', 'si', 'sí', 'yes', 'y']:
            print("❌ Limpieza cancelada.")
            return False
        
        print("\n⏳ Limpiando...")
        deleted, freed, errors = cls.clean(dry_run=False)
        
        if errors:
            for err in errors:
                print(f"   ⚠️ {err}")
        
        # Formatear tamaño liberado
        if freed < 1024:
            freed_str = f"{freed} B"
        elif freed < 1024 * 1024:
            freed_str = f"{freed / 1024:.2f} KB"
        else:
            freed_str = f"{freed / (1024 * 1024):.2f} MB"
        
        print(f"\n✅ Limpieza completada:")
        print(f"   • Elementos eliminados: {deleted}")
        print(f"   • Espacio liberado: {freed_str}")
        
        return True