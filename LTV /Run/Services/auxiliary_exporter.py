# ============================================================================
# FILE: Run/Services/auxiliary_exporter.py
# NUEVO - EXPORTACIÓN AUXILIAR DE DATOS
# ============================================================================
"""
Exporta copias de Data_LTV y Results_LTV a carpeta AUXILIAR.
"""

import shutil
import os
from pathlib import Path
from datetime import datetime
from typing import Optional


class AuxiliaryExporter:
    """
    Exporta datos a carpeta AUXILIAR después de cada ejecución.
    Mantiene copias sin mover los originales.
    """
    
    AUXILIAR_DIR_NAME = "AUXILIAR"
    
    @classmethod
    def get_project_root(cls) -> Path:
        """Retorna la raíz del proyecto."""
        return Path(__file__).parent.parent.parent
    
    @classmethod
    def get_auxiliar_folder(cls, country_code: str = None) -> Path:
        """Retorna la carpeta AUXILIAR para el país."""
        root = cls.get_project_root()
        if country_code:
            return root / f"Outputs_{country_code}" / cls.AUXILIAR_DIR_NAME
        return root / "Outputs" / cls.AUXILIAR_DIR_NAME
    
    @classmethod
    def export(cls, data_ltv_path: Path, results_path: Path, 
               country_code: str, timestamp: Optional[str] = None) -> bool:
        """
        Exporta copias de Data_LTV y Results_LTV a carpeta AUXILIAR.
        
        Args:
            data_ltv_path: Ruta a Data_LTV (origen)
            results_path: Ruta a Results_LTV (origen)
            country_code: Código del país (GT, CR)
            timestamp: Timestamp opcional para nombrar la subcarpeta
        
        Returns:
            True si éxito, False si error
        """
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        aux_base = cls.get_auxiliar_folder(country_code)
        aux_folder = aux_base / timestamp
        
        try:
            aux_folder.mkdir(parents=True, exist_ok=True)
            print(f"📦 Exportando datos a AUXILIAR: {aux_folder}")
            
            # Copiar Data_LTV
            if data_ltv_path.exists():
                dest_data = aux_folder / "Data_LTV"
                if dest_data.exists():
                    shutil.rmtree(dest_data)
                shutil.copytree(data_ltv_path, dest_data)
                print(f"   ✅ Data_LTV copiado ({sum(1 for _ in data_ltv_path.rglob('*') if _.is_file())} archivos)")
            else:
                print(f"   ⚠️ Data_LTV no encontrado en {data_ltv_path}")
            
            # Copiar Results_LTV
            if results_path.exists():
                dest_results = aux_folder / "Results_LTV"
                if dest_results.exists():
                    shutil.rmtree(dest_results)
                shutil.copytree(results_path, dest_results)
                print(f"   ✅ Results_LTV copiado ({sum(1 for _ in results_path.rglob('*') if _.is_file())} archivos)")
            else:
                print(f"   ⚠️ Results_LTV no encontrado en {results_path}")
            
            # Crear archivo de metadata
            metadata = aux_folder / "metadata.txt"
            with open(metadata, 'w', encoding='utf-8') as f:
                f.write(f"Export timestamp: {timestamp}\n")
                f.write(f"Country: {country_code}\n")
                f.write(f"Data_LTV source: {data_ltv_path}\n")
                f.write(f"Results_LTV source: {results_path}\n")
            
            print(f"   ✅ Exportación completada")
            return True
            
        except Exception as e:
            print(f"   ❌ Error en exportación: {e}")
            return False
    
    @classmethod
    def cleanup_old_exports(cls, country_code: str, keep_last: int = 10):
        """
        Limpia exportaciones viejas, manteniendo solo las últimas 'keep_last'.
        """
        aux_base = cls.get_auxiliar_folder(country_code)
        if not aux_base.exists():
            return
        
        # Obtener todas las subcarpetas (por timestamp)
        folders = [f for f in aux_base.iterdir() if f.is_dir()]
        folders.sort(key=lambda x: x.name, reverse=True)
        
        # Eliminar las más viejas
        to_delete = folders[keep_last:]
        for folder in to_delete:
            try:
                shutil.rmtree(folder)
                print(f"   🗑️ Exportación antigua eliminada: {folder.name}")
            except Exception as e:
                print(f"   ⚠️ Error eliminando {folder.name}: {e}")
    
    @classmethod
    def export_with_cleanup(cls, data_ltv_path: Path, results_path: Path,
                            country_code: str, keep_last: int = 10) -> bool:
        """
        Exporta y luego limpia exportaciones viejas.
        """
        result = cls.export(data_ltv_path, results_path, country_code)
        if result:
            cls.cleanup_old_exports(country_code, keep_last)
        return result