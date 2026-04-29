"""
Repositorio para gestión de CAC (Costo de Adquisición de Clientes).
Lee desde archivo Excel CAC.xlsx con hojas por país.
VERSIÓN MULTI-PAÍS.
"""

import os
import pandas as pd
from typing import Dict, Optional
from pathlib import Path

from Model.Data.cac_adapter import CACAdapter
from Category.Cohort.cohort_config import TimeGranularity


class CACRepository:
    """
    Gestiona el Costo de Adquisición (CAC) por cohorte.
    
    Fuente de datos: CAC.xlsx (Excel con hojas por país)
    - Cada hoja debe tener columnas 'cohort' y 'cac'
    - cohort: identificador de cohorte (ej: '2024-Q1', '2024-01', '2024')
    - cac: valor numérico del CAC en USD
    
    AHORA: Soporta multi-país mediante CountryConfig.
    """
    
    _cache: Dict[str, Dict[str, float]] = {}
    
    @classmethod
    def get_cac_mapping(cls, country_config, cac_path: Optional[str] = None, 
                        granularity: str = "quarterly",
                        use_cache: bool = True,
                        transform: bool = True) -> Dict[str, float]:
        """
        Retorna diccionario de CAC por cohorte para un país específico.
        
        Args:
            country_config: Configuración del país (con cac_sheet)
            cac_path: Ruta al archivo CAC.xlsx
            granularity: Granularidad destino
            use_cache: Si True, usa cache en memoria
            transform: Si True, transforma desde trimestral a granularidad destino
        
        Returns:
            Dict[str, float]: {cohort_id: cac_value}
        """
        if country_config is None:
            return cls._get_legacy_cac_mapping(cac_path, granularity, use_cache, transform)
        
        sheet_name = country_config.cac_sheet
        cache_key = f"{country_config.code}_{granularity}_{transform}"
        
        if use_cache and cache_key in cls._cache:
            print(f"✅ CAC desde cache para {country_config.code}: {len(cls._cache[cache_key])} cohortes")
            return cls._cache[cache_key].copy()
        
        if cac_path is None:
            cac_path = cls._find_cac_file()
        
        base_map = {}
        if cac_path and os.path.exists(cac_path):
            base_map = cls._read_cac_from_excel(cac_path, sheet_name)
        
        if not base_map:
            print(f"⚠️ No se encontró CAC para {country_config.name} (hoja: {sheet_name})")
            return {}
        
        if not transform or granularity == "quarterly":
            result = base_map
        else:
            target = TimeGranularity.from_string(granularity)
            result = CACAdapter.transform(base_map, target)
            print(f"🔄 CAC transformado: quarterly → {granularity}")
        
        if use_cache:
            cls._cache[cache_key] = result.copy()
        
        print(f"✅ CAC cargado para {country_config.name}: {len(result)} cohortes")
        
        sample = list(result.items())[:3]
        for cohort, cac_val in sample:
            print(f"   {cohort}: ${cac_val:.2f}")
        if len(result) > 3:
            print(f"   ... y {len(result) - 3} más")
        
        return result
    
    @classmethod
    def _get_legacy_cac_mapping(cls, cac_path: Optional[str] = None, 
                                 granularity: str = "quarterly",
                                 use_cache: bool = True,
                                 transform: bool = True) -> Dict[str, float]:
        """Modo legacy para compatibilidad (busca hoja 'CAC' o archivo CAC_GT.xlsx)."""
        cache_key = f"legacy_{granularity}_{transform}"
        
        if use_cache and cache_key in cls._cache:
            return cls._cache[cache_key].copy()
        
        if cac_path is None:
            cac_path = cls._find_cac_file()
        
        base_map = {}
        if cac_path and os.path.exists(cac_path):
            # Intentar leer hoja 'CAC' primero
            base_map = cls._read_cac_from_excel(cac_path, "CAC")
            if not base_map:
                # Fallback a archivo CAC_GT.xlsx
                base_map = cls._read_cac_from_excel_legacy(cac_path)
        
        if not transform or granularity == "quarterly":
            result = base_map
        else:
            target = TimeGranularity.from_string(granularity)
            result = CACAdapter.transform(base_map, target)
        
        if use_cache:
            cls._cache[cache_key] = result.copy()
        
        return result
    
    @classmethod
    def _read_cac_from_excel(cls, cac_path: str, sheet_name: str) -> Dict[str, float]:
        """Lee el archivo CAC.xlsx desde una hoja específica."""
        try:
            df = pd.read_excel(cac_path, sheet_name=sheet_name)
            df.columns = [str(col).strip().lower() for col in df.columns]
            
            if 'cohort' not in df.columns or 'cac' not in df.columns:
                print(f"⚠️ Hoja '{sheet_name}': debe tener columnas 'cohort' y 'cac'")
                return {}
            
            df['cohort'] = df['cohort'].astype(str).str.strip()
            df['cac'] = pd.to_numeric(df['cac'], errors='coerce')
            df = df.dropna(subset=['cac'])
            
            return {row['cohort']: row['cac'] for _, row in df.iterrows()}
        except Exception as e:
            print(f"⚠️ Error leyendo hoja '{sheet_name}' de {cac_path}: {e}")
            return {}
    
    @classmethod
    def _read_cac_from_excel_legacy(cls, cac_path: str) -> Dict[str, float]:
        """Modo legacy: lee archivo CAC_GT.xlsx."""
        try:
            df = pd.read_excel(cac_path)
            df.columns = [str(col).strip().lower() for col in df.columns]
            
            if 'cohort' not in df.columns or 'cac' not in df.columns:
                return {}
            
            df['cohort'] = df['cohort'].astype(str).str.strip()
            df['cac'] = pd.to_numeric(df['cac'], errors='coerce')
            df = df.dropna(subset=['cac'])
            
            return {row['cohort']: row['cac'] for _, row in df.iterrows()}
        except Exception:
            return {}
    
    @classmethod
    def _find_cac_file(cls) -> Optional[str]:
        """Busca el archivo CAC.xlsx o CAC_GT.xlsx en rutas comunes."""
        env_path = os.environ.get("LTV_CAC_PATH")
        if env_path and os.path.exists(env_path):
            return env_path
        
        input_dir = os.environ.get("LTV_INPUT_DIR")
        if input_dir:
            candidate = os.path.join(input_dir, "CAC.xlsx")
            if os.path.exists(candidate):
                return candidate
            candidate = os.path.join(input_dir, "CAC_GT.xlsx")
            if os.path.exists(candidate):
                return candidate
        
        if os.path.exists("CAC.xlsx"):
            return "CAC.xlsx"
        if os.path.exists("CAC_GT.xlsx"):
            return "CAC_GT.xlsx"
        
        return None
    
    @classmethod
    def clear_cache(cls):
        cls._cache.clear()
        print("✅ Cache de CAC limpiado")