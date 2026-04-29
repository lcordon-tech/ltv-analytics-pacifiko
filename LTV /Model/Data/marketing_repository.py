# Archivo: Model/Data/marketing_repository.py
# ⚠️ DEPRECADO: Usar CACRepository en su lugar
# Se mantiene para compatibilidad con código existente

from typing import Dict
from .cac_repository import CACRepository


class MarketingDataRepository:
    """
    ⚠️ DEPRECADO: Esta clase ha sido reemplazada por CACRepository.
    Se mantiene por compatibilidad con código existente.
    
    La nueva implementación lee desde CAC_GT.xlsx en la carpeta data_xlsx.
    """
    
    @staticmethod
    def get_cac_mapping() -> Dict[str, float]:
        """
        ⚠️ DEPRECADO: Usar CACRepository.get_cac_mapping() en su lugar.
        
        Retorna diccionario de CAC por cohorte.
        Intenta leer desde CAC_GT.xlsx, si no existe retorna diccionario vacío.
        """
        print("⚠️ [DEPRECADO] MarketingDataRepository.get_cac_mapping()")
        print("   Usar CACRepository.get_cac_mapping() en su lugar")
        
        return CACRepository.get_cac_mapping()
    
    @staticmethod
    def get_monthly_spend() -> Dict[str, float]:
        """Mantiene compatibilidad con código existente."""
        return MarketingDataRepository.get_cac_mapping()