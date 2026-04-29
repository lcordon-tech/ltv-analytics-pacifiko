"""
Utilidades para fallback dinámico de cohortes.
Elimina hardcodeos de retention y cogs.
"""

from typing import Dict, Optional


def get_closest_cohort_value(cohort: str, available_map: Dict[str, float]) -> float:
    """
    Busca el valor más cercano por proximidad de cohorte.
    
    Args:
        cohort: String de cohorte (ej: 'Q10', '2024-03', '2024-W12')
        available_map: Diccionario {cohorte: valor}
    
    Returns:
        Valor más cercano encontrado, o 0.0 si no hay mapa disponible
    """
    if not available_map:
        return 0.0
    
    # Si existe exactamente, usarlo
    if cohort in available_map:
        return available_map[cohort]
    
    # Extraer número de cohorte para comparación numérica
    cohort_num = _extract_cohort_number(cohort)
    if cohort_num is None:
        # Si no se puede extraer número, buscar por orden alfabético
        return _get_closest_by_string(cohort, available_map)
    
    # Extraer números de todas las cohortes disponibles
    available_nums = []
    for k, v in available_map.items():
        num = _extract_cohort_number(k)
        if num is not None:
            available_nums.append((num, k, v))
    
    if not available_nums:
        return 0.0
    
    # Ordenar y buscar el más cercano
    available_nums.sort(key=lambda x: x[0])
    
    # Búsqueda binaria manual del más cercano
    closest_num = min(available_nums, key=lambda x: abs(x[0] - cohort_num))
    
    print(f"   🔄 Fallback: {cohort} → {closest_num[1]} (distancia: {abs(closest_num[0] - cohort_num)})")
    return closest_num[2]


def _extract_cohort_number(cohort: str) -> Optional[int]:
    """
    Extrae número numérico de una cohorte para comparación.
    
    Soporta:
    - Q1, Q2, Q10 → 1, 2, 10
    - 2024-03 → 202403
    - 2024-W12 → 202412
    """
    import re
    
    cohort = str(cohort).strip().upper()
    
    # Formato Q* (quarterly)
    if cohort.startswith('Q'):
        try:
            return int(cohort[1:])
        except ValueError:
            pass
    
    # Formato YYYY-MM (monthly)
    match = re.match(r'(\d{4})-(\d{2})', cohort)
    if match:
        return int(match.group(1) + match.group(2))
    
    # Formato YYYY-Wxx (weekly)
    match = re.match(r'(\d{4})-W(\d{2})', cohort)
    if match:
        return int(match.group(1) + match.group(2))
    
    # Formato YYYY-H1 / YYYY-H2 (semiannual)
    match = re.match(r'(\d{4})-H([12])', cohort)
    if match:
        return int(match.group(1) + match.group(2))
    
    # Formato YYYY (yearly)
    if cohort.isdigit() and len(cohort) == 4:
        return int(cohort) * 100
    
    return None


def _get_closest_by_string(cohort: str, available_map: Dict[str, float]) -> float:
    """Fallback por orden alfabético cuando no se puede extraer número."""
    available_sorted = sorted(available_map.keys())
    
    # Buscar el inmediatamente anterior o siguiente
    for i, key in enumerate(available_sorted):
        if key > cohort:
            if i == 0:
                return available_map[key]
            # Comparar distancia con anterior
            prev_dist = abs(len(key) - len(available_sorted[i-1]))
            curr_dist = abs(len(key) - len(cohort))
            if curr_dist < prev_dist:
                return available_map[key]
            return available_map[available_sorted[i-1]]
    
    # Si no hay mayor, devolver el último
    return available_map[available_sorted[-1]] if available_sorted else 0.0


def log_fallback_stats(cohorts_with_fallback: dict, total_cohorts: int):
    """Genera estadísticas de uso de fallback."""
    if not cohorts_with_fallback:
        print("✅ Todas las cohortes usaron valores directos (sin fallback)")
        return
    
    print(f"\n📊 ESTADÍSTICAS DE FALLBACK:")
    print(f"   Cohortes con fallback: {len(cohorts_with_fallback)} de {total_cohorts}")
    print(f"   Detalle de sustituciones:")
    for original, used in list(cohorts_with_fallback.items())[:10]:
        print(f"      • {original} → {used}")
    if len(cohorts_with_fallback) > 10:
        print(f"      ... y {len(cohorts_with_fallback) - 10} más")