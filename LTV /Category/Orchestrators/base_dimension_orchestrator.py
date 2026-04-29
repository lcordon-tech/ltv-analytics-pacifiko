# Category/Orchestrators/base_dimension_orchestrator.py
"""
Clase base abstracta para todos los orquestadores de dimensión.
VERSIÓN MODIFICADA: Soporta cohortes dinámicos via CohortGrouper.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from collections import defaultdict

from Category.Grouping.category_grouper import CategoryGrouper
from Category.Analytics.conversion_analyzer import CategoryConversionAnalyzer
from Category.Analytics.frequency_analyzer import CategoryFrequencyAnalyzer
from Category.Analytics.time_analyzer import CategoryTimeAnalyzer
from Category.Grouping.cohort_grouper import CohortGrouper
from Category.Analytics.metrics_analyzer import MetricsQualityAnalyzer
from Category.Grouping.entry_grouper import EntryBasedBehaviorGrouper
from Category.Utils.dimension_config import get_dimension_config, DimensionMode
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity


class BaseDimensionOrchestrator(ABC):
    """
    Clase base abstracta para todos los orquestadores de dimensión.
    Soporta cohortes dinámicos vía CohortGrouper.
    """
    
    def __init__(self, customers: List[Any], grouping_mode: str = "entry_based",
                 cohort_config: Optional[CohortConfig] = None):
        """
        Args:
            customers: Lista de objetos Customer
            grouping_mode: "behavioral" o "entry_based"
            cohort_config: Configuración de cohortes. Si es None, usa quarterly default.
        """
        self.customers = customers
        self.grouping_mode = grouping_mode
        self.cohort_config = cohort_config or CohortConfig()
        self._dimension_config = None
        
        # Crear grouper de cohortes dinámico
        self.cohort_grouper = CohortGrouper(self.cohort_config.granularity.value)
        
        print(f"🔧 BaseDimensionOrchestrator.__init__ - grouping_mode = {self.grouping_mode}")
        print(f"   cohort_granularity = {self.cohort_config.granularity.value}")
    
    def _get_config(self) -> dict:
        """Obtiene la configuración de la dimensión (lazy loading)."""
        if self._dimension_config is None:
            mode = self._get_dimension_mode()
            self._dimension_config = get_dimension_config(mode)
        return self._dimension_config
    
    @abstractmethod
    def _get_dimension_mode(self) -> int:
        """Retorna el modo de dimensión (1,2,3,4,5,6)."""
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo para la dimensión configurada.
        """
        config = self._get_config()
        mode_id = config['mode_id']
        group_by_attr = config['group_by_attr']
        output_key = config['output_key']
        
        print(f"⚙️  Iniciando Orquestador para: {output_key}")
        print(f"   Modo de Agrupación: {self.grouping_mode.upper()}")
        print(f"   Granularidad cohortes: {self.cohort_config.granularity.value}")
        print(f"   mode_id: {mode_id}, group_by: {group_by_attr}")
        
        # --- 1. SELECCIÓN DE AGRUPADOR (por dimensión) ---
        if self.grouping_mode == "entry_based":
            print(f"   ✅ Usando EntryBasedBehaviorGrouper con mode={mode_id}")
            grouped_data, stats = EntryBasedBehaviorGrouper.group(
                self.customers, mode=mode_id
            )
        else:
            print(f"   ✅ Usando CategoryGrouper con group_by={group_by_attr}")
            grouped_data, stats = CategoryGrouper.group(
                self.customers, group_by=group_by_attr
            )
        
        # Verificar que hay datos
        if not grouped_data:
            print(f"   ⚠️ No se generaron grupos para {output_key}")
            return {
                "frequency": {"historical": [], "cohorts": {}},
                "time": {"historical": [], "cohorts": {}},
                "conversion": {"historical": [], "cohorts": {}},
                "metadata": {**stats, "attribution_model": self.grouping_mode, 
                            "dimension": output_key, "error": "No groups generated",
                            "cohort_granularity": self.cohort_config.granularity.value}
            }
        
        print(f"   📊 Grupos generados: {len(grouped_data)}")
        
        raw_historical = []
        raw_cohorts = []
        
        # Estructura final
        final_results = {
            "frequency": {"historical": [], "cohorts": {}},
            "time": {"historical": [], "cohorts": {}},
            "conversion": {"historical": [], "cohorts": {}},
            "metadata": {**stats, "attribution_model": self.grouping_mode, 
                        "dimension": output_key,
                        "cohort_granularity": self.cohort_config.granularity.value}
        }
        
        # --- 2. BUCLE DE RECOPILACIÓN ---
        for dim_value in sorted(grouped_data.keys()):
            dim_customers = grouped_data[dim_value]
            aov_dim = MetricsQualityAnalyzer.calculate_aov(dim_customers)
            
            # A. HISTÓRICO (sin desglose por cohorte)
            temp_hist_map = {dim_value: dim_customers}
            h_freq = CategoryFrequencyAnalyzer.analyze(temp_hist_map)[0]
            h_time = CategoryTimeAnalyzer.analyze(temp_hist_map)[0]
            h_conv = CategoryConversionAnalyzer.analyze(temp_hist_map)[0]
            
            hist_record = {
                **h_freq, **h_time, **h_conv,
                output_key: dim_value,
                "AOV_Ref": aov_dim,
                "Tag": "General"
            }
            raw_historical.append(hist_record)
            
            # B. COHORTES (desglose por cohorte dinámico)
            dim_cohorts = self.cohort_grouper.group_instances(dim_customers)
            
            for cohort_id, cohort_customers in dim_cohorts.items():
                aov_coh = MetricsQualityAnalyzer.calculate_aov(cohort_customers)
                label = f"{dim_value} ({cohort_id})"
                
                c_freq = CategoryFrequencyAnalyzer.analyze({label: cohort_customers})[0]
                c_time = CategoryTimeAnalyzer.analyze({label: cohort_customers})[0]
                c_conv = CategoryConversionAnalyzer.analyze({label: cohort_customers})[0]
                
                cohort_record = {
                    **c_freq, **c_time, **c_conv,
                    output_key: dim_value,
                    "Tag": cohort_id,
                    "Cohorte_ID": cohort_id,
                    "AOV_Ref": aov_coh
                }
                raw_cohorts.append(cohort_record)
        
        # --- 3. SCORING ---
        full_universe = raw_historical + raw_cohorts
        scored_universe = MetricsQualityAnalyzer.evaluate_all(full_universe)
        
        print(f"📊 Scoring aplicado a {len(scored_universe)} registros")
        
        if scored_universe:
            if 'Final_Score' in scored_universe[0]:
                print(f"  ✅ Final_Score presente")
            else:
                print(f"  ⚠️ Final_Score NO generado")
        
        # --- 4. REDISTRIBUCIÓN ---
        report_fingerprints = self._get_fingerprints()
        
        core_score_cols = [
            "Final_Score", "Confidence_Score", "Performance_Score",
            "LTV_Score", "Global_Score", "Global_Quality",
            "Sample_Quality", "Sample_Penalty"
        ]
        
        universal_cols = [
            output_key, "Tag", "Cohorte_ID", "AOV_Ref",
            "Quality_Bucket", "Total_Filas_CSV_Auditoria"
        ] + core_score_cols
        
        for record in scored_universe:
            is_general = record.get("Tag") == "General"
            cid = record.get("Cohorte_ID")
            
            for report_type, fingerprint_cols in report_fingerprints.items():
                filtered_record = {}
                
                # Columnas universales
                for col in universal_cols:
                    if col in record:
                        filtered_record[col] = record[col]
                
                # Columnas específicas del reporte
                for col in fingerprint_cols:
                    if col in record:
                        filtered_record[col] = record[col]
                
                # Scores específicos por métrica
                for f_col in fingerprint_cols:
                    score_col = f"{f_col}_Score"
                    label_col = f"{f_col}_Quality"
                    if score_col in record:
                        filtered_record[score_col] = record[score_col]
                    if label_col in record:
                        filtered_record[label_col] = record[label_col]
                
                # Asegurar core scores
                for score_col in core_score_cols:
                    if score_col in record and score_col not in filtered_record:
                        filtered_record[score_col] = record[score_col]
                
                if is_general:
                    final_results[report_type]["historical"].append(filtered_record)
                else:
                    if cid not in final_results[report_type]["cohorts"]:
                        final_results[report_type]["cohorts"][cid] = []
                    final_results[report_type]["cohorts"][cid].append(filtered_record)
        
        # --- 5. LOGS Y CIERRE ---
        MetricsQualityAnalyzer.export_summary_log(scored_universe)
        
        for section in ['frequency', 'time', 'conversion']:
            historical = final_results[section].get('historical', [])
            if historical and 'Final_Score' in historical[0]:
                print(f"  ✅ {section}.historical contiene Final_Score")
        
        return final_results
    
    def _get_fingerprints(self) -> Dict:
        """Columnas específicas por tipo de reporte."""
        return {
            "frequency": [
                "Total_Clientes", "Total_Pedidos", "Pedidos_Promedio",
                "Clientes_2a", "Pct_2da_Compra", "Clientes_3a", "Pct_3ra_Compra",
                "Clientes_4a", "Pct_4ta_Compra"
            ],
            "time": [
                "Muestra_1a2", "Mediana_Dias_1a2", "Promedio_Dias_1a2",
                "Muestra_2a3", "Mediana_Dias_2a3", "Muestra_3a4", "Mediana_Dias_3a4"
            ],
            "conversion": [
                "Total_Clientes", "Clientes_30d", "Pct_Conv_30d",
                "Clientes_60d", "Pct_Conv_60d", "Clientes_90d", "Pct_Conv_90d",
                "Clientes_180d", "Pct_Conv_180d", "Clientes_360d", "Pct_Conv_360d"
            ]
        }