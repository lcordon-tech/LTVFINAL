# Category/Reporting/global_exporter.py
"""
Orquestador Maestro del Pipeline LTV.
VERSIÓN MODIFICADA: Soporta cohortes dinámicos.
"""

import os
import importlib
from datetime import datetime
from typing import List, Optional

import pandas as pd

from Category.Utils.dimension_config import DimensionMode, get_all_dimension_modes, get_dimension_config
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity


class GlobalLTVOrchestrator:
    """
    Orquestador Maestro que ejecuta el pipeline para múltiples dimensiones.
    Soporta cohortes dinámicos vía cohort_config.
    """
    
    def __init__(self, customers: list, ue_results: dict = None,
                 grouping_mode: str = "entry_based", output_dir: str = "Final_Reports",
                 dimensions: Optional[List[int]] = None,
                 cohort_config: Optional[CohortConfig] = None):
        """
        Args:
            customers: Lista maestra de objetos Customer.
            ue_results: Diccionario con el CAC por cohorte.
            grouping_mode: "behavioral" o "entry_based".
            output_dir: Directorio raíz de reportes.
            dimensions: Lista de modos de dimensión a procesar.
                        Si es None, procesa todas (1,2,3,4,5,6).
            cohort_config: Configuración de cohortes. Si es None, usa quarterly default.
        """
        self.customers = customers
        self.ue_results = ue_results or {}
        self.grouping_mode = grouping_mode
        self.cohort_config = cohort_config or CohortConfig()
        
        print(f"🔧 GlobalLTVOrchestrator.__init__ - grouping_mode = {self.grouping_mode}")
        print(f"   cohort_granularity = {self.cohort_config.granularity.value}")
        
        # Dimensiones a procesar
        if dimensions is None:
            self.dimensions = get_all_dimension_modes()
        else:
            self.dimensions = dimensions
        
        # Crear directorio base
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        gran_suffix = self.cohort_config.granularity.value[:3]  # qua, mon, wee, etc.
        folder_name = f"Batch_{self.timestamp}_{self.grouping_mode}_{gran_suffix}"
        self.output_dir = os.path.join(output_dir, folder_name)
        
        os.environ["LTV_OUTPUT_DIR"] = self.output_dir
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    # ==========================================================================
    # MÉTODO PRINCIPAL
    # ==========================================================================
    def run_pipeline_completo(self):
        """
        Ejecuta toda la suite de análisis para todas las dimensiones configuradas.
        Método principal llamado desde mainMD.py
        """
        print("=" * 90)
        print(f"🚀 PIPELINE GLOBAL LTV + UE".center(90))
        print(f"MODO: {self.grouping_mode.upper()} | GRANULARIDAD: {self.cohort_config.granularity.value}".center(90))
        print(f"BATCH: {self.timestamp}".center(90))
        print(f"DIMENSIONES A PROCESAR: {self.dimensions}".center(90))
        print("=" * 90)
        
        # Mostrar resumen de cohortes
        self._print_cohort_summary()
        
        for mode in self.dimensions:
            try:
                self._process_dimension(mode)
            except Exception as e:
                print(f"❌ Error procesando dimensión {mode}: {e}")
                import traceback
                traceback.print_exc()
        
        print("\n" + "=" * 90)
        print(f"✨ PIPELINE FINALIZADO CON ÉXITO".center(90))
        print(f"📂 Resultados consolidados en: {self.output_dir}".center(90))
        print("=" * 90)
    
    def _print_cohort_summary(self):
        """Imprime resumen de configuración de cohortes."""
        print(f"\n📊 CONFIGURACIÓN DE COHORTES:")
        print(f"   Granularidad: {self.cohort_config.granularity.value}")
        print(f"   Ventanas conversión: {self.cohort_config.conversion_windows}")
        print(f"   Total cohortes: {self.cohort_config.num_periods or 'auto'}")
        if self.cohort_config.start_date:
            print(f"   Rango: {self.cohort_config.start_date.date()} → {self.cohort_config.end_date.date()}")
        print("-" * 90)
    
    # ==========================================================================
    # MÉTODOS PRIVADOS
    # ==========================================================================
    def _get_orchestrator_class(self, mode: int):
        """Retorna la clase orquestador para un modo de dimensión."""
        class_map = {
            DimensionMode.CATEGORY: ('CategoryBehaviorOrchestrator', 
                                     'Category.Orchestrators.behavior_orchestrator'),
            DimensionMode.SUBCATEGORY: ('SubcategoryBehaviorOrchestrator',
                                        'Category.Orchestrators.subcat_behavior_orchestrator'),
            DimensionMode.BRAND: ('BrandBehaviorOrchestrator',
                                  'Category.Orchestrators.brand_behavior_orchestrator'),
            DimensionMode.PRODUCT: ('ProductBehaviorOrchestrator',
                                    'Category.Orchestrators.product_behavior_orchestrator'),
            DimensionMode.SUBCATEGORY_BRAND: ('SubcategoryBrandOrchestrator',
                                              'Category.Orchestrators.subcategory_brand_orchestrator'),
        }
        
        if mode not in class_map:
            raise ValueError(f"No hay orquestador para modo {mode}")
        
        class_name, module_path = class_map[mode]
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    
    def _get_exporter_class(self, mode: int):
        """Retorna la clase exporter para un modo de dimensión."""
        exporter_map = {
            DimensionMode.CATEGORY: ('CategoryExporter',
                                     'Category.Reporting.category_exporter'),
            DimensionMode.SUBCATEGORY: ('SubcategoryExporter',
                                        'Category.Reporting.subcategory_exporter'),
            DimensionMode.BRAND: ('BrandExporter',
                                  'Category.Reporting.brand_exporter'),
            DimensionMode.PRODUCT: ('ProductExporter',
                                    'Category.Reporting.product_exporter'),
            DimensionMode.SUBCATEGORY_BRAND: ('SubcategoryBrandExporter',
                                              'Category.Reporting.subcategory_brand_exporter'),
        }
        
        if mode not in exporter_map:
            raise ValueError(f"No hay exporter para modo {mode}")
        
        class_name, module_path = exporter_map[mode]
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    
    def _process_dimension(self, mode: int):
        """Procesa una dimensión específica."""
        config = get_dimension_config(mode)
        dim_name = config['output_key']
        
        print(f"\n{'='*60}")
        print(f"📊 PROCESANDO DIMENSIÓN: {dim_name.upper()} (modo {mode})")
        print(f"   grouping_mode: {self.grouping_mode}")
        print(f"   cohort_granularity: {self.cohort_config.granularity.value}")
        print(f"{'='*60}")
        
        # 1. Obtener orquestador y ejecutar (pasar cohort_config)
        OrchestratorClass = self._get_orchestrator_class(mode)
        orchestrator = OrchestratorClass(
            customers=self.customers,
            grouping_mode=self.grouping_mode,
            cohort_config=self.cohort_config
        )
        results = orchestrator.run()
        
        # 2. Obtener exporter
        ExporterClass = self._get_exporter_class(mode)
        exporter = ExporterClass(
            results_dict=results,
            customers=self.customers,
            ue_results=self.ue_results,
            grouping_mode=self.grouping_mode
        )
        
        # 3. Construir summary DataFrames
        print(f"📊 Construyendo summary DataFrames para {dim_name}...")
        df_summary_hist = exporter.build_summary_dataframe(mode="historical")
        df_summary_cohort = exporter.build_summary_dataframe(mode="cohorts")
        
        # 4. Exportar a Excel
        exporter.export_to_excel(
            filename=config['excel_filename'],
            df_summary_hist=df_summary_hist,
            df_summary_cohort=df_summary_cohort
        )
        
        # 5. Dashboard y visualizaciones
        self._generate_dashboard_and_visuals(results, df_summary_hist, config)
        
        print(f"✅ {dim_name} completado")
    
    def _generate_dashboard_and_visuals(self, results: dict, summary_df: pd.DataFrame, config: dict):
        """Genera dashboard y visualizaciones para una dimensión."""
        try:
            from Category.Analytics.dashboard_calculator import CategoryDashboardCalculator
            from Category.Reporting.dashboard_exporter import CategoryDashboardExporter
            from Category.Reporting.visualizer import CategoryVisualizer
            
            dim_name = config['output_key']
            
            # Dashboard calculator
            calc = CategoryDashboardCalculator(results)
            dash_data = calc.run()
            
            if not dash_data:
                print(f"⚠️ No se generaron insights para {dim_name}")
                return
            
            # Mapeo de mode a analysis_type
            analysis_type_map = {
                1: "category",
                2: "subcategory",
                3: "brand",
                4: "product",
                5: "subcategory_brand"
            }
            analysis_type = analysis_type_map.get(config['mode_id'], "category")
            
            visualizer = CategoryVisualizer(
                dashboard_data=dash_data,
                output_dir=self.output_dir,
                timestamp=self.timestamp,
                summary_df=summary_df
            )
            visualizer.run(analysis_type=analysis_type)
            
            # Dashboard exporter
            dash_exporter = CategoryDashboardExporter(
                dashboard_data=dash_data,
                summary_df=summary_df,
                dimension_name=dim_name
            )
            txt_path = os.path.join(self.output_dir, f"{config['txt_filename']}_{self.timestamp}.txt")
            dash_exporter.export_as_txt(txt_path)
            
        except ImportError as e:
            print(f"⚠️ Dashboard/Visualizer no disponible para {dim_name}: {e}")
        except Exception as e:
            print(f"⚠️ Error en dashboard/visuals para {dim_name}: {e}")