# ============================================================================
# FILE: Run/Menu/menu_controller.py
# COMPLETO - CON SEÑALES DE NAVEGACIÓN
# ============================================================================
# Archivo: Run/Menu/menu_controller.py
# Versión v13.3 - MULTI-PAÍS CON NAVEGACIÓN MEJORADA

import os
import sys
import signal
from pathlib import Path
from typing import List, Optional, Tuple

RUN_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RUN_PATH not in sys.path:
    sys.path.insert(0, RUN_PATH)

from Run.Config.paths import Paths, PathsConfig
from Run.Config.credentials import Credentials
from Run.Config.dev_mode_manager import DevModeManager
from Run.Country.country_context import CountryContext
from Run.FX.fx_engine import FXEngine
from Run.Menu.menu_auth import MenuAuth
from Run.Menu.menu_config import MenuConfig
from Run.Menu.menu_executor import MenuExecutor
from Run.Core.cohort_context_manager import CohortContextManager
from Run.Utils.logger import SystemLogger


class MenuController:
    """Orquestador principal - MULTI-PAÍS CON NAVEGACIÓN MEJORADA"""
    
    # Constantes de modos
    MODE_FULL = '1'
    MODE_DR_ONLY = '2'
    MODE_MODEL = '3'
    MODE_QUERY = '4'
    MODE_CONFIG = '5'
    MODE_CHANGE_COUNTRY = '0'
    MODE_BACK = 'b'
    MODE_QUIT = 'q'
    
    # Señales de retorno
    RETURN_EXIT = "EXIT"
    RETURN_BACK_TO_COUNTRY = "BACK_TO_COUNTRY"
    
    SUBMODE_MODEL_COMPLETE = '1'
    SUBMODE_MODEL_GENERAL = '2'
    SUBMODE_CATEGORY = '3'
    SUBMODE_SUBCATEGORY = '4'
    SUBMODE_BRAND = '5'
    SUBMODE_PRODUCT = '6'
    SUBMODE_SPECIAL = '7'
    SUBMODE_HEAVY_ONLY = '8'
    
    DIM_CATEGORY = '1'
    DIM_SUBCATEGORY = '2'
    DIM_BRAND = '3'
    DIM_PRODUCT = '4'
    
    def __init__(self, paths: PathsConfig, country_context: CountryContext, fx_engine: FXEngine):
        self.paths = paths
        self.country_context = country_context
        self.fx_engine = fx_engine
        self.logger = SystemLogger()
        
        self.dev_mode = DevModeManager()
        self.auth = None
        
        self.config = MenuConfig(paths, self.logger)
        self.executor = MenuExecutor(paths, self.logger, country_context, fx_engine)
        
        self._cohort_context = None
        
        # Configurar manejador de señales para cancelación
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def set_auth(self, auth):
        """Inyecta el objeto de autenticación después de la creación."""
        self.auth = auth
        if self.auth and self.country_context:
            self.auth.set_country(self.country_context.code)
    
    def _signal_handler(self, signum, frame):
        """Maneja Ctrl+C durante ejecución"""
        if hasattr(self, 'executor') and self.executor.current_process_running:
            print("\n\n⚠️ Proceso en ejecución. ¿Cancelar? (s/n): ", end="")
            resp = input().strip().lower()
            if resp in ['s', 'si', 'sí', 'yes', 'y']:
                self.executor.request_cancel()
                print("⏹️ Cancelación solicitada...")
        else:
            self._graceful_shutdown_handler(signum, frame)
    
    def _graceful_shutdown_handler(self, signum, frame):
        print("\n\n⚠️ Interrupción detectada. Cerrando conexiones...")
        self.logger.info("Ctrl+C detectado - iniciando shutdown graceful")
        self.executor.ssh_manager.stop()
    
    def _get_cohort_context(self) -> CohortContextManager:
        if self._cohort_context is None:
            supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
            self._cohort_context = CohortContextManager(supuestos_path, self.country_context)
        return self._cohort_context
    
    def _validate_input_files(self) -> bool:
        input_dir = self.paths.inputs_dir
        required_files = [self.paths.sois_file, self.paths.supuestos_file, self.paths.catalogo_file]
        
        missing = []
        for file in required_files:
            full_path = input_dir / file
            if not full_path.exists():
                missing.append(file)
        
        if missing:
            print(f"\n❌ Archivos faltantes en {input_dir}:")
            for f in missing:
                print(f"   • {f}")
            print("\n📌 Opciones:")
            print("   1. Colocar los archivos en la carpeta indicada")
            print("   2. Cambiar carpeta de entrada (opción en Configuraciones)")
            return False
        
        print(f"✅ Archivos Excel encontrados ({len(required_files)}/{len(required_files)})")
        return True
    
    def _validate_pre_conditions(self) -> bool:
        print("\n" + "🔍 VALIDACIÓN PRE-OPERACIONAL".center(60, "-"))
        
        if not self._validate_input_files():
            return False
        
        print(f"🌎 País activo: {self.country_context.name} ({self.country_context.code})")
        print(f"💱 Tipo de cambio base: {self.country_context.default_fx_rate}")
        
        test_file = self.paths.results_base / ".write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
            print("✅ Permisos de escritura OK")
        except Exception:
            print(f"❌ No se puede escribir en {self.paths.results_base}")
            return False
        
        print("-" * 60)
        return True
    
# ============================================================================
# FILE: Run/Menu/menu_controller.py
# SECCIÓN MODIFICADA - display_main_menu y run()
# ============================================================================

    def display_main_menu(self):
        """Menú principal - SIN opción de cambiar país (usar 'b' para volver)"""
        print("\n" + "=" * 60)
        print(f"      SISTEMA LTV - {self.country_context.name}".center(60))
        print("=" * 60)
        print(f"\n⚙️ CONFIGURACIÓN ACTUAL:")
        print(f"   🌎 País: {self.country_context.name} ({self.country_context.currency})")
        print(f"   📊 Agrupación: {self.config.get_grouping_mode_display()}")
        print(f"   🏷️  Modo marca: {self.config.get_brand_mode_display()}")
        print(f"   📅 Granularidad: {self.config.get_granularity_display()}")
        print(f"   📂 Input dir: {self.paths.inputs_dir}")
        print("\n" + "-" * 40)
        print("1. 🚀 PIPELINE COMPLETO")
        print("2. 💾 SOLO DATA REPOSITORY")
        print("3. 📊 MODELO")
        print("4. 🔍 BUSCADOR")
        print("5. ⚙️ CONFIGURACIONES")
        print("b. 🔙 Cambiar país / Volver")
        print("q. ❌ SALIR")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()

    
    def display_model_submenu(self):
        print("\n" + "=" * 60)
        print(f"      MODELO - {self.country_context.name}".center(60))
        print("=" * 60)
        print(f"\n⚙️ Modo marca actual: {self.config.get_brand_mode_display()}")
        print(f"📅 Granularidad actual: {self.config.get_granularity_display()}")
        print("\n1. 📊 Modelo COMPLETO (Todas las dimensiones + análisis pesados)")
        print("2. 📋 Modelo GENERAL (Solo Category + Subcategory)")
        print("3. 📂 Categoría (Category)")
        print("4. 📁 Subcategoría (Subcategory)")
        print("5. 🏷️ Marca (Brand)")
        print("6. 🎯 Producto")
        print("7. 🎛️ Especial (Seleccionar dimensiones específicas)")
        print("8. 🔬 SOLO ANÁLISIS PESADOS (sin reportes multi-dimensión)")
        print("\nb. 🔙 Volver al menú principal")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    def display_config_submenu(self):
        print("\n" + "=" * 60)
        print("      CONFIGURACIONES".center(60))
        print("=" * 60)
        print(f"\n1. 🔄 Modo de agrupación: {self.config.get_grouping_mode_display()}")
        print(f"2. 🏷️  Modo de análisis de marca: {self.config.get_brand_mode_display()}")
        print(f"3. 📅 Granularidad de cohortes: {self.config.get_granularity_display()}")
        print(f"4. 📂 Cambiar carpeta de ENTRADA (inputs)")
        print(f"5. 💾 Cambiar carpeta de SALIDA (resultados)")
        print(f"6. 📊 GESTIÓN DE COHORTES (agregar/editar/ver/eliminar)")
        print("\nb. 🔙 Volver")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    def display_special_dimensions_menu(self, selected: List[str]) -> str:
        print("\n" + "=" * 60)
        print("      SELECCIÓN DE DIMENSIONES".center(60))
        print("=" * 60)
        print(f"\n✅ Dimensiones seleccionadas: {', '.join(selected) if selected else 'NINGUNA'}")
        print("\n📂 Dimensiones disponibles:")
        print("   1. Categoría (Category)")
        print("   2. Subcategoría (Subcategory)")
        print("   3. Marca (Brand)")
        print("   4. Producto (Product)")
        print("\n   q. ✅ Ejecutar análisis con las dimensiones seleccionadas")
        print("   r. 🔄 Reiniciar selección")
        print("   b. 🔙 Volver al menú de modelo")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    # ==================================================================
    # MÉTODOS DE CONFIGURACIÓN
    # ==================================================================
    
    def _select_grouping_mode(self):
        self.config.select_grouping_mode()
    
    def _select_brand_mode(self):
        self.config.select_brand_mode()
    
    def _select_granularity(self):
        self.config.select_granularity()
    
    def _select_input_folder(self):
        new_path = Paths.select_input_folder(self.country_context.code)
        if new_path:
            self.paths.inputs_dir = new_path
    
    def _select_output_folder(self):
        new_path = Paths.select_output_folder(self.country_context.code)
        if new_path:
            self.paths.results_base = new_path
    
    def _manage_cohorts(self):
        self.config.manage_cohorts_menu()
    
    # ==================================================================
    # MÉTODOS DE EJECUCIÓN
    # ==================================================================
    
    def _run_full_pipeline(self):
        Credentials.load_for_country(self.country_context.code)
        
        if not self._validate_pre_conditions():
            return
        date_range = self.executor.get_date_range_from_user()
        self.executor.run_full_pipeline(
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_dr_only(self):
        Credentials.load_for_country(self.country_context.code)
        
        if not self._validate_pre_conditions():
            return
        date_range = self.executor.get_date_range_from_user()
        self.executor.run_dr_only(date_range)
    
    def _run_model_complete(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1, 2, 3, 4, 5, 6], "Modelo Completo",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_general(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1, 2], "Modelo General", only_category=True,
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_category(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1], "Categoría",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_subcategory(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [2], "Subcategoría",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_brand(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        dim_code = self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode]
        dim_name = self.config.get_brand_mode_display()
        self.executor.run_model_analysis(
            [dim_code], dim_name,
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_product(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [4], "Producto",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_special_mode(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        selected = []
        dim_map = {
            "Categoría": 1,
            "Subcategoría": 2,
            "Marca": self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode],
            "Producto": 4,
        }
        
        while True:
            option = self.display_special_dimensions_menu(selected)
            
            if option == self.DIM_CATEGORY:
                if "Categoría" not in selected:
                    selected.append("Categoría")
            elif option == self.DIM_SUBCATEGORY:
                if "Subcategoría" not in selected:
                    selected.append("Subcategoría")
            elif option == self.DIM_BRAND:
                if "Marca" not in selected:
                    selected.append("Marca")
            elif option == self.DIM_PRODUCT:
                if "Producto" not in selected:
                    selected.append("Producto")
            elif option == 'r':
                selected = []
                print("\n🔄 Selección reiniciada.")
            elif option == 'q':
                if not selected:
                    print("❌ No has seleccionado ninguna dimensión.")
                    continue
                dimensions = [dim_map[name] for name in selected]
                display_name = " + ".join(selected)
                self.executor.run_model_analysis(
                    dimensions, display_name, only_category=False, date_range=date_range,
                    grouping_mode=self.config.current_grouping_mode,
                    conversion_mode=self.config.current_conversion_mode,
                    granularity=self.config.current_granularity
                )
                return
            elif option == 'b':
                return
            else:
                print("❌ Opción inválida.")
    
    def _run_heavy_analysis_only(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_heavy_analysis_only(
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_query_mode(self):
        from Model.Domain.controller import LTVController
        from Model.Data.real_data_repository import RealDataRepository
        from Model.Data.cac_repository import CACRepository
        from Category.Utils.query_engine import DimensionQueryEngine
        from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
        from types import SimpleNamespace
        
        print("\n" + "=" * 60)
        print(f"      BUSCADOR INTERACTIVO LTV - {self.country_context.name}".center(60))
        print("=" * 60)
        
        confirm = input("\n👉 ¿Continuar? (s/n): ").strip().lower()
        if confirm not in ['s', 'si', 'sí', 'yes', 'y']:
            return
        
        if not self.executor.data_ltv_has_files():
            print("\n❌ No se encontraron datos en Data_LTV")
            respuesta = input("¿Deseas ejecutar DataRepository primero? (s/n): ").strip().lower()
            if respuesta in ['s', 'si', 'sí', 'yes', 'y']:
                if not self.executor.run_dr():
                    print("❌ DataRepository falló.")
                    return
            else:
                return
        
        try:
            real_repo = RealDataRepository()
            raw_data = real_repo.get_orders_from_excel(
                path_or_dir=str(self.paths.data_ltv),
                country_config=self.country_context
            )
            
            ltv_engine = LTVController()
            ltv_engine.process_raw_data(raw_data)
            customers = ltv_engine.get_customers()
            
            print(f"   ✅ {len(customers)} clientes cargados en memoria")
            
            granularity = self.config.current_granularity
            cac_path = self.paths.inputs_dir / self.paths.cac_file
            
            temp_country_config = SimpleNamespace(
                code=self.country_context.code,
                cac_sheet=self.country_context.code,
                name=self.country_context.name
            )
            
            print(f"   📂 Leyendo CAC desde: {cac_path}")
            print(f"   📊 País: {self.country_context.code}, Hoja: {temp_country_config.cac_sheet}")
            
            ad_spend = CACRepository.get_cac_mapping(
                country_config=temp_country_config,
                cac_path=str(cac_path),
                granularity=granularity,
                transform=True
            )
            
            print(f"   📊 CAC cargado: {len(ad_spend)} cohortes")
            if ad_spend:
                sample = list(ad_spend.items())[:5]
                for cohort, cac_val in sample:
                    print(f"      {cohort}: ${cac_val:.2f}")
                if len(ad_spend) > 5:
                    print(f"      ... y {len(ad_spend) - 5} más")
            else:
                print(f"   ⚠️ No se encontró CAC para {self.country_context.code}")
                print(f"   Verifica que CAC.xlsx tenga una hoja llamada '{self.country_context.code}'")
                print(f"   con columnas 'cohort' y 'cac'")
            
            granularity_map = {
                "quarterly": TimeGranularity.QUARTERLY,
                "monthly": TimeGranularity.MONTHLY,
                "weekly": TimeGranularity.WEEKLY,
                "semiannual": TimeGranularity.SEMIANNUAL,
                "yearly": TimeGranularity.YEARLY,
            }
            time_granularity = granularity_map.get(granularity, TimeGranularity.QUARTERLY)
            cohort_config = CohortConfig(granularity=time_granularity)
            
            engine = DimensionQueryEngine(
                customers,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                ue_results=None,
                cohort_config=cohort_config,
                cac_map=ad_spend
            )
            
            while True:
                print("\n" + "-" * 40)
                print(f"🔍 BUSCADOR LTV - {self.country_context.name}")
                print("-" * 40)
                print("1. 📂 Buscar por CATEGORÍA")
                print("2. 📁 Buscar por SUBCATEGORÍA")
                
                if self.config.current_brand_mode == self.config.BRAND_MODE_FLAT:
                    print("3. 🏷️ Buscar por MARCA (plano)")
                else:
                    print("3. 🏷️ Buscar por MARCA (jerárquico)")
                
                print("4. 🎯 Buscar por PRODUCTO")
                print("5. 🔄 Cambiar modo de conversión")
                print("6. 📅 Cambiar granularidad de cohortes")
                print("b. 🔙 Volver al menú principal")
                print("-" * 40)
                print(f"⚙️ Modo conversión: {self.config.get_conversion_mode_display()}")
                print(f"📅 Granularidad: {self.config.get_granularity_display()}")
                
                option = input("\n👉 Opción: ").strip().lower()
                
                if option == '1':
                    engine.interactive_search(dimension="category")
                elif option == '2':
                    engine.interactive_search(dimension="subcategory")
                elif option == '3':
                    if self.config.current_brand_mode == self.config.BRAND_MODE_FLAT:
                        engine.interactive_search(dimension="brand")
                    else:
                        engine.interactive_search(dimension="subcategory_brand")
                elif option == '4':
                    engine.interactive_search(dimension="name")
                elif option == '5':
                    if engine.conversion_mode == engine.CONVERSION_CUMULATIVE:
                        engine.set_conversion_mode(engine.CONVERSION_INCREMENTAL)
                    else:
                        engine.set_conversion_mode(engine.CONVERSION_CUMULATIVE)
                    self.config.current_conversion_mode = engine.conversion_mode
                    print(f"\n✅ Modo cambiado a: {self.config.get_conversion_mode_display()}")
                elif option == '6':
                    self._change_granularity_in_query(engine)
                elif option == 'b':
                    break
                else:
                    print("❌ Opción inválida.")
                    
        except ImportError as e:
            print(f"\n❌ Error importando módulos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
        except Exception as e:
            print(f"\n❌ Error al cargar datos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
    
    def _change_granularity_in_query(self, engine):
        from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
        from Category.Cohort.cohort_manager import CohortManager
        
        print("\n" + "=" * 50)
        print("   CAMBIAR GRANULARIDAD DE COHORTES".center(50))
        print("=" * 50)
        print(f"Granularidad actual: {self.config.current_granularity}")
        print("\nOpciones:")
        print("   1. Anual (yearly)")
        print("   2. Semestral (semiannual)")
        print("   3. Trimestral (quarterly) - DEFAULT")
        print("   4. Mensual (monthly)")
        print("   5. Semanal (weekly)")
        print("   b. Cancelar")
        
        option = input("\n👉 Opción: ").strip()
        
        granularity_map = {
            '1': 'yearly',
            '2': 'semiannual',
            '3': 'quarterly',
            '4': 'monthly',
            '5': 'weekly',
        }
        
        if option in granularity_map:
            new_granularity = granularity_map[option]
            self.config.current_granularity = new_granularity
            self.config._save_config()
            
            time_map = {
                'quarterly': TimeGranularity.QUARTERLY,
                'monthly': TimeGranularity.MONTHLY,
                'weekly': TimeGranularity.WEEKLY,
                'semiannual': TimeGranularity.SEMIANNUAL,
                'yearly': TimeGranularity.YEARLY,
            }
            
            new_config = CohortConfig(granularity=time_map.get(new_granularity, TimeGranularity.QUARTERLY))
            
            cohort_context = self._get_cohort_context()
            new_cac_map = cohort_context.get_cac_map(granularity=new_granularity)
            
            engine.cohort_config = new_config
            engine.cohort_manager = CohortManager(new_config)
            engine.cac_map = new_cac_map
            
            print(f"✅ Granularidad cambiada a: {new_granularity}")
            input("\nPresiona Enter para continuar...")
        elif option == 'b':
            return
    
    def _wait_for_user(self):
        input("\n👉 Presiona Enter para volver al menú principal...")
    
    # ==================================================================
    # MÉTODO PRINCIPAL RUN - RETORNA SEÑALES
    # ==================================================================
    
    def run(self):
        """Ejecuta el menú principal. Retorna señal para el main loop."""
        
        Credentials.load_for_country(self.country_context.code)
        
        while True:
            main_option = self.display_main_menu()
            
            # 🔧 UNIFICADO: 'b' siempre vuelve al selector de países
            if main_option == self.MODE_BACK:
                self.executor.ssh_manager.stop()
                return self.RETURN_BACK_TO_COUNTRY
            
            elif main_option == self.MODE_FULL:
                self._run_full_pipeline()
                self._wait_for_user()
                
            elif main_option == self.MODE_DR_ONLY:
                self._run_dr_only()
                self._wait_for_user()
                
            elif main_option == self.MODE_MODEL:
                date_range = self.executor.get_date_range_from_user()
                while True:
                    model_option = self.display_model_submenu()
                    
                    if model_option == self.SUBMODE_MODEL_COMPLETE:
                        self._run_model_complete(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_MODEL_GENERAL:
                        self._run_model_general(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_CATEGORY:
                        self._run_model_category(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_SUBCATEGORY:
                        self._run_model_subcategory(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_BRAND:
                        self._run_model_brand(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_PRODUCT:
                        self._run_model_product(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_SPECIAL:
                        self._run_special_mode(date_range)
                        self._wait_for_user()
                    elif model_option == self.SUBMODE_HEAVY_ONLY:
                        self._run_heavy_analysis_only(date_range)
                        self._wait_for_user()
                    elif model_option == self.MODE_BACK:
                        break
                    else:
                        print("❌ Opción inválida.")
                        
            elif main_option == self.MODE_QUERY:
                self._run_query_mode()
                
            elif main_option == self.MODE_CONFIG:
                while True:
                    config_option = self.display_config_submenu()
                    if config_option == '1':
                        self._select_grouping_mode()
                    elif config_option == '2':
                        self._select_brand_mode()
                    elif config_option == '3':
                        self._select_granularity()
                    elif config_option == '4':
                        self._select_input_folder()
                    elif config_option == '5':
                        self._select_output_folder()
                    elif config_option == '6':
                        self._manage_cohorts()
                    elif config_option == self.MODE_BACK:
                        break
                    else:
                        print("❌ Opción inválida")
                        
            elif main_option == self.MODE_QUIT:
                self.executor.ssh_manager.stop()
                print("\n👋 ¡Hasta luego!")
                return self.RETURN_EXIT
            else:
                print("❌ Opción inválida")