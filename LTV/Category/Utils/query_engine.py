# Category/Utils/query_engine.py
"""
Motor de consultas para buscar dimensiones específicas (Category, Subcategory, Brand, Product, Subcategory+Brand)
sin necesidad de regenerar todo el pipeline.

VERSIÓN DINÁMICA v2.0 - Soporta cohortes dinámicos vía CohortManager.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import os
import re

from Category.Cohort.cohort_manager import CohortManager
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
from Model.Data.cac_repository import CACRepository


class DimensionQueryEngine:
    """
    Motor para consultar métricas de dimensiones específicas.
    
    Uso:
        # Con configuración por defecto (quarterly)
        engine = DimensionQueryEngine(customers, grouping_mode="entry_based")
        
        # Con configuración personalizada (mensual)
        config = CohortConfig(granularity=TimeGranularity.MONTHLY)
        engine = DimensionQueryEngine(customers, grouping_mode="entry_based", cohort_config=config)
        
        # Buscar categoría específica
        result = engine.query(category="Electrónica")
    """
    
    # Modos de agrupación
    GROUPING_BEHAVIORAL = "behavioral"
    GROUPING_ENTRY_BASED = "entry_based"
    
    # Modos de conversión
    CONVERSION_CUMULATIVE = "cumulative"    # Acumulativa (creciente) - DEFAULT
    CONVERSION_INCREMENTAL = "incremental"  # Incremental (distribución)
    
    # En Category/Utils/query_engine.py

    def __init__(self, customers: List[Any], grouping_mode: str = "entry_based",
                conversion_mode: str = "cumulative", ue_results: dict = None,
                cohort_config: Optional[CohortConfig] = None,
                cac_map: Optional[Dict[str, float]] = None):  # ← NUEVO PARÁMETRO
        """
        Args:
            customers: Lista de objetos Customer
            grouping_mode: "entry_based" o "behavioral"
            conversion_mode: "cumulative" o "incremental"
            ue_results: Resultados de UnitEconomicsAnalyzer (opcional)
            cohort_config: Configuración de cohortes
            cac_map: Mapa de CAC ya transformado (opcional, priority sobre ue_results)
        """
        self.customers = customers
        self.grouping_mode = grouping_mode
        self.conversion_mode = conversion_mode
        self._cache = {}
        self._product_groups = None
        
        # CohortManager
        self.cohort_config = cohort_config or CohortConfig()
        self.cohort_manager = CohortManager(self.cohort_config)
        print(f"   📊 CohortManager granularidad: {self.cohort_config.granularity.value}")
        
        # ========== CAC MAP: prioridad a cac_map si se pasa ==========
        if cac_map is not None:
            self.cac_map = cac_map
            print(f"   📊 CAC map recibido directamente: {len(self.cac_map)} cohortes")
        elif ue_results:
            # Extraer desde ue_results (compatibilidad)
            if "cohorts" in ue_results:
                cohorts_data = ue_results["cohorts"]
            else:
                cohorts_data = ue_results
            
            self.cac_map = {}
            for cohort_id, data in cohorts_data.items():
                if isinstance(data, dict):
                    self.cac_map[cohort_id] = data.get("cac", 0)
                else:
                    self.cac_map[cohort_id] = data
            print(f"   📊 CAC map desde ue_results: {len(self.cac_map)} cohortes")
        else:
            # Intentar cargar desde archivo
            cac_path = os.environ.get("LTV_CAC_PATH")
            if cac_path:
                self.cac_map = CACRepository.get_cac_mapping(cac_path, self.cohort_config.granularity.value, transform=True)
            else:
                self.cac_map = {}
            print(f"   📊 CAC map desde archivo: {len(self.cac_map)} cohortes")
        
        if not self.cac_map:
            print(f"   ⚠️ Sin mapa de CAC - LTV/CAC ratio no disponible")
        
        mode_display = "Comportamental" if grouping_mode == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        conv_display = "Acumulativa" if conversion_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        print(f"🔧 DimensionQueryEngine inicializado con grouping_mode={mode_display}, conversion_mode={conv_display}")
    
    def _get_cac_for_customer(self, customer) -> float:
        """
        Obtiene el CAC para un cliente basado en su cohorte (fecha de primera compra).
        Usa CohortManager para obtener el cohort_id dinámicamente.
        """
        orders = customer.get_orders_sorted()
        if not orders:
            return 0
        
        first_order = orders[0]
        first_date = first_order.order_date
        
        # Usar CohortManager para obtener cohort_id dinámico
        cohort_id = self.cohort_manager.get_cohort_id(first_date)
        
        return self.cac_map.get(cohort_id, 0)
    
    def _get_dimension_customers(self, dimension: str, value: str) -> List[Any]:
        """
        Filtra clientes según el modo de agrupación.
        
        Args:
            dimension: "category", "subcategory", "brand", "name", "subcategory_brand"
            value: Valor a buscar
        
        Returns:
            Lista de clientes que cumplen con el criterio
        """
        filtered_customers = []
        
        # Normalizar el valor de búsqueda
        search_value_raw = str(value).strip()
        search_value_lower = search_value_raw.lower()
        
        # Para subcategory_brand, crear versiones normalizadas
        if dimension == "subcategory_brand":
            # Normalizar: eliminar espacios extras, unificar separadores
            search_value_norm = (
                search_value_lower
                .replace('|', ' ')
                .replace('(', ' ')
                .replace(')', ' ')
                .replace('-', ' ')
                .split()
            )
            search_value_norm = ' '.join(search_value_norm)
        else:
            search_value_norm = search_value_lower
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            
            if self.grouping_mode == self.GROUPING_ENTRY_BASED:
                # Modo entry_based: solo considerar la PRIMERA compra
                if not orders:
                    continue
                
                first_order = orders[0]
                
                if dimension == "subcategory_brand":
                    attr_value_raw = getattr(first_order, "subcategory_brand", "N/A")
                    attr_value = str(attr_value_raw).strip()
                    attr_value_lower = attr_value.lower()
                    
                    # Normalizar el valor del atributo
                    attr_value_norm = (
                        attr_value_lower
                        .replace('|', ' ')
                        .replace('(', ' ')
                        .replace(')', ' ')
                        .replace('-', ' ')
                        .split()
                    )
                    attr_value_norm = ' '.join(attr_value_norm)
                    
                    # Comparar versiones normalizadas
                    if attr_value_norm == search_value_norm:
                        filtered_customers.append(customer)
                else:
                    attr_value = str(getattr(first_order, dimension, "")).strip().lower()
                    if attr_value == search_value_lower:
                        filtered_customers.append(customer)
                        
            else:  # behavioral
                # Modo behavioral: considerar TODAS las compras
                has_dimension = False
                
                for order in orders:
                    if dimension == "subcategory_brand":
                        attr_value_raw = getattr(order, "subcategory_brand", "N/A")
                        attr_value = str(attr_value_raw).strip()
                        attr_value_lower = attr_value.lower()
                        
                        # Normalizar el valor del atributo
                        attr_value_norm = (
                            attr_value_lower
                            .replace('|', ' ')
                            .replace('(', ' ')
                            .replace(')', ' ')
                            .replace('-', ' ')
                            .split()
                        )
                        attr_value_norm = ' '.join(attr_value_norm)
                        
                        if attr_value_norm == search_value_norm:
                            has_dimension = True
                            break
                    else:
                        attr_value = str(getattr(order, dimension, "")).strip().lower()
                        if attr_value == search_value_lower:
                            has_dimension = True
                            break
                
                if has_dimension:
                    filtered_customers.append(customer)
        
        return filtered_customers
    
    def _calculate_conversion_rates(self, customers: List[Any], total_clientes: int) -> Dict[str, float]:
        """
        Calcula las tasas de conversión según el modo seleccionado.
        Usa las ventanas de conversión del CohortConfig.
        """
        windows = self.cohort_config.conversion_windows
        base = total_clientes if total_clientes > 0 else 1
        
        if self.conversion_mode == self.CONVERSION_INCREMENTAL:
            # Modo INCREMENTAL: cada cliente cuenta SOLO en la primera ventana que cumple
            conv_counts = {w: 0 for w in windows}
            
            for customer in customers:
                orders = customer.get_orders_sorted()
                if len(orders) >= 2:
                    d1 = orders[0].order_date
                    d2 = orders[1].order_date
                    diff = (d2 - d1).days
                    
                    for w in windows:
                        if diff <= w:
                            conv_counts[w] += 1
                            break  # Solo la PRIMERA ventana
            
            return {
                f"Pct_Conv_{w}d": round((conv_counts[w] / base) * 100, 2)
                for w in windows
            }
        
        else:  # Modo CUMULATIVE
            conv_counts = {w: 0 for w in windows}
            
            for customer in customers:
                orders = customer.get_orders_sorted()
                if len(orders) >= 2:
                    d1 = orders[0].order_date
                    d2 = orders[1].order_date
                    diff = (d2 - d1).days
                    
                    # Acumulativo: cuenta en TODAS las ventanas que cumple
                    for w in windows:
                        if diff <= w:
                            conv_counts[w] += 1
            
            return {
                f"Pct_Conv_{w}d": round((conv_counts[w] / base) * 100, 2)
                for w in windows
            }
    
    def _calculate_metrics_for_customers(self, customers: List[Any], 
                                          dimension_name: str,
                                          dimension_value: str) -> Dict[str, Any]:
        """
        Calcula métricas para un conjunto de clientes filtrados por dimensión.
        """
        if not customers:
            return {
                "dimension": dimension_name,
                "value": dimension_value,
                "grouping_mode": self.grouping_mode,
                "conversion_mode": self.conversion_mode,
                "granularity": self.cohort_config.granularity.value,
                "found": False,
                "error": "No se encontraron clientes con este valor"
            }
        
        # Métricas básicas
        total_clientes = len(customers)
        
        # Contar órdenes por cliente
        order_counts = [len(c.get_orders_sorted()) for c in customers]
        total_ordenes = sum(order_counts)
        pedidos_promedio = round(total_ordenes / total_clientes, 2) if total_clientes > 0 else 0
        
        # Frecuencia de compras
        n_c2 = sum(1 for c in order_counts if c >= 2)
        n_c3 = sum(1 for c in order_counts if c >= 3)
        n_c4 = sum(1 for c in order_counts if c >= 4)
        
        pct_2da = round((n_c2 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        pct_3ra = round((n_c3 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        pct_4ta = round((n_c4 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        
        # Revenue y LTV
        total_revenue = sum(c.total_revenue() for c in customers)
        total_cp = sum(c.total_cp() for c in customers)  # LTV BRUTO (sin CAC)
        aov = round(total_revenue / total_ordenes, 2) if total_ordenes > 0 else 0
        ltv_promedio = round(total_cp / total_clientes, 2) if total_clientes > 0 else 0
        
        # Calcular CAC y LTV/CAC Ratio (usando CohortManager)
        total_cac = 0
        for customer in customers:
            total_cac += self._get_cac_for_customer(customer)
        
        cac_promedio = round(total_cac / total_clientes, 2) if total_clientes > 0 else 0
        ltv_cac_ratio = round(ltv_promedio / cac_promedio, 2) if cac_promedio > 0 else 0
        
        # Tiempo entre compras (mediana)
        tiempos_1a2 = []
        tiempos_2a3 = []
        tiempos_3a4 = []
        
        for customer in customers:
            orders = customer.get_orders_sorted()
            if len(orders) >= 2:
                diff = (orders[1].order_date - orders[0].order_date).days
                if diff > 0:
                    tiempos_1a2.append(diff)
            if len(orders) >= 3:
                diff = (orders[2].order_date - orders[1].order_date).days
                if diff > 0:
                    tiempos_2a3.append(diff)
            if len(orders) >= 4:
                diff = (orders[3].order_date - orders[2].order_date).days
                if diff > 0:
                    tiempos_3a4.append(diff)
        
        mediana_1a2 = round(np.median(tiempos_1a2), 0) if tiempos_1a2 else 0
        mediana_2a3 = round(np.median(tiempos_2a3), 0) if tiempos_2a3 else 0
        mediana_3a4 = round(np.median(tiempos_3a4), 0) if tiempos_3a4 else 0
        
        # Tasas de conversión (usando ventanas del config)
        pct_conv = self._calculate_conversion_rates(customers, total_clientes)
        
        return {
            "dimension": dimension_name,
            "value": dimension_value,
            "grouping_mode": self.grouping_mode,
            "conversion_mode": self.conversion_mode,
            "granularity": self.cohort_config.granularity.value,
            "found": True,
            "total_clientes": total_clientes,
            "total_ordenes": total_ordenes,
            "pedidos_promedio": pedidos_promedio,
            "pct_2da_compra": pct_2da,
            "pct_3ra_compra": pct_3ra,
            "pct_4ta_compra": pct_4ta,
            "abs_2da_compra": n_c2,
            "abs_3ra_compra": n_c3,
            "abs_4ta_compra": n_c4,
            "aov": aov,
            "ltv_promedio": ltv_promedio,
            "cac_promedio": cac_promedio,
            "ltv_cac_ratio": ltv_cac_ratio,
            "revenue_total": round(total_revenue, 2),
            "cp_total": round(total_cp, 2),
            "mediana_dias_1a2": mediana_1a2,
            "mediana_dias_2a3": mediana_2a3,
            "mediana_dias_3a4": mediana_3a4,
            **pct_conv
        }
    
    def query(self, category: str = None, subcategory: str = None, 
              brand: str = None, product: str = None,
              subcategory_brand: str = None) -> Dict[str, Any]:
        """
        Busca una dimensión específica.
        
        Ejemplos:
            query(category="Electrónica")
            query(subcategory="Laptops")
            query(brand="Samsung")
            query(product="Galaxy S21")
            query(subcategory_brand="Televisores | Samsung")
        """
        if category:
            customers = self._get_dimension_customers("category", category)
            result = self._calculate_metrics_for_customers(customers, "Categoria", category)
            result["conversion_mode"] = self.conversion_mode
            return result
        
        if subcategory:
            customers = self._get_dimension_customers("subcategory", subcategory)
            result = self._calculate_metrics_for_customers(customers, "Subcategoria", subcategory)
            result["conversion_mode"] = self.conversion_mode
            return result
        
        if brand:
            customers = self._get_dimension_customers("brand", brand)
            result = self._calculate_metrics_for_customers(customers, "Brand", brand)
            result["conversion_mode"] = self.conversion_mode
            return result
        
        if product:
            customers = self._get_dimension_customers("name", product)
            result = self._calculate_metrics_for_customers(customers, "Producto", product)
            result["conversion_mode"] = self.conversion_mode
            return result
        
        if subcategory_brand:
            customers = self._get_dimension_customers("subcategory_brand", subcategory_brand)
            result = self._calculate_metrics_for_customers(customers, "Subcategoria_Marca", subcategory_brand)
            result["conversion_mode"] = self.conversion_mode
            return result
        
        return {"error": "Debes especificar category, subcategory, brand, product o subcategory_brand"}
    
    def list_available_values(self, dimension: str) -> List[str]:
        """
        Lista todos los valores disponibles para una dimensión.
        
        Args:
            dimension: "category", "subcategory", "brand", "name", "subcategory_brand"
        """
        values = set()
        for customer in self.customers:
            if self.grouping_mode == self.GROUPING_ENTRY_BASED:
                orders = customer.get_orders_sorted()
                if orders:
                    if dimension == "subcategory_brand":
                        val = getattr(orders[0], "subcategory_brand", None)
                    else:
                        val = getattr(orders[0], dimension, None)
                    
                    if val and str(val).strip() not in ["", "N/A", "nan", "None"]:
                        values.add(str(val).strip())
            else:
                for order in customer.get_orders_sorted():
                    if dimension == "subcategory_brand":
                        val = getattr(order, "subcategory_brand", None)
                    else:
                        val = getattr(order, dimension, None)
                    
                    if val and str(val).strip() not in ["", "N/A", "nan", "None"]:
                        values.add(str(val).strip())
        
        return sorted(values)
    
    def _score_relevance(self, product_name: str, search_term: str) -> float:
        """
        Calcula qué tan relevante es un producto para el término de búsqueda.
        """
        search_lower = search_term.lower().strip()
        product_lower = product_name.lower().strip()
        
        if not search_lower:
            return 0
        
        score = 0
        
        # 1. Coincidencia exacta
        if product_lower == search_lower:
            score += 100
        
        # 2. Coincidencia al inicio del nombre
        elif product_lower.startswith(search_lower):
            score += 50
        
        # 3. Coincidencia de palabras completas
        product_words = product_lower.split()
        search_words = search_lower.split()
        
        matched_words = 0
        for sw in search_words:
            if sw in product_words:
                matched_words += 1
                score += 10
                if product_words and product_words[0] == sw:
                    score += 5
        
        # 4. Proporción de palabras coincidentes
        if search_words:
            word_match_ratio = matched_words / len(search_words)
            score += word_match_ratio * 20
        
        # 5. Coincidencia parcial
        if search_lower in product_lower:
            score += 1
        
        # 6. Bonus por longitud
        score += min(len(product_lower) * 0.01, 5)
        
        return round(score, 2)
    
    def _group_similar_products(self, products: List[str]) -> Dict[str, List[str]]:
        """Agrupa productos similares."""
        grouped = {}
        
        for product in products:
            base = product
            
            patterns = [
                r',\s*Color\s+\w+',
                r',\s*Colour\s+\w+',
                r',\s*Talla\s+\w+',
                r',\s*Size\s+\w+',
                r',\s*[\w]+\s+Incluido',
                r'\s*\([^)]*\)',
                r',\s*[A-Z][a-z]+$',
            ]
            
            for pattern in patterns:
                base = re.sub(pattern, '', base, flags=re.IGNORECASE)
            
            words = base.split()
            if len(words) > 7:
                base = ' '.join(words[:7])
            
            base = re.sub(r'\s+', ' ', base).strip().rstrip(',').strip()
            
            if base not in grouped:
                grouped[base] = []
            grouped[base].append(product)
        
        return grouped
    
    def _extract_variant_detail(self, variant: str, base_name: str) -> str:
        """Extrae la parte única de una variante."""
        detail = variant.replace(base_name, "").strip()
        
        if not detail:
            return variant[:40] + "..." if len(variant) > 40 else variant
        
        detail = detail.lstrip(',').lstrip('-').lstrip(';').strip()
        
        detail = re.sub(r'^Color\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Colour\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Talla\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Size\s+', '', detail, flags=re.IGNORECASE)
        
        if len(detail) > 35:
            detail = detail[:32] + "..."
        
        return detail if detail else "Estándar"
    
    def interactive_search(self, dimension: str = None):
        """
        Modo interactivo para buscar dimensiones.
        """
        mode_display = "Comportamental" if self.grouping_mode == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        conv_display = "Acumulativa" if self.conversion_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        
        if dimension is None:
            print("\n" + "=" * 60)
            print("      BUSCADOR DE DIMENSIONES LTV".center(60))
            print("=" * 60)
            print(f"\n⚙️ Modo de agrupación: {mode_display}")
            print(f"⚙️ Modo de conversión: {conv_display}")
            print(f"📅 Granularidad de cohortes: {self.cohort_config.granularity.value}")
            print("\n¿Qué quieres buscar?")
            print("1. 📂 Categoría")
            print("2. 📁 Subcategoría")
            print("3. 🏷️  Marca (plano - todas las compras)")
            print("4. 🎯 Producto")
            print("5. 🔗 Subcategoría + Marca (jerárquico)")
            print("c. 🔄 Cambiar modo de conversión")
            print("g. 🔄 Cambiar granularidad (temporal)")
            print("q. 🔙 Volver")
            print("=" * 60)
            
            option = input("\n👉 Selecciona una opción: ").strip().lower()
            
            if option == 'c':
                if self.conversion_mode == self.CONVERSION_CUMULATIVE:
                    self.conversion_mode = self.CONVERSION_INCREMENTAL
                    print("\n✅ Modo de conversión cambiado a: INCREMENTAL (distribución)")
                else:
                    self.conversion_mode = self.CONVERSION_CUMULATIVE
                    print("\n✅ Modo de conversión cambiado a: ACUMULATIVA (creciente)")
                input("\nPresiona Enter para continuar...")
                return self.interactive_search(dimension)
            
            if option == 'g':
                self._change_granularity()
                return self.interactive_search(dimension)
            
            dim_map = {
                '1': ('category', 'Categorías'),
                '2': ('subcategory', 'Subcategorías'),
                '3': ('brand', 'Marcas'),
                '4': ('name', 'Productos'),
                '5': ('subcategory_brand', 'Combinaciones (Subcategoría + Marca)')
            }
            
            if option not in dim_map:
                return
            
            dimension, display_name = dim_map[option]
        else:
            display_name = {
                'category': 'Categorías',
                'subcategory': 'Subcategorías',
                'brand': 'Marcas',
                'name': 'Productos',
                'subcategory_brand': 'Combinaciones (Subcategoría + Marca)'
            }.get(dimension, 'Valores')
        
        print(f"\n📋 {display_name} disponibles (modo: {mode_display}, granularidad: {self.cohort_config.granularity.value}):")
        
        try:
            terminal_width = os.get_terminal_size().columns
            display_width = min(terminal_width - 10, 120)
        except:
            display_width = 100
        
        print("-" * min(display_width, 100))
        
        values = self.list_available_values(dimension)
        
        if not values:
            print("⚠️ No hay datos disponibles para esta dimensión.")
            return
        
        # Agrupar productos
        if dimension == 'name':
            grouped = self._group_similar_products(values)
            display_values = []
            self._product_groups = {}
            
            for base_name, variants in grouped.items():
                if len(variants) == 1:
                    display_values.append(variants[0])
                    self._product_groups[variants[0]] = {'type': 'single', 'variants': variants}
                else:
                    display_name_group = f"{base_name} [{len(variants)} variantes]"
                    display_values.append(display_name_group)
                    self._product_groups[display_name_group] = {'type': 'group', 'variants': variants, 'base': base_name}
            
            values = display_values
        else:
            self._product_groups = None
        
        for i, val in enumerate(values[:30], 1):
            if len(val) > 80:
                display_val = val[:77] + "..."
            else:
                display_val = val
            print(f"   {i:2}. {display_val}")
        
        if len(values) > 30:
            print(f"   ... y {len(values) - 30} más")
        
        print("-" * min(display_width, 100))
        
        search_term = input("\n🔍 Escribe el nombre a buscar (o parte de él): ").strip()
        
        if not search_term:
            print("❌ Búsqueda cancelada.")
            return
        
        matches = [v for v in values if search_term.lower() in v.lower()]
        
        if not matches:
            print(f"❌ No se encontraron coincidencias para '{search_term}'")
            return
        
        if dimension == 'name':
            matches_with_scores = [(m, self._score_relevance(m, search_term)) for m in matches]
            matches_with_scores.sort(key=lambda x: x[1], reverse=True)
            matches = [m[0] for m in matches_with_scores]
            print(f"\n🔍 Se encontraron {len(matches)} coincidencias (ordenadas por relevancia):")
        else:
            print(f"\n🔍 Se encontraron {len(matches)} coincidencias:")
        
        for i, match in enumerate(matches[:15], 1):
            display_match = match[:70] + "..." if len(match) > 70 else match
            print(f"   {i:2}. {display_match}")
        
        if len(matches) > 15:
            print(f"   ... y {len(matches) - 15} más")
        
        selected = input("\n👉 Selecciona el número exacto o escribe el nombre: ").strip()
        
        if selected.isdigit() and 1 <= int(selected) <= len(matches):
            selected_value = matches[int(selected) - 1]
        else:
            filtered = [m for m in matches if selected.lower() in m.lower()]
            if len(filtered) == 1:
                selected_value = filtered[0]
            else:
                selected_value = selected
        
        # Manejar selección de productos con variantes
        if dimension == 'name' and self._product_groups and selected_value in self._product_groups:
            group_info = self._product_groups[selected_value]
            
            if group_info['type'] == 'group':
                variants = group_info['variants']
                base_name = group_info['base']
                
                print(f"\n🔍 Producto '{base_name}' tiene {len(variants)} variantes:")
                
                for i, variant in enumerate(variants[:10], 1):
                    variant_detail = self._extract_variant_detail(variant, base_name)
                    print(f"   {i:2}. {variant_detail}")
                
                if len(variants) > 10:
                    print(f"   ... y {len(variants) - 10} más")
                
                print("\n📊 Opciones:")
                print("   0. Ver TODAS las variantes agrupadas (recomendado)")
                print(f"   1-{len(variants)}. Seleccionar una variante específica")
                
                choice = input("\n👉 Selecciona una opción: ").strip()
                
                if choice == '0' or choice == '':
                    print(f"\n🔍 Buscando {len(variants)} variantes de '{base_name}'...")
                    
                    all_customers = []
                    for variant in variants:
                        customers = self._get_dimension_customers("name", variant)
                        all_customers.extend(customers)
                    
                    unique_customers = list({c.customer_id: c for c in all_customers}.values())
                    
                    result = self._calculate_metrics_for_customers(unique_customers, "Producto", base_name)
                    result["conversion_mode"] = self.conversion_mode
                    self._print_result(result)
                    return
                
                elif choice.isdigit() and 1 <= int(choice) <= len(variants):
                    selected_variant = variants[int(choice) - 1]
                    print(f"\n🔍 Buscando variante específica...")
                    result = self.query(product=selected_variant)
                    result["conversion_mode"] = self.conversion_mode
                    self._print_result(result)
                    return
                else:
                    print("❌ Opción inválida. Mostrando todas las variantes agrupadas.")
                    all_customers = []
                    for variant in variants:
                        customers = self._get_dimension_customers("name", variant)
                        all_customers.extend(customers)
                    unique_customers = list({c.customer_id: c for c in all_customers}.values())
                    result = self._calculate_metrics_for_customers(unique_customers, "Producto", base_name)
                    result["conversion_mode"] = self.conversion_mode
                    self._print_result(result)
                    return
            
            else:
                result = self.query(product=selected_value)
                result["conversion_mode"] = self.conversion_mode
                self._print_result(result)
                return
        
        # Búsqueda normal
        if dimension == 'category':
            result = self.query(category=selected_value)
        elif dimension == 'subcategory':
            result = self.query(subcategory=selected_value)
        elif dimension == 'brand':
            result = self.query(brand=selected_value)
        elif dimension == 'name':
            result = self.query(product=selected_value)
        elif dimension == 'subcategory_brand':
            result = self.query(subcategory_brand=selected_value)
        else:
            result = {"error": f"Dimensión {dimension} no soportada"}
        
        result["conversion_mode"] = self.conversion_mode
        self._print_result(result)
    
    def _change_granularity(self):
        """Permite cambiar la granularidad de cohortes interactivamente."""
        print("\n" + "=" * 50)
        print("   CAMBIAR GRANULARIDAD DE COHORTES".center(50))
        print("=" * 50)
        print(f"Granularidad actual: {self.cohort_config.granularity.value}")
        print("\nOpciones:")
        print("   1. Diaria (daily)")
        print("   2. Semanal (weekly)")
        print("   3. Mensual (monthly)")
        print("   4. Trimestral (quarterly) - DEFAULT")
        print("   5. Semestral (semiannual)")
        print("   6. Anual (yearly)")
        print("   q. Cancelar")
        
        option = input("\n👉 Opción: ").strip()
        
        granularity_map = {
            '1': TimeGranularity.DAILY,
            '2': TimeGranularity.WEEKLY,
            '3': TimeGranularity.MONTHLY,
            '4': TimeGranularity.QUARTERLY,
            '5': TimeGranularity.SEMIANNUAL,
            '6': TimeGranularity.YEARLY,
        }
        
        if option in granularity_map:
            new_granularity = granularity_map[option]
            self.cohort_config = CohortConfig(granularity=new_granularity)
            self.cohort_manager = CohortManager(self.cohort_config)
            print(f"✅ Granularidad cambiada a: {new_granularity.value}")
            
            # Recargar CAC con nueva granularidad
            cac_path = os.environ.get("LTV_CAC_PATH")
            self.cac_map = CACRepository.get_cac_mapping(cac_path, new_granularity.value)
            if self.cac_map:
                print(f"✅ CAC recargado: {len(self.cac_map)} cohortes")
            else:
                print(f"⚠️ No se pudo recargar CAC para granularidad {new_granularity.value}")
        else:
            print("❌ Cancelado")
    
    def _print_result(self, result: Dict[str, Any]):
        """Imprime los resultados de forma formateada."""
        if not result.get("found", False):
            print(f"\n❌ {result.get('error', 'No se encontraron resultados')}")
            return
        
        modo = "Comportamental" if result.get("grouping_mode") == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        
        conv_mode = result.get("conversion_mode", self.conversion_mode)
        conv_modo = "Acumulativa" if conv_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        
        if conv_mode == self.CONVERSION_INCREMENTAL:
            conv_note = "\n   📌 NOTA: Tasas INCREMENTALES (distribución - cada cliente cuenta una sola vez en la primera ventana que cumple)"
        else:
            conv_note = "\n   📌 NOTA: Tasas ACUMULATIVAS (crecientes - clientes cuentan en múltiples ventanas)"
        
        print("\n" + "=" * 70)
        print(f"📊 RESULTADOS PARA {result['dimension'].upper()}: {result['value']}".center(70))
        print(f"   (Agrupación: {modo} | Conversión: {conv_modo} | Granularidad: {result.get('granularity', 'quarterly')})".center(70))
        print(conv_note)
        print("=" * 70)
        
        ltv_cac_display = ""
        if result.get('ltv_cac_ratio', 0) > 0:
            ltv_cac_display = f"\n   • LTV/CAC Ratio:        {result['ltv_cac_ratio']:.2f}x"
        
        cac_display = ""
        if result.get('cac_promedio', 0) > 0:
            cac_display = f"\n   • CAC promedio:          ${result['cac_promedio']:,.2f}"
        
        print(f"""
    📈 MÉTRICAS GENERALES:
    • Clientes únicos:     {result['total_clientes']:,}
    • Total de órdenes:    {result['total_ordenes']:,}
    • Pedidos por cliente: {result['pedidos_promedio']}

    🔄 FRECUENCIA DE COMPRA:
    • 2da compra:          {result['abs_2da_compra']:,} ({result['pct_2da_compra']}%)
    • 3ra compra:          {result['abs_3ra_compra']:,} ({result['pct_3ra_compra']}%)
    • 4ta compra:          {result['abs_4ta_compra']:,} ({result['pct_4ta_compra']}%)

    ⏱️ TIEMPO ENTRE COMPRAS (mediana):
    • 1ra → 2da:           {int(result['mediana_dias_1a2'])} días
    • 2da → 3ra:           {int(result['mediana_dias_2a3'])} días
    • 3ra → 4ta:           {int(result['mediana_dias_3a4'])} días

    📊 TASAS DE CONVERSIÓN (2da compra):
    • 30 días:             {result.get('Pct_Conv_30d', 0)}%
    • 60 días:             {result.get('Pct_Conv_60d', 0)}%
    • 90 días:             {result.get('Pct_Conv_90d', 0)}%
    • 180 días:            {result.get('Pct_Conv_180d', 0)}%
    • 360 días:            {result.get('Pct_Conv_360d', 0)}%

    💰 VALOR ECONÓMICO:
    • AOV (Ticket promedio): ${result['aov']:,.2f}
    • LTV promedio:          ${result['ltv_promedio']:,.2f}
    • CAC promedio:          ${result['cac_promedio']:,.2f}
    • LTV/CAC ratio:          {result['ltv_cac_ratio']:,.2f}x
    • Revenue total:         ${result['revenue_total']:,.2f}
    • Contribution Profit:   ${result['cp_total']:,.2f}
    """)
        print("=" * 70)
    
    def quick_search(self, dimension: str, value: str) -> None:
        """Búsqueda rápida sin interactividad."""
        if dimension == 'category':
            result = self.query(category=value)
        elif dimension == 'subcategory':
            result = self.query(subcategory=value)
        elif dimension == 'brand':
            result = self.query(brand=value)
        elif dimension == 'product':
            result = self.query(product=value)
        elif dimension == 'subcategory_brand':
            result = self.query(subcategory_brand=value)
        else:
            print(f"❌ Dimensión '{dimension}' no soportada. Usa: category, subcategory, brand, product, subcategory_brand")
            return
        
        result["conversion_mode"] = self.conversion_mode
        self._print_result(result)
    
    def set_conversion_mode(self, mode: str):
        """Cambia el modo de conversión."""
        if mode in [self.CONVERSION_CUMULATIVE, self.CONVERSION_INCREMENTAL]:
            self.conversion_mode = mode
            print(f"✅ Modo de conversión cambiado a: {mode}")
        else:
            print(f"❌ Modo inválido. Usa 'cumulative' o 'incremental'")
    
    def set_granularity(self, granularity: str):
        """Cambia la granularidad de cohortes."""
        try:
            new_granularity = TimeGranularity.from_string(granularity)
            self.cohort_config = CohortConfig(granularity=new_granularity)
            self.cohort_manager = CohortManager(self.cohort_config)
            print(f"✅ Granularidad cambiada a: {new_granularity.value}")
            
            # Recargar CAC
            cac_path = os.environ.get("LTV_CAC_PATH")
            self.cac_map = CACRepository.get_cac_mapping(cac_path, new_granularity.value)
            if self.cac_map:
                print(f"✅ CAC recargado: {len(self.cac_map)} cohortes")
        except Exception as e:
            print(f"❌ Error cambiando granularidad: {e}")