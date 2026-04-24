"""
Sistema de tipo de cambio (FX) multi-país.
Con validación de hojas y manejo de errores mejorado.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List
from Run.Country.country_context import CountryContext


class FXEngine:
    """Gestiona tipos de cambio por país y cohorte."""
    
    def __init__(self, country_context: CountryContext, fx_path: Path):
        """
        Args:
            country_context: Contexto del país
            fx_path: Ruta al archivo TIPO_DE_CAMBIO.xlsx
        """
        self.context = country_context
        self.fx_path = fx_path
        self._rates: Dict[str, float] = {}
        self._available_sheets: List[str] = []
        self._load_rates()
    
    def _load_rates(self):
        """Carga tipos de cambio desde Excel con validación robusta y logging detallado."""
        
        print(f"\n📂 [FXEngine] Cargando tipos de cambio para {self.context.code}")
        print(f"   Archivo: {self.fx_path}")
        
        if not self.fx_path.exists():
            print(f"   ❌ Archivo FX no encontrado: {self.fx_path}")
            print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            return
        
        print(f"   ✅ Archivo encontrado")
        
        try:
            # Obtener nombre de la hoja según país
            sheet_name = self.context.get_excel_sheet("fx")
            print(f"   🔍 Buscando hoja: '{sheet_name}'")
            
            # Validar que el archivo se pueda leer
            try:
                excel_file = pd.ExcelFile(self.fx_path)
                self._available_sheets = excel_file.sheet_names
                print(f"   📄 Hojas disponibles en el archivo: {self._available_sheets}")
            except Exception as e:
                print(f"   ❌ No se pudo leer el archivo FX: {e}")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            # Validar que la hoja exista
            if sheet_name not in self._available_sheets:
                print(f"   ❌ Hoja '{sheet_name}' NO encontrada en {self.fx_path}")
                print(f"   📄 Hojas disponibles: {self._available_sheets}")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            print(f"   ✅ Hoja '{sheet_name}' encontrada, cargando datos...")
            
            # Leer la hoja
            df = pd.read_excel(self.fx_path, sheet_name=sheet_name)
            print(f"   📊 Filas leídas: {len(df)}")
            
            if df.empty:
                print(f"   ⚠️ Hoja '{sheet_name}' está vacía")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            # Mostrar columnas encontradas
            print(f"   📋 Columnas encontradas: {list(df.columns)}")
            
            # Identificar columna de tasa
            rate_col = None
            possible_rate_cols = ['rate', 'rate_usd', 'rate_crc_usd', 'rate_gtq_usd', 
                                  'fx_rate', 'tipo_cambio', 'exchange_rate']
            
            for col in df.columns:
                col_lower = str(col).lower().strip()
                if col_lower in possible_rate_cols or 'rate' in col_lower:
                    rate_col = col
                    print(f"   🔍 Columna de tasa detectada: '{rate_col}'")
                    break
            
            if rate_col is None:
                # Si no encuentra, usar primera columna numérica después de 'cohort'
                for col in df.columns:
                    if col != 'cohort' and pd.api.types.is_numeric_dtype(df[col]):
                        rate_col = col
                        print(f"   🔍 Usando primera columna numérica como tasa: '{rate_col}'")
                        break
            
            if rate_col is None:
                print(f"   ❌ No se encontró columna de tasa en hoja '{sheet_name}'")
                print(f"   📋 Columnas disponibles: {list(df.columns)}")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            # Limpiar y cargar datos
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            rate_count = 0
            for _, row in df.iterrows():
                cohort = row['cohort']
                try:
                    rate = float(row[rate_col])
                    if rate > 0:
                        self._rates[cohort] = rate
                        rate_count += 1
                except (ValueError, TypeError):
                    continue
            
            if self._rates:
                print(f"   ✅ FXEngine: {rate_count} tasas cargadas para {self.context.code}")
                print(f"   📌 Hoja: '{sheet_name}' | Columna: '{rate_col}'")
                
                # Mostrar primeras 5 tasas como ejemplo
                sample = list(self._rates.items())[:5]
                for cohort, rate in sample:
                    print(f"      {cohort}: {rate:.4f}")
                if len(self._rates) > 5:
                    print(f"      ... y {len(self._rates) - 5} más")
            else:
                print(f"   ⚠️ No se cargaron tasas válidas desde hoja '{sheet_name}'")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            
        except Exception as e:
            print(f"   ❌ Error cargando FX: {e}")
            print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            import traceback
            traceback.print_exc()
    
    def get_rate(self, cohort: str, granularity: str = "quarterly") -> float:
        """
        Retorna tipo de cambio para una cohorte.
        
        Args:
            cohort: Identificador de cohorte (ej: 'Q1', '2024-01')
            granularity: Granularidad de cohorte
        
        Returns:
            Tasa de cambio (moneda local → USD)
        """
        cohort_clean = cohort.upper().strip()
        
        # 1. Buscar en tabla por cohorte exacta
        if cohort_clean in self._rates:
            return self._rates[cohort_clean]
        
        # 2. Para granularidad mensual/semanal, intentar buscar por mapeo
        if granularity != "quarterly":
            # Si la cohorte tiene formato YYYY-MM, intentar extraer trimestre
            if '-' in cohort_clean and len(cohort_clean) == 7:  # YYYY-MM
                year, month = cohort_clean.split('-')
                quarter = (int(month) - 1) // 3 + 1
                quarterly_cohort = f"{year}-Q{quarter}"
                if quarterly_cohort in self._rates:
                    return self._rates[quarterly_cohort]
        
        # 3. Si la cohorte es quarterly pero con formato diferente
        if cohort_clean.startswith("Q"):
            # Buscar cualquier cohorte que termine con este Q
            for existing_cohort in self._rates:
                if existing_cohort.endswith(cohort_clean):
                    return self._rates[existing_cohort]
        
        # 4. Usar tasa por defecto del país
        return self.context.default_fx_rate
    
    def get_rates_map(self, granularity: str = "quarterly") -> Dict[str, float]:
        """
        Retorna el mapa completo de tasas (ya cargado en memoria).
        """
        return self._rates.copy()
    
    def convert_to_usd(self, amount: float, cohort: str, granularity: str = "quarterly") -> float:
        """
        Convierte monto de moneda local a USD.
        
        Args:
            amount: Monto en moneda local
            cohort: Cohort identifier
            granularity: Granularidad
        
        Returns:
            Monto en USD
        """
        rate = self.get_rate(cohort, granularity)
        if rate <= 0:
            print(f"⚠️ Tasa inválida ({rate}) para cohorte {cohort}. Usando 1.0")
            return amount
        return amount / rate
    
    def convert_from_usd(self, amount: float, cohort: str, granularity: str = "quarterly") -> float:
        """
        Convierte monto de USD a moneda local.
        
        Args:
            amount: Monto en USD
            cohort: Cohort identifier
            granularity: Granularidad
        
        Returns:
            Monto en moneda local
        """
        rate = self.get_rate(cohort, granularity)
        if rate <= 0:
            print(f"⚠️ Tasa inválida ({rate}) para cohorte {cohort}. Usando 1.0")
            return amount
        return amount * rate
    
    def get_available_sheets(self) -> List[str]:
        """Retorna las hojas disponibles en el archivo FX."""
        return self._available_sheets.copy()
    
    def validate_coverage(self, cohorts: List[str]) -> Dict[str, List[str]]:
        """
        Valida qué cohortes tienen tasa definida vs cuáles no.
        
        Args:
            cohorts: Lista de cohortes a validar
        
        Returns:
            Dict con 'covered' y 'missing'
        """
        covered = []
        missing = []
        
        for cohort in cohorts:
            rate = self.get_rate(cohort)
            if rate != self.context.default_fx_rate or cohort in self._rates:
                covered.append(cohort)
            else:
                missing.append(cohort)
        
        return {
            'covered': covered,
            'missing': missing,
            'coverage_pct': round(len(covered) / len(cohorts) * 100, 2) if cohorts else 0
        }
    
    def print_summary(self):
        """Imprime resumen completo del FXEngine."""
        print("\n" + "=" * 50)
        print(f" FX ENGINE SUMMARY - {self.context.code} ".center(50))
        print("=" * 50)
        print(f"📁 Archivo: {self.fx_path.name if self.fx_path else 'No definido'}")
        print(f"🌎 País: {self.context.name} ({self.context.code})")
        print(f"💱 Tasa default: {self.context.default_fx_rate}")
        print(f"📊 Tasas cargadas: {len(self._rates)}")
        
        if self._rates:
            print(f"\n📋 Ejemplo de tasas:")
            sample = list(self._rates.items())[:5]
            for cohort, rate in sample:
                print(f"   {cohort}: {rate:.4f}")
            if len(self._rates) > 5:
                print(f"   ... y {len(self._rates) - 5} más")
        
        if self._available_sheets:
            print(f"\n📄 Hojas disponibles en {self.fx_path.name}: {self._available_sheets}")
        
        print("=" * 50)