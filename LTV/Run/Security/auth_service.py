# ============================================================================
# FILE: Run/Security/auth_service.py
# COMPLETO - CON NUEVA ESTRUCTURA
# ============================================================================
"""
Servicio de autenticación unificado - VERSIÓN REDISEÑADA
"""

from typing import Optional, Dict
from pathlib import Path
import json
from .user_manager import UserManager
from Run.Config.dev_mode_manager import DevModeManager


class AuthService:
    """Unifica autenticación y proporciona credenciales finales."""
    
    _instance = None
    _current_user: Optional[str] = None
    _current_country: Optional[str] = None
    _session_active: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._user_mgr = UserManager()
            cls._instance._dev_mode = DevModeManager()
            cls._instance._ensure_credentials_file()
        return cls._instance
    
    def _ensure_credentials_file(self):
        """Asegura que credentials.json existe con estructura base."""
        creds_file = Path(__file__).parent.parent.parent / "config" / "credentials.json"
        if not creds_file.exists():
            creds_file.parent.mkdir(parents=True, exist_ok=True)
            default = {
                "db": {"user": "", "password": ""},
                "countries": {"GT": {"ssh": ""}, "CR": {"ssh": ""}},
                "users": {}
            }
            with open(creds_file, 'w', encoding='utf-8') as f:
                json.dump(default, f, indent=2)
    
    @property
    def is_authenticated(self) -> bool:
        return self._current_user is not None and self._session_active
    
    def get_current_user(self) -> Optional[str]:
        return self._current_user
    
    def set_country(self, country_code: str):
        self._current_country = country_code.upper()
    
    def authenticate(self, alias: str, password: str) -> bool:
        """Autentica usuario y mantiene sesión."""
        user = self._user_mgr.authenticate(alias, password)
        if user:
            self._current_user = alias
            self._session_active = True
            return True
        return False
    
    def logout(self):
        self._current_user = None
        self._session_active = False
    
    def get_db_credentials(self, country_code: str = None) -> Optional[Dict]:
        """Retorna credenciales DB + SSH para un país."""
        target_country = (country_code or self._current_country or "GT").upper()
        
        if not self.is_authenticated:
            print(f"⚠️ No autenticado")
            return None
        
        # Obtener DB creds globales
        db_creds = self._user_mgr.get_db_credentials()
        
        if not db_creds or not db_creds.get('user'):
            print(f"⚠️ No hay credenciales DB configuradas")
            return None
        
        # Obtener SSH específico del país
        ssh_command = self._user_mgr.get_ssh_command(target_country)
        
        # Construir host (esto debería venir de configuración por país)
        # Por ahora usamos localhost con puerto dinámico
        host_map = {"GT": "127.0.0.1:3336", "CR": "127.0.0.1:3337"}
        
        return {
            "host": host_map.get(target_country, "127.0.0.1:3306"),
            "database": f"db_{target_country.lower()}" if target_country == "GT" else "CRProdDb",
            "db_user": db_creds.get('user', ''),
            "db_password": db_creds.get('password', ''),
            "ssh_command": ssh_command
        }
    
    def get_current_db_credentials(self) -> Optional[Dict]:
        return self.get_db_credentials(self._current_country)
    
    def should_ask_login(self) -> bool:
        """Determina si debe pedir login."""
        if self.is_authenticated:
            return False
        
        if not self._dev_mode.is_enabled():
            return True
        
        users = self._user_mgr.list_users()
        if not users:
            return True
        
        return True
    
    def create_user(self, alias: str, password: str, db_user: str, db_password: str,
                    ssh_gt: str = "", ssh_cr: str = "") -> bool:
        """Crea usuario con DB global y SSH por país."""
        result = self._user_mgr.create_user(alias, password, db_user, db_password, ssh_gt, ssh_cr)
        if result:
            self._current_user = alias
            self._session_active = True
        return result
    
    def update_user_password(self, alias: str, new_password: str) -> bool:
        return self._user_mgr.update_user_password(alias, new_password)
    
    def update_ssh_command(self, country_code: str, ssh_command: str) -> bool:
        """Actualiza comando SSH para un país."""
        return self._user_mgr.update_ssh_command(country_code, ssh_command)
    
    def delete_user(self, alias: str) -> bool:
        if alias == self._current_user:
            self.logout()
        return self._user_mgr.delete_user(alias)
    
    def list_users(self) -> list:
        return self._user_mgr.list_users()
    
    def user_exists(self, alias: str) -> bool:
        return self._user_mgr.user_exists(alias)
    
    def get_db_user(self) -> str:
        return self._user_mgr.get_db_user()
    
    def get_db_password(self) -> str:
        return self._user_mgr.get_db_password()