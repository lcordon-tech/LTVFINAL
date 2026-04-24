# ============================================================================
# FILE: Run/Security/user_manager.py
# COMPLETO - CON NUEVA ESTRUCTURA
# ============================================================================
"""
Gestión de usuarios - VERSIÓN REDISEÑADA
Almacena DB creds globales y SSH por país
"""

import json
import bcrypt
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime


class UserManager:
    """Gestiona usuarios - AHORA con DB global y SSH por país."""
    
    _instance = None
    _config_path = Path(__file__).parent.parent.parent / "config" / "credentials.json"
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self):
        if not self._config_path.exists():
            self._create_default()
        
        with open(self._config_path, 'r', encoding='utf-8') as f:
            self._data = json.load(f)
        
        # Migrar formato antiguo si es necesario
        self._migrate_if_needed()
        
        # Asegurar estructura nueva
        if "users" not in self._data:
            self._data["users"] = {}
        if "db" not in self._data:
            self._data["db"] = {"user": "", "password": ""}
        if "countries" not in self._data:
            self._data["countries"] = {}
        
        self._save()
    
    def _migrate_if_needed(self):
        """Migra formato antiguo a nuevo."""
        # Verificar si hay usuarios en formato antiguo
        users = self._data.get("users", {})
        migration_needed = False
        
        for alias, user_data in users.items():
            # Si el usuario tiene 'countries' con credenciales DB, extraer DB global
            if 'countries' in user_data and user_data['countries']:
                # Tomar credenciales del primer país disponible
                for country, creds in user_data['countries'].items():
                    if 'db_user' in creds and creds['db_user']:
                        if not self._data.get('db', {}).get('user'):
                            self._data['db'] = {
                                'user': creds['db_user'],
                                'password': creds['db_password']
                            }
                            migration_needed = True
                        # Mover SSH a estructura de países
                        ssh = creds.get('ssh', '') or creds.get('ssh_cmd', '')
                        if ssh:
                            if 'countries' not in self._data:
                                self._data['countries'] = {}
                            if country not in self._data['countries']:
                                self._data['countries'][country] = {}
                            self._data['countries'][country]['ssh'] = ssh
                            migration_needed = True
                        
                        # Limpiar credenciales DB del país
                        user_data['countries'][country] = {}
        
        if migration_needed:
            print("🔧 Migración automática a nuevo formato completada")
    
    def _create_default(self):
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        default = {
            "db": {"user": "", "password": ""},
            "countries": {},
            "users": {}
        }
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(default, f, indent=2)
        self._data = default
    
    def _save(self):
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(self._data, f, indent=2)
    
    def _hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def _verify_password(self, password: str, hashed: str) -> bool:
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    
    def create_user(self, alias: str, password: str, db_user: str, db_password: str,
                    ssh_gt: str = "", ssh_cr: str = "") -> bool:
        """Crea nuevo usuario con DB global y SSH por país."""
        alias = alias.strip().lower()
        if alias in self._data["users"]:
            return False
        
        # Guardar DB creds globales
        self._data["db"]["user"] = db_user
        self._data["db"]["password"] = db_password
        
        # Guardar SSH por país
        if ssh_gt:
            if "GT" not in self._data["countries"]:
                self._data["countries"]["GT"] = {}
            self._data["countries"]["GT"]["ssh"] = ssh_gt
        if ssh_cr:
            if "CR" not in self._data["countries"]:
                self._data["countries"]["CR"] = {}
            self._data["countries"]["CR"]["ssh"] = ssh_cr
        
        # Crear usuario
        self._data["users"][alias] = {
            "alias": alias,
            "password_hash": self._hash_password(password),
            "created_at": datetime.now().isoformat()
        }
        self._save()
        return True
    
    def authenticate(self, alias: str, password: str) -> Optional[Dict]:
        """Autentica usuario, retorna datos si éxito."""
        alias = alias.strip().lower()
        user = self._data["users"].get(alias)
        if not user:
            return None
        
        if self._verify_password(password, user["password_hash"]):
            return user
        return None
    
    def get_db_credentials(self) -> Dict:
        """Retorna credenciales DB globales."""
        return self._data.get("db", {})
    
    def get_ssh_command(self, country_code: str) -> str:
        """Retorna comando SSH para un país."""
        countries = self._data.get("countries", {})
        country_data = countries.get(country_code, {})
        return country_data.get("ssh", "")
    
    def update_user_password(self, alias: str, new_password: str) -> bool:
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        self._data["users"][alias]["password_hash"] = self._hash_password(new_password)
        self._save()
        return True
    
    def update_ssh_command(self, country_code: str, ssh_command: str) -> bool:
        """Actualiza comando SSH para un país."""
        country_code = country_code.upper()
        if "countries" not in self._data:
            self._data["countries"] = {}
        if country_code not in self._data["countries"]:
            self._data["countries"][country_code] = {}
        self._data["countries"][country_code]["ssh"] = ssh_command
        self._save()
        return True
    
    def delete_user(self, alias: str) -> bool:
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        del self._data["users"][alias]
        self._save()
        return True
    
    def list_users(self) -> List[str]:
        return list(self._data["users"].keys())
    
    def user_exists(self, alias: str) -> bool:
        return alias.strip().lower() in self._data["users"]
    
    def get_db_user(self) -> str:
        return self._data.get("db", {}).get("user", "")
    
    def get_db_password(self) -> str:
        return self._data.get("db", {}).get("password", "")