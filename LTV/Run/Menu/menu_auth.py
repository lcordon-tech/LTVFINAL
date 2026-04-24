# ============================================================================
# FILE: Run/Menu/menu_auth.py
# COMPLETO - CON NUEVO FLUJO DE CREACIÓN DE USUARIO
# ============================================================================
"""
Autenticación y gestión de usuarios - VERSIÓN REDISEÑADA
"""

import getpass
import json
from pathlib import Path
from typing import Optional

from Run.Security.auth_service import AuthService
from Run.Utils.logger import SystemLogger


class MenuAuth:
    """Menú de autenticación y gestión de usuarios - REDISEÑADO."""
    
    BACK_TO_AUTH = "BACK_TO_AUTH"
    EXIT_SYSTEM = "EXIT"
    
    def __init__(self, logger: SystemLogger = None):
        self.auth = AuthService()
        self.logger = logger or SystemLogger()
        self._current_country: Optional[str] = None
    
    def set_country(self, country_code: str):
        self._current_country = country_code.upper()
        self.auth.set_country(country_code)
    
    def authenticate_standalone(self) -> bool:
        """Flujo de autenticación standalone."""
        while True:
            print("\n" + "=" * 50)
            print("   SISTEMA LTV - AUTENTICACIÓN".center(50))
            print("=" * 50)
            print("1. 🔐 Login")
            print("2. 👤 Crear usuario")
            print("3. ✏️ Editar usuario")
            print("4. 🗑️ Eliminar usuario")
            print("5. 📋 Listar usuarios")
            print("q. ❌ Salir")
            print("=" * 50)
            
            option = input("\n👉 Opción: ").strip().lower()
            
            if option == '1':
                if self._menu_login():
                    return True
            elif option == '2':
                self._menu_create_user()
            elif option == '3':
                self._menu_edit_user()
            elif option == '4':
                self._menu_delete_user()
            elif option == '5':
                self._menu_list_users()
            elif option == 'q':
                return False
            else:
                print("❌ Opción inválida")
    
    def authenticate(self, country_code: str = None) -> bool:
        """Método legacy para compatibilidad."""
        self.set_country(country_code or "GT")
        
        if self.auth.is_authenticated:
            print(f"✅ Sesión activa: {self.auth.get_current_user()}")
            return True
        
        return self.authenticate_standalone()
    
    def get_db_credentials_for_country(self, country_code: str) -> Optional[dict]:
        """Obtiene credenciales DB para un país."""
        return self.auth.get_db_credentials(country_code)
    
    def _menu_login(self) -> bool:
        print("\n" + "=" * 50)
        print("       LOGIN".center(50))
        print("=" * 50)
        
        alias = input("Alias: ").strip()
        print("Password: [hidden]", end=" ")
        password = getpass.getpass("")
        
        if self.auth.authenticate(alias, password):
            print(f"\n✅ Bienvenido, {alias}")
            if self.logger:
                self.logger.info(f"Login exitoso: {alias}")
            return True
        
        print("\n❌ Alias o contraseña incorrectos")
        return False
    
    def _menu_create_user(self) -> bool:
        print("\n" + "=" * 50)
        print("   CREAR NUEVO USUARIO".center(50))
        print("=" * 50)
        
        alias = input("Alias: ").strip().lower()
        if self.auth.user_exists(alias):
            print("❌ El alias ya existe")
            return False
        
        password = getpass.getpass("Contraseña: ")
        confirm = getpass.getpass("Confirmar contraseña: ")
        
        if password != confirm:
            print("❌ Las contraseñas no coinciden")
            return False
        
        # 🔧 CREDENCIALES DB GLOBALES (SOLO UNA VEZ)
        print("\n" + "=" * 40)
        print("   CREDENCIALES DE BASE DE DATOS".center(40))
        print("=" * 40)
        print("Estas credenciales serán usadas para TODOS los países.")
        
        db_user = input("Usuario DB: ").strip()
        db_password = getpass.getpass("Contraseña DB: ")
        
        if not db_user or not db_password:
            print("❌ Usuario y contraseña DB son obligatorios")
            return False
        
        # 🔧 SSH POR PAÍS
        print("\n" + "=" * 40)
        print("   CONFIGURACIÓN SSH POR PAÍS".center(40))
        print("=" * 40)
        print("(Deja en blanco si no se necesita túnel SSH)")
        
        ssh_gt = input("SSH command para GT []: ").strip()
        ssh_cr = input("SSH command para CR []: ").strip()
        
        # Crear usuario
        if self.auth.create_user(alias, password, db_user, db_password, ssh_gt, ssh_cr):
            print(f"\n✅ Usuario '{alias}' creado exitosamente")
            print(f"   DB User: {db_user}")
            print(f"   SSH GT: {'configurado' if ssh_gt else 'no configurado'}")
            print(f"   SSH CR: {'configurado' if ssh_cr else 'no configurado'}")
            if self.logger:
                self.logger.info(f"Usuario creado: {alias}")
            return True
        
        print("❌ Error al crear usuario")
        return False
    
    def _menu_edit_user(self) -> bool:
        users = self.auth.list_users()
        if not users:
            print("❌ No hay usuarios registrados")
            return False
        
        print("\n" + "=" * 50)
        print("   EDITAR USUARIO".center(50))
        print("=" * 50)
        
        for i, u in enumerate(users, 1):
            print(f"   {i}. {u}")
        
        try:
            idx = int(input("\n👉 Selecciona usuario (número): ")) - 1
            if idx < 0 or idx >= len(users):
                raise ValueError
            alias = users[idx]
        except:
            print("❌ Selección inválida")
            return False
        
        print(f"\n✏️ Editando: {alias}")
        print("   1. Cambiar contraseña")
        print("   2. Editar SSH por país")
        print("   b. 🔙 Volver")
        
        opt = input("\n👉 Opción: ").strip().lower()
        
        if opt == '1':
            new_pass = getpass.getpass("Nueva contraseña: ")
            confirm = getpass.getpass("Confirmar: ")
            if new_pass != confirm:
                print("❌ No coinciden")
                return False
            if self.auth.update_user_password(alias, new_pass):
                print("✅ Contraseña actualizada")
                return True
        
        elif opt == '2':
            print("\n📌 Editar SSH por país:")
            ssh_gt = input(f"   SSH GT [{self.auth._user_mgr.get_ssh_command('GT')}]: ").strip()
            ssh_cr = input(f"   SSH CR [{self.auth._user_mgr.get_ssh_command('CR')}]: ").strip()
            
            if ssh_gt:
                self.auth.update_ssh_command("GT", ssh_gt)
            if ssh_cr:
                self.auth.update_ssh_command("CR", ssh_cr)
            print("✅ SSH actualizado")
            return True
        elif opt == 'b':
            return False
        
        return False
    
    def _menu_delete_user(self) -> bool:
        users = self.auth.list_users()
        if not users:
            print("❌ No hay usuarios")
            return False
        
        print("\n" + "=" * 50)
        print("   ELIMINAR USUARIO".center(50))
        print("=" * 50)
        
        for i, u in enumerate(users, 1):
            print(f"   {i}. {u}")
        
        try:
            idx = int(input("\n👉 Selecciona usuario (número): ")) - 1
            alias = users[idx]
        except:
            print("❌ Selección inválida")
            return False
        
        confirm = input(f"⚠️ ¿Eliminar permanentemente a '{alias}'? (s/n): ").strip().lower()
        if confirm in ['s', 'si', 'sí', 'yes', 'y']:
            if self.auth.delete_user(alias):
                print(f"✅ Usuario '{alias}' eliminado")
                return True
        return False
    
    def _menu_list_users(self):
        users = self.auth.list_users()
        if not users:
            print("❌ No hay usuarios registrados")
            return
        
        print("\n" + "=" * 50)
        print("   USUARIOS REGISTRADOS".center(50))
        print("=" * 50)
        for u in users:
            print(f"   • {u}")
        
        db_user = self.auth.get_db_user()
        if db_user:
            print(f"\n💾 DB User global: {db_user}")
        
        print("\n🔧 SSH configurado:")
        ssh_gt = self.auth._user_mgr.get_ssh_command('GT')
        ssh_cr = self.auth._user_mgr.get_ssh_command('CR')
        print(f"   GT: {'configurado' if ssh_gt else 'no configurado'}")
        print(f"   CR: {'configurado' if ssh_cr else 'no configurado'}")
    
    def get_current_db_credentials(self):
        return self.auth.get_current_db_credentials()
    
    def has_valid_credentials(self, country_code: str = None) -> bool:
        return self.auth.get_db_credentials(country_code) is not None