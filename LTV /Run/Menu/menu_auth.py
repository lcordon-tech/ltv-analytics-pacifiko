# ============================================================================
# FILE: Run/Menu/menu_auth.py
# COMPLETO - CON DEV MODE + RECUPERACIÓN + PASSWORD CON STARS
# ============================================================================
"""
Autenticación y gestión de usuarios - VERSIÓN MEJORADA
Soporta modo desarrollador, recuperación de contraseña y password con asteriscos.
"""

import getpass
import sys
import msvcrt  # Windows
import os
from pathlib import Path
from typing import Optional, Dict

from Run.Security.auth_service import AuthService
from Run.Config.dev_mode_manager import DevModeManager
from Run.Utils.logger import SystemLogger


def getpass_with_stars(prompt: str = "Password: ") -> str:
    """
    Lee contraseña mostrando asteriscos (*) por cada carácter.
    Funciona en Windows y Unix.
    """
    import sys
    password = ""
    
    print(prompt, end='', flush=True)
    
    if sys.platform == "win32":
        # Windows
        while True:
            ch = msvcrt.getch()
            if ch == b'\r' or ch == b'\n':
                print()
                break
            elif ch == b'\x08':  # Backspace
                if password:
                    password = password[:-1]
                    sys.stdout.write('\b \b')
                    sys.stdout.flush()
            else:
                try:
                    char = ch.decode('utf-8')
                    password += char
                    sys.stdout.write('*')
                    sys.stdout.flush()
                except UnicodeDecodeError:
                    pass
    else:
        # Unix - usar getpass pero con asteriscos simulados
        import termios
        import tty
        
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            while True:
                ch = sys.stdin.read(1)
                if ch == '\r' or ch == '\n':
                    print()
                    break
                elif ch == '\x7f' or ch == '\b':
                    if password:
                        password = password[:-1]
                        sys.stdout.write('\b \b')
                        sys.stdout.flush()
                else:
                    password += ch
                    sys.stdout.write('*')
                    sys.stdout.flush()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    
    return password


class MenuAuth:
    """Menú de autenticación y gestión de usuarios - CON DEV MODE y RECUPERACIÓN."""
    
    BACK_TO_AUTH = "BACK_TO_AUTH"
    EXIT_SYSTEM = "EXIT"
    
    # Países soportados
    SUPPORTED_COUNTRIES = ['GT', 'CR']
    
    def __init__(self, logger: SystemLogger = None):
        self.auth = AuthService()
        self.dev_mode = DevModeManager()
        self.logger = logger or SystemLogger()
        self._current_country: Optional[str] = None
    
    def set_country(self, country_code: str):
        self._current_country = country_code.upper()
        self.auth.set_country(country_code)
    
    def _auto_login_first_user(self) -> bool:
        """Dev mode: autologin con el primer usuario existente."""
        users = self.auth.list_users()
        if not users:
            print("\n⚠️ No hay usuarios registrados. Modo desarrollador no puede auto-login.")
            print("   Crea un usuario primero o desactiva el modo desarrollador.")
            return False
        
        first_user = users[0]
        print(f"\n🔓 MODO DESARROLLADOR ACTIVO")
        print(f"   Auto-login como: {first_user}")
        
        # Simular autenticación (sin password)
        # Nota: No podemos llamar a authenticate porque requiere password
        # Enviamos una señal al AuthService para que considere autenticado
        # Esto es un bypass controlado
        
        # Crear sesión simulada
        self.auth._current_user = first_user
        self.auth._session_active = True
        self.auth._user_mgr = self.auth._user_mgr  # Ya existe
        
        print(f"✅ Bienvenido, {first_user} (modo desarrollador)")
        if self.logger:
            self.logger.info(f"Auto-login (dev mode): {first_user}")
        return True
    
    def authenticate_standalone(self) -> bool:
        """Flujo de autenticación standalone con dev mode."""
        
        # 🔧 Si ya hay sesión activa, mostrar menú de gestión
        if self.auth.is_authenticated:
            print(f"\n✅ Sesión activa: {self.auth.get_current_user()}")
            print("\n¿Qué deseas hacer?")
            print("   1. 🔐 Continuar con sesión actual")
            print("   2. 🔧 Configurar modo desarrollador")
            print("   3. 🚪 Cerrar sesión")
            
            option = input("\n👉 Opción: ").strip().lower()
            
            if option == '1':
                return True
            elif option == '2':
                self._menu_toggle_dev_mode()
                # Después de cambiar dev mode, volver a evaluar
                return self.authenticate_standalone()
            elif option == '3':
                self.auth.logout()
                print("✅ Sesión cerrada")
                # Recursión para volver a login
                return self.authenticate_standalone()
            else:
                print("❌ Opción inválida. Continuando con sesión actual...")
                return True
        
        # 🔧 DEV MODE: Si está activo y NO hay sesión, auto-login
        if self.dev_mode.is_enabled() and self.auth.list_users():
            return self._auto_login_first_user()
        
        # Resto del menú normal...
        while True:
            print("\n" + "=" * 50)
            print("   SISTEMA LTV - AUTENTICACIÓN".center(50))
            print("=" * 50)
            print("1. 🔐 Login")
            print("2. 👤 Crear usuario")
            print("3. 🔑 Recuperar contraseña")
            print("4. ✏️ Editar usuario")
            print("5. 🗑️ Eliminar usuario")
            print("6. 📋 Listar usuarios")
            print("7. 🔧 Modo desarrollador")
            print("q. ❌ Salir")
            print("=" * 50)
            
            option = input("\n👉 Opción: ").strip().lower()
            
            if option == '1':
                if self._menu_login():
                    return True
            elif option == '2':
                self._menu_create_user()
            elif option == '3':
                self._menu_recover_password()
            elif option == '4':
                self._menu_edit_user()
            elif option == '5':
                self._menu_delete_user()
            elif option == '6':
                self._menu_list_users()
            elif option == '7':
                self._menu_toggle_dev_mode()
            elif option == 'q':
                return False
            else:
                print("❌ Opción inválida")
    
    def _menu_toggle_dev_mode(self):
        """Alterna el modo desarrollador."""
        # 🔧 Restricción: Solo se puede alternar si hay usuarios
        users = self.auth.list_users()
        if not users:
            print("\n⚠️ No hay usuarios registrados.")
            print("   Crea un usuario primero antes de activar modo desarrollador.")
            return
        
        current = self.dev_mode.is_enabled()
        new_state = not current
        
        print("\n" + "=" * 50)
        print("   MODO DESARROLLADOR".center(50))
        print("=" * 50)
        print(f"Estado actual: {'ACTIVADO' if current else 'DESACTIVADO'}")
        print(f"\n⚠️ Modo desarrollador permite auto-login sin contraseña")
        print(f"   Solo afecta la pantalla de autenticación, no otras validaciones.")
        
        confirm = input(f"\n👉 ¿{'DESACTIVAR' if current else 'ACTIVAR'} modo desarrollador? (s/n): ").strip().lower()
        if confirm in ['s', 'si', 'sí', 'yes', 'y']:
            self.dev_mode.set_enabled(new_state)
            print(f"✅ Modo desarrollador {'ACTIVADO' if new_state else 'DESACTIVADO'}")
            if self.logger:
                self.logger.info(f"Dev mode {'enabled' if new_state else 'disabled'}")
        else:
            print("❌ Cancelado")
    
    def _menu_recover_password(self):
        """Recuperación de contraseña validando credenciales DB."""
        print("\n" + "=" * 50)
        print("   RECUPERAR CONTRASEÑA".center(50))
        print("=" * 50)
        print("Para recuperar tu contraseña, necesitas validar tus credenciales de base de datos.")
        print("-" * 50)
        
        alias = input("Alias: ").strip().lower()
        
        if not self.auth.user_exists(alias):
            print("❌ El alias no existe")
            return
        
        # Seleccionar país
        print("\n📌 País para validación:")
        for i, country in enumerate(self.SUPPORTED_COUNTRIES, 1):
            print(f"   {i}. {country}")
        
        country_choice = input("\n👉 Selecciona país (número): ").strip()
        try:
            idx = int(country_choice) - 1
            if idx < 0 or idx >= len(self.SUPPORTED_COUNTRIES):
                raise ValueError
            country = self.SUPPORTED_COUNTRIES[idx]
        except:
            print("❌ País inválido")
            return
        
        # Obtener credenciales DB almacenadas para ese usuario y país
        stored_creds = self.auth.get_db_credentials_for_user(alias, country)
        
        if not stored_creds or not stored_creds.get('user'):
            print(f"❌ No hay credenciales DB configuradas para {alias} en {country}")
            print("   Contacta al administrador para recuperar tu cuenta.")
            return
        
        print(f"\n🔐 Valida tus credenciales DB para {country}:")
        print(f"   Usuario DB almacenado: {stored_creds.get('user', 'N/A')[:3]}...")
        
        db_user = input("Usuario DB: ").strip()
        print("Contraseña DB: ", end="")
        db_password = getpass_with_stars("")
        
        # Validar coincidencia
        if db_user != stored_creds.get('user', '') or db_password != stored_creds.get('password', ''):
            print("\n❌ Credenciales DB incorrectas. No se puede recuperar la contraseña.")
            if self.logger:
                self.logger.warning(f"Recuperación fallida para {alias} ({country})")
            return
        
        print("\n✅ Credenciales DB validadas correctamente.")
        
        # Nueva contraseña
        new_password = getpass_with_stars("\nNueva contraseña: ")
        confirm = getpass_with_stars("Confirmar contraseña: ")
        
        if new_password != confirm:
            print("❌ Las contraseñas no coinciden")
            return
        
        if len(new_password) < 4:
            print("❌ La contraseña debe tener al menos 4 caracteres")
            return
        
        if self.auth.update_user_password(alias, new_password):
            print("\n✅ Contraseña actualizada correctamente")
            if self.logger:
                self.logger.info(f"Contraseña recuperada para {alias}")
        else:
            print("\n❌ Error al actualizar la contraseña")
    
    def _menu_login(self) -> bool:
        print("\n" + "=" * 50)
        print("       LOGIN".center(50))
        print("=" * 50)
        
        alias = input("Alias: ").strip()
        print("Password: ", end="")
        password = getpass_with_stars("")
        
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
        
        print("Contraseña: ", end="")
        password = getpass_with_stars("")
        print("Confirmar contraseña: ", end="")
        confirm = getpass_with_stars("")
        
        if password != confirm:
            print("❌ Las contraseñas no coinciden")
            return False
        
        if len(password) < 4:
            print("❌ La contraseña debe tener al menos 4 caracteres")
            return False
        
        # 🔧 CREDENCIALES DB POR PAÍS
        print("\n" + "=" * 50)
        print("   CREDENCIALES DE BASE DE DATOS POR PAÍS".center(50))
        print("=" * 50)
        print("(Pueden ser diferentes para GT y CR)")
        
        db_credentials = {}
        
        for country in self.SUPPORTED_COUNTRIES:
            print(f"\n📌 PAÍS: {country}")
            print("-" * 30)
            db_user = input(f"   Usuario DB ({country}): ").strip()
            print(f"   Contraseña DB ({country}): ", end="")
            db_password = getpass_with_stars("")
            
            if not db_user or not db_password:
                print(f"   ⚠️ Credenciales para {country} omitidas")
                continue
            
            db_credentials[country] = {
                'user': db_user,
                'password': db_password
            }
        
        if not db_credentials:
            print("❌ Debes configurar al menos un país con credenciales DB")
            return False
        
        # 🔧 SSH POR PAÍS
        print("\n" + "=" * 50)
        print("   CONFIGURACIÓN SSH POR PAÍS".center(50))
        print("=" * 50)
        print("(Deja en blanco si no se necesita túnel SSH)")
        
        ssh_commands = {}
        for country in self.SUPPORTED_COUNTRIES:
            ssh_cmd = input(f"   SSH command para {country} []: ").strip()
            if ssh_cmd:
                ssh_commands[country] = ssh_cmd
        
        # Crear usuario con credenciales por país
        if self.auth.create_user_with_country_creds(alias, password, db_credentials, ssh_commands):
            print(f"\n✅ Usuario '{alias}' creado exitosamente")
            print("\n📋 CREDENCIALES CONFIGURADAS:")
            for country, creds in db_credentials.items():
                print(f"   {country}: DB User={creds['user']}, SSH={'✓' if ssh_commands.get(country) else '✗'}")
            if self.logger:
                self.logger.info(f"Usuario creado con credenciales por país: {alias}")
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
        print("   2. Editar credenciales DB por país")
        print("   3. Editar SSH por país")
        print("   b. 🔙 Volver")
        
        opt = input("\n👉 Opción: ").strip().lower()
        
        if opt == '1':
            print("Nueva contraseña: ", end="")
            new_pass = getpass_with_stars("")
            print("Confirmar: ", end="")
            confirm = getpass_with_stars("")
            if new_pass != confirm:
                print("❌ No coinciden")
                return False
            if len(new_pass) < 4:
                print("❌ La contraseña debe tener al menos 4 caracteres")
                return False
            if self.auth.update_user_password(alias, new_pass):
                print("✅ Contraseña actualizada")
                return True
        
        elif opt == '2':
            print("\n📌 EDITAR CREDENCIALES DB POR PAÍS:")
            print("   (Deja en blanco para mantener valor actual)")
            
            for country in self.SUPPORTED_COUNTRIES:
                current = self.auth.get_db_credentials_for_user(alias, country)
                current_user = current.get('user', '') if current else ''
                
                print(f"\n   📍 {country}:")
                print(f"      DB User actual: {current_user}")
                
                new_user = input(f"      Nuevo usuario DB ({country}) [Enter para mantener]: ").strip()
                if new_user:
                    print(f"      Nueva contraseña DB ({country}): ", end="")
                    new_pass = getpass_with_stars("")
                    self.auth.update_db_credentials(alias, country, new_user, new_pass)
                    print(f"      ✅ Credenciales DB para {country} actualizadas")
                else:
                    print(f"      ⏭️ Manteniendo credenciales actuales para {country}")
            
            return True
        
        elif opt == '3':
            print("\n📌 EDITAR SSH POR PAÍS:")
            print("   (Deja en blanco para mantener valor actual)")
            
            for country in self.SUPPORTED_COUNTRIES:
                current = self.auth.get_ssh_command(alias, country)
                print(f"   SSH {country} actual: {current if current else 'no configurado'}")
                new_cmd = input(f"   Nueva SSH para {country} []: ").strip()
                if new_cmd:
                    self.auth.update_ssh_command_for_user(alias, country, new_cmd)
                    print(f"   ✅ SSH para {country} actualizado")
            
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
        
        print("\n" + "=" * 60)
        print("   USUARIOS REGISTRADOS".center(60))
        print("=" * 60)
        
        dev_mode_status = "ACTIVADO" if self.dev_mode.is_enabled() else "DESACTIVADO"
        print(f"\n🔧 Modo desarrollador: {dev_mode_status}")
        
        for u in users:
            print(f"\n👤 {u}")
            print("   📋 Credenciales DB:")
            for country in self.SUPPORTED_COUNTRIES:
                db_creds = self.auth.get_db_credentials_for_user(u, country)
                if db_creds and db_creds.get('user'):
                    print(f"      {country}: user={db_creds.get('user', 'N/A')}")
                else:
                    print(f"      {country}: NO CONFIGURADO")
            
            print("   🔧 SSH:")
            for country in self.SUPPORTED_COUNTRIES:
                ssh_cmd = self.auth.get_ssh_command_for_user(u, country)
                print(f"      {country}: {'configurado' if ssh_cmd else 'no configurado'}")
        
        print("\n" + "=" * 60)
    
    def authenticate(self, country_code: str = None) -> bool:
        """Método legacy para compatibilidad."""
        self.set_country(country_code or "GT")
        
        if self.auth.is_authenticated:
            print(f"✅ Sesión activa: {self.auth.get_current_user()}")
            return True
        
        return self.authenticate_standalone()
    
    def get_current_db_credentials(self):
        """Retorna credenciales DB para el país actual."""
        return self.auth.get_db_credentials(self._current_country)
    
    def has_valid_credentials(self, country_code: str = None) -> bool:
        """Verifica si hay credenciales DB para un país."""
        target = country_code or self._current_country
        return self.auth.get_db_credentials(target) is not None
    
    def get_db_credentials_for_country(self, country_code: str) -> Optional[dict]:
        """Obtiene credenciales DB para un país específico."""
        return self.auth.get_db_credentials(country_code)