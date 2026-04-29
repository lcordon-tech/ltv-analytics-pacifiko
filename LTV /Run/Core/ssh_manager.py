# Run/Core/ssh_manager.py
"""
Gestor centralizado de SSH lifecycle.
"""

import subprocess
import time
import sys
from typing import Optional

from Run.Config.credentials import Credentials
from Run.Utils.logger import SystemLogger


class SSHManager:
    """Gestiona el ciclo de vida del túnel SSH."""
    
    _instance = None
    _process: Optional[subprocess.Popen] = None
    _is_active = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self.logger = SystemLogger()
    
    def is_active(self) -> bool:
        """Verifica si el túnel SSH está activo."""
        if self._process is None:
            return False
        return self._process.poll() is None
    
    def start(self) -> bool:
        """Inicia el túnel SSH."""
        if self.is_active():
            print("✅ Túnel SSH ya está activo")
            return True
        
        ssh_creds = Credentials.get_ssh_credentials()
        ssh_cmd = ssh_creds.get_command()
        
        if not ssh_cmd or ssh_cmd == "xxx" or ssh_cmd.strip() == "":
            print("ℹ️ No hay túnel SSH configurado. Modo local.")
            return True
        
        print("\n" + " 🔐 ESTABLECIENDO CONEXIÓN SEGURA (SSH) ".center(70, "-"))
        
        try:
            if sys.platform == "win32":
                self._process = subprocess.Popen(
                    ssh_cmd,
                    shell=True,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )
            else:
                self._process = subprocess.Popen(
                    ssh_cmd,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            
            print("⏳ Esperando establecimiento del túnel (5 segundos)...")
            for i in range(5):
                time.sleep(1)
                print(f"   ...{i+1}/5")
            
            if self._process.poll() is not None:
                print("❌ El túnel SSH se cerró inmediatamente.")
                self._is_active = False
                return False
            
            self._is_active = True
            print("✅ Túnel SSH activo")
            return True
            
        except Exception as e:
            print(f"❌ Error al abrir túnel SSH: {e}")
            self._is_active = False
            return False
    
    def stop(self):
        """Cierra el túnel SSH."""
        if self._process:
            print("\n🔒 Cerrando túnel SSH...")
            try:
                self._process.terminate()
                self._process.wait(timeout=3)
                print("✅ Túnel SSH cerrado")
            except Exception as e:
                print(f"⚠️ Error al cerrar túnel: {e}")
                try:
                    self._process.kill()
                except:
                    pass
            finally:
                self._process = None
                self._is_active = False
    
    def ensure_runtime_environment(self) -> bool:
        """
        Garantiza que el entorno runtime esté listo.
        - SSH activo si está configurado
        """
        print("\n" + "🔧 VERIFICANDO ENTORNO RUNTIME".center(60, "-"))
        
        if not self.start():
            print("⚠️ No se pudo establecer SSH. Continuando en modo local...")
        
        print("-" * 60)
        return True