import subprocess
import time
from typing import Optional

import sys
import os

# Ajustar path para imports
RUN_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RUN_PATH not in sys.path:
    sys.path.insert(0, RUN_PATH)

from Config.credentials import SSHCredentials


class SSHService:
    def __init__(self, credentials: SSHCredentials):
        self.credentials = credentials
        self.process: Optional[subprocess.Popen] = None
    
    def test_connection(self) -> bool:
        """Prueba la conexión SSH antes de usarla"""
        command = self.credentials.get_command()
        if not command:
            return True
        
        try:
            result = subprocess.run(
                command + ' "exit"',
                shell=True,
                timeout=5,
                capture_output=True
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def start(self) -> bool:
        command = self.credentials.get_command()
        if not command:
            print("\n🔓 SSH deshabilitado (modo local)")
            return True
        
        print("\n" + " 🔐 ESTABLECIENDO CONEXIÓN SEGURA (SSH) ".center(70, "-"))
        try:
            self.process = subprocess.Popen(command, shell=True)
            print(f"⏳ Esperando {self.credentials.wait_seconds} segundos...")
            time.sleep(self.credentials.wait_seconds)
            return True
        except Exception as e:
            print(f"❌ Error al abrir SSH: {e}")
            return False
    
    def stop(self):
        if self.process:
            self.process.terminate()
            self.process = None
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()