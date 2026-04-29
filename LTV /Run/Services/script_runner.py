import subprocess
import sys
import os
import time
from typing import List, Dict, Optional


class ScriptRunner:
    def __init__(self):
        self.results = []
    
    def run_script(self, script_path: str, env_overrides: Optional[Dict[str, str]] = None) -> bool:
        if not os.path.exists(script_path):
            print(f"❌ Script no encontrado: {script_path}")
            return False
        
        folder = os.path.dirname(script_path)
        script_name = os.path.basename(script_path)
        
        # Obtener la raíz del proyecto (3 niveles arriba de Services)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        print(f"\n{'='*80}")
        print(f"🚀 EJECUTANDO: {script_name}")
        print(f"📂 UBICACIÓN: {folder}")
        print(f"📂 PROJECT ROOT: {project_root}")
        print(f"{'='*80}")
        
        start_time = time.time()
        
        env_vars = os.environ.copy()
        if env_overrides:
            for key, value in env_overrides.items():
                env_vars[key] = value
                print(f"   🔧 {key} = {value}")
        
        # Agregar project_root al PYTHONPATH
        pythonpath = env_vars.get("PYTHONPATH", "")
        if pythonpath:
            env_vars["PYTHONPATH"] = f"{project_root};{pythonpath}"
        else:
            env_vars["PYTHONPATH"] = project_root
        
        try:
            subprocess.run(
                [sys.executable, script_name], 
                cwd=folder, 
                env=env_vars, 
                check=True
            )
            duration = time.time() - start_time
            print(f"✅ FINALIZADO: {script_name} ({duration:.2f}s)")
            self.results.append({"script": script_name, "success": True, "duration": duration})
            return True
        except Exception as e:
            print(f"❌ ERROR: {script_name} falló: {e}")
            self.results.append({"script": script_name, "success": False, "error": str(e)})
            return False
    
    def run_scripts(self, scripts: List[str], env_overrides_by_script: Optional[Dict[str, Dict[str, str]]] = None) -> bool:
        for script_path in scripts:
            script_name = os.path.basename(script_path)
            env_overrides = None
            if env_overrides_by_script and script_name in env_overrides_by_script:
                env_overrides = env_overrides_by_script[script_name]
            if not self.run_script(script_path, env_overrides):
                return False
        return True
    
    def get_summary(self) -> str:
        total = len(self.results)
        success = sum(1 for r in self.results if r["success"])
        return f"📊 Resumen: {success}/{total} scripts ejecutados correctamente"