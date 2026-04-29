# ============================================================================
# FILE: Run/Config/paths.py
# COMPLETO - CON DATA_XLSX COMO ORIGEN ÚNICO
# ============================================================================
"""
Sistema de rutas multi-país con fallback automático.
AHORA: data_xlsx es el origen único para todos los archivos Excel y CSV.
"""

import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class PathsConfig:
    base_path: Path
    code_path: Path
    data_ltv: Path
    inputs_dir: Path
    results_base: Path
    country: str = "GT"
    sois_file: str = "SOIS.xlsx"
    supuestos_file: str = "SUPUESTOS.xlsx"
    catalogo_file: str = "catalogLTV.xlsx"
    cac_file: str = "CAC.xlsx"
    fx_file: str = "TIPO_DE_CAMBIO.xlsx"
    
    def __post_init__(self):
        for folder in [self.data_ltv, self.inputs_dir, self.results_base]:
            folder.mkdir(parents=True, exist_ok=True)
    
    def to_env_dict(self, current_run_folder: Optional[Path] = None) -> dict:
        env = {
            "LTV_PATH_CONTROL": str(self.data_ltv),
            "LTV_INPUT_DIR": str(self.inputs_dir),
            "LTV_SOIS_FILE": self.sois_file,
            "LTV_SUPUESTOS_FILE": self.supuestos_file,
            "LTV_CATALOGO_FILE": self.catalogo_file,
            "LTV_CAC_FILE": self.cac_file,
            "LTV_FX_FILE": self.fx_file,
            "LTV_COUNTRY": self.country,
        }
        if current_run_folder:
            env["LTV_OUTPUT_DIR"] = str(current_run_folder)
        return env
    
    def resolve(self):
        self.data_ltv.mkdir(parents=True, exist_ok=True)
        self.inputs_dir.mkdir(parents=True, exist_ok=True)
        self.results_base.mkdir(parents=True, exist_ok=True)
        return self


class Paths:
    @staticmethod
    def get_project_root() -> Path:
        return Path(__file__).parent.parent.parent
    
    @staticmethod
    def get_config_folder() -> Path:
        return Path(__file__).parent
    
    @staticmethod
    def get_data_xlsx_folder() -> Path:
        """Retorna la carpeta data_xlsx (origen único de archivos Excel/CSV)."""
        return Paths.get_config_folder() / "data_xlsx"
    
    @staticmethod
    def get_default_inputs_folder() -> Path:
        """Retorna la carpeta de inputs por defecto (AHORA: data_xlsx)."""
        return Paths.get_data_xlsx_folder()
    
    @staticmethod
    def get_default_outputs_folder(country: str = "GT") -> Path:
        """Retorna la carpeta de outputs por defecto."""
        return Paths.get_project_root() / f"Data_LTV_{country}" / "Results_LTV"
    
    @staticmethod
    def get_recovery_fallback(timestamp: str = None) -> Path:
        from datetime import datetime
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return Path.home() / "Downloads" / f"LTV_Recovery_{timestamp}"
    
    @staticmethod
    def _validate_or_reset_path(path: Optional[Path], default: Path, name: str) -> Path:
        """Valida si un path existe, si no, retorna el default."""
        if path is None:
            print(f"📁 {name}: Usando default (no configurado)")
            return default
        
        if not path.exists():
            print(f"⚠️ {name}: '{path}' no existe, usando default: '{default}'")
            return default
        
        return path
    
    @staticmethod
    def _get_paths_file(country: str = "GT") -> Path:
        return Paths.get_config_folder() / "data_json" / f"user_paths_{country}.json"
    
    @staticmethod
    def _load_saved_input_folder(country: str = "GT") -> Optional[Path]:
        paths_file = Paths._get_paths_file(country)
        try:
            if paths_file.exists():
                with open(paths_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    if "inputs_dir" in saved:
                        return Path(saved["inputs_dir"])
        except Exception:
            pass
        return None
    
    @staticmethod
    def _save_input_folder(path: Path, country: str = "GT"):
        paths_file = Paths._get_paths_file(country)
        try:
            paths_file.parent.mkdir(parents=True, exist_ok=True)
            if paths_file.exists():
                with open(paths_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
            else:
                saved = {}
            
            saved["inputs_dir"] = str(path)
            
            with open(paths_file, 'w', encoding='utf-8') as f:
                json.dump(saved, f, indent=2)
        except Exception as e:
            print(f"⚠️ No se pudo guardar la carpeta de entrada: {e}")
    
    @staticmethod
    def _load_saved_output_folder(country: str = "GT") -> Optional[Path]:
        paths_file = Paths._get_paths_file(country)
        try:
            if paths_file.exists():
                with open(paths_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    if "results_base" in saved:
                        return Path(saved["results_base"])
        except Exception:
            pass
        return None
    
    @staticmethod
    def _save_output_folder(path: Path, country: str = "GT"):
        paths_file = Paths._get_paths_file(country)
        try:
            paths_file.parent.mkdir(parents=True, exist_ok=True)
            if paths_file.exists():
                with open(paths_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
            else:
                saved = {}
            
            saved["results_base"] = str(path)
            
            with open(paths_file, 'w', encoding='utf-8') as f:
                json.dump(saved, f, indent=2)
        except Exception as e:
            print(f"⚠️ No se pudo guardar la carpeta de salida: {e}")
    
    @staticmethod
    def get_production_paths(country: str = "GT") -> PathsConfig:
        """
        Obtiene rutas de producción con validación automática.
        AHORA: El default de inputs_dir es data_xlsx.
        """
        root = Paths.get_project_root()
        base = root / f"Data_LTV_{country}"
        
        # ========== INPUTS DIR - AHORA DATA_XLSX POR DEFECTO ==========
        saved_inputs = Paths._load_saved_input_folder(country)
        default_inputs = Paths.get_default_inputs_folder()  # data_xlsx
        default_inputs.mkdir(parents=True, exist_ok=True)
        
        inputs_dir = Paths._validate_or_reset_path(
            saved_inputs, default_inputs, "Carpeta de entrada"
        )
        
        # Si se usó default y no estaba guardado, guardarlo
        if saved_inputs != inputs_dir:
            Paths._save_input_folder(inputs_dir, country)
        
        # ========== RESULTS BASE ==========
        saved_results = Paths._load_saved_output_folder(country)
        default_results = Paths.get_default_outputs_folder(country)
        default_results.mkdir(parents=True, exist_ok=True)
        
        results_base = Paths._validate_or_reset_path(
            saved_results, default_results, "Carpeta de salida"
        )
        
        # Si se usó default y no estaba guardado, guardarlo
        if saved_results != results_base:
            Paths._save_output_folder(results_base, country)
        
        return PathsConfig(
            base_path=base,
            code_path=root,
            data_ltv=base / "Data_LTV",
            inputs_dir=inputs_dir,
            results_base=results_base,
            country=country
        )
    
    @staticmethod
    def select_input_folder(country: str = "GT") -> Optional[Path]:
        """Selecciona carpeta de entrada con diálogo gráfico."""
        saved_path = Paths._load_saved_input_folder(country)
        default_path = Paths.get_default_inputs_folder()
        
        if saved_path and saved_path.exists():
            print(f"\n📂 Carpeta de entrada guardada: {saved_path}")
            usar = input("¿Usar esta carpeta? (s/n): ").strip().lower()
            if usar in ['s', 'si', 'sí', 'yes', 'y', '']:
                return saved_path
        
        print("\n" + "=" * 50)
        print(f"   SELECCIONAR CARPETA DE ENTRADA ({country})".center(50))
        print("=" * 50)
        print(f"📁 Carpeta DEFAULT: {default_path}")
        print("\n📁 Opciones:")
        print("   1. 📂 Abrir explorador de archivos")
        print(f"   2. 📁 Usar carpeta DEFAULT (data_xlsx)")
        print("   3. ⌨️  Ingresar ruta manualmente")
        print("   4. ❌ Cancelar")
        print("-" * 50)
        
        option = input("👉 Opción (1/2/3/4): ").strip()
        
        if option == '1':
            try:
                import tkinter as tk
                from tkinter import filedialog
                root = tk.Tk()
                root.withdraw()
                root.attributes('-topmost', True)
                folder = filedialog.askdirectory(title="Selecciona carpeta de datos LTV")
                root.destroy()
                if folder:
                    path = Path(folder)
                    Paths._save_input_folder(path, country)
                    print(f"✅ Carpeta seleccionada: {path}")
                    return path
                else:
                    print("⚠️ No se seleccionó ninguna carpeta")
            except Exception as e:
                print(f"⚠️ Error al abrir selector gráfico: {e}")
                print("   Usando entrada manual como fallback...")
                return Paths._manual_input_folder(country)
        
        elif option == '2':
            default_path.mkdir(parents=True, exist_ok=True)
            Paths._save_input_folder(default_path, country)
            print(f"✅ Carpeta DEFAULT seleccionada: {default_path}")
            return default_path
        
        elif option == '3':
            return Paths._manual_input_folder(country)
        
        else:
            print("⚠️ Cancelado. Usando carpeta actual.")
            return None

    @staticmethod
    def _manual_input_folder(country: str = "GT") -> Optional[Path]:
        """Entrada manual de ruta para entrada."""
        print("\n📝 Ingresa la ruta completa de la carpeta:")
        print("   (Puedes copiar y pegar la ruta)")
        ruta = input("👉 ").strip()
        if ruta:
            path = Path(ruta)
            if path.exists() or True:  # Permitir rutas nuevas
                Paths._save_input_folder(path, country)
                print(f"✅ Carpeta guardada: {path}")
                return path
            else:
                print(f"❌ La ruta '{ruta}' no existe")
                crear = input("¿Crear la carpeta? (s/n): ").strip().lower()
                if crear in ['s', 'si', 'sí', 'yes', 'y']:
                    path.mkdir(parents=True, exist_ok=True)
                    Paths._save_input_folder(path, country)
                    print(f"✅ Carpeta creada y guardada: {path}")
                    return path
        return None
    
    @staticmethod
    def select_output_folder(country: str = "GT") -> Optional[Path]:
        """Selecciona carpeta de salida con diálogo gráfico."""
        saved_path = Paths._load_saved_output_folder(country)
        default_path = Paths.get_default_outputs_folder(country)
        
        if saved_path and saved_path.exists():
            print(f"\n📂 Carpeta de salida guardada: {saved_path}")
            usar = input("¿Usar esta carpeta? (s/n): ").strip().lower()
            if usar in ['s', 'si', 'sí', 'yes', 'y', '']:
                return saved_path
        
        print("\n" + "=" * 50)
        print(f"   SELECCIONAR CARPETA DE SALIDA ({country})".center(50))
        print("=" * 50)
        print("📁 Opciones:")
        print("   1. 📂 Abrir explorador de archivos")
        print(f"   2. 📁 Usar carpeta DEFAULT ({default_path})")
        print("   3. ⌨️  Ingresar ruta manualmente")
        print("   4. ❌ Cancelar")
        print("-" * 50)
        
        option = input("👉 Opción (1/2/3/4): ").strip()
        
        if option == '1':
            try:
                import tkinter as tk
                from tkinter import filedialog
                root = tk.Tk()
                root.withdraw()
                root.attributes('-topmost', True)
                folder = filedialog.askdirectory(title="Selecciona carpeta para RESULTADOS LTV")
                root.destroy()
                if folder:
                    path = Path(folder)
                    Paths._save_output_folder(path, country)
                    print(f"✅ Carpeta seleccionada: {path}")
                    return path
                else:
                    print("⚠️ No se seleccionó ninguna carpeta")
            except Exception as e:
                print(f"⚠️ Error al abrir selector gráfico: {e}")
                print("   Usando entrada manual como fallback...")
                return Paths._manual_output_folder(country)
        
        elif option == '2':
            default_path.mkdir(parents=True, exist_ok=True)
            Paths._save_output_folder(default_path, country)
            print(f"✅ Carpeta DEFAULT seleccionada: {default_path}")
            return default_path
        
        elif option == '3':
            return Paths._manual_output_folder(country)
        
        else:
            print("⚠️ Cancelado. Usando carpeta actual.")
            return None

    @staticmethod
    def _manual_output_folder(country: str = "GT") -> Optional[Path]:
        """Entrada manual de ruta para salida."""
        print("\n📝 Ingresa la ruta completa de la carpeta:")
        print("   (Puedes copiar y pegar la ruta)")
        ruta = input("👉 ").strip()
        if ruta:
            path = Path(ruta)
            path.mkdir(parents=True, exist_ok=True)
            Paths._save_output_folder(path, country)
            print(f"✅ Carpeta guardada: {path}")
            return path
        return None