#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sistema LTV Pacifiko v7.0 - MULTI-PAÍS CON NUEVA SEGURIDAD
Entry point principal con selector de país al inicio.
"""

import sys
import os
import traceback
from pathlib import Path

# Configurar path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Run.Config.paths import Paths
from Run.Config.credentials import Credentials
from Run.Country.country_loader import CountryLoader
from Run.Country.country_selector import CountrySelector
from Run.Country.country_context import CountryContext
from Run.FX.fx_engine import FXEngine
from Run.Menu.menu_controller import MenuController
from Run.Menu.menu_auth import MenuAuth
from Run.Utils.logger import SystemLogger


# Constantes de navegación
BACK_TO_COUNTRY = "BACK_TO_COUNTRY"
EXIT_SYSTEM = "EXIT"


def main():
    """Punto de entrada principal - LOOP AUTH → COUNTRY → MAIN"""
    logger = SystemLogger()
    logger.info("=" * 60)
    logger.info("INICIANDO SISTEMA LTV v7.0 - MULTI-PAÍS")
    logger.info("=" * 60)
    
    # 🔧 MIGRACIÓN AUTOMÁTICA DE CREDENCIALES (CSV → JSON)
    try:
        from Run.Config.credential_store import CredentialStore
        print("\n" + "=" * 60)
        print("   VERIFICANDO SISTEMA DE CREDENCIALES".center(60))
        print("=" * 60)
        
        if CredentialStore.migrate_from_csv():
            print("✅ Migración completada. Usando nuevo sistema de credenciales cifrado.")
        else:
            print("ℹ️ Sistema de credenciales actualizado (JSON cifrado activo)")
        
        # Mostrar resumen del estado
        db_creds = CredentialStore.get_db_credentials()
        if db_creds.get('user'):
            print(f"   💾 DB User global: {db_creds.get('user')}")
        
        countries = CredentialStore.get_all_countries()
        if countries:
            print(f"   🌎 Países configurados: {', '.join(countries)}")
        
        print("-" * 60)
    except Exception as e:
        print(f"⚠️ Advertencia al verificar credenciales: {e}")
        print("   El sistema usará formato CSV legacy como fallback")
    
    while True:
        try:
            # ========== 1. AUTHENTICATION MENU ==========
            auth = MenuAuth(logger)
            
            # Auth menu returns: True (authenticated) or False (exit)
            authenticated = auth.authenticate_standalone()
            if not authenticated:
                print("\n👋 Saliendo del sistema...")
                logger.info("Usuario eligió salir desde autenticación")
                sys.exit(0)
            
            # ========== 2. COUNTRY SELECTOR (con opción de volver a auth) ==========
            while True:
                selector = CountrySelector()
                
                if not selector.has_countries():
                    print("❌ No hay configuraciones de países disponibles")
                    print("   Presiona Enter para volver al menú de autenticación...")
                    input()
                    break  # Volver a auth
                
                country_code = selector.select_with_back()
                
                # Señales de navegación
                if country_code == "BACK_TO_AUTH":
                    print("\n🔙 Volviendo al menú de autenticación...")
                    break  # Salir del country loop, volver a auth
                
                if country_code == EXIT_SYSTEM:
                    print("\n👋 Saliendo del sistema...")
                    logger.info("Usuario eligió salir desde selector de país")
                    sys.exit(0)
                
                # Cargar configuración del país
                country_config = CountryLoader.load_country(country_code)
                
                if not country_config:
                    print(f"❌ Error cargando configuración para {country_code}")
                    print("   Presiona Enter para intentar de nuevo...")
                    input()
                    continue
                
                # Verificar que usuario tiene credenciales para este país
                db_creds = auth.get_db_credentials_for_country(country_code)
                if not db_creds:
                    print(f"\n⚠️ No hay credenciales DB para {country_config.name}")
                    print("   Por favor, edita tu usuario y agrega credenciales para este país")
                    print("   o selecciona otro país.")
                    print("\n   Presiona Enter para continuar...")
                    input()
                    continue
                
                # Guardar variables de entorno
                os.environ["LTV_COUNTRY"] = country_config.code
                os.environ["LTV_COUNTRY_START_DATE"] = str(country_config.cohort_start_year)
                os.environ["LTV_COUNTRY_END_DATE"] = str(country_config.cohort_end_year)
                os.environ["LTV_DEFAULT_FX_RATE"] = str(country_config.default_fx_rate)
                
                print(f"\n🌎 País seleccionado: {country_config.name} ({country_config.code})")
                print(f"📅 Cohortes desde: {country_config.cohort_start_year}")
                print(f"💱 Moneda: {country_config.currency} | FX default: {country_config.default_fx_rate}")
                
                # Cargar credenciales al sistema legacy
                Credentials.load_for_country(country_config.code)
                
                # ========== 3. CONFIGURAR RUTAS ==========
                paths = Paths.get_production_paths(country_config.code)
                logger.info(f"📂 Directorio base: {paths.base_path}")
                logger.info(f"📂 Inputs: {paths.inputs_dir}")
                
                # Mostrar info de rutas
                print(f"\n📁 RUTAS DE TRABAJO:")
                print(f"   Inputs: {paths.inputs_dir}")
                print(f"   Outputs: {paths.results_base}")
                print(f"   Data LTV: {paths.data_ltv}")
                
                # ========== 4. CREAR CONTEXTO DE PAÍS ==========
                country_context = CountryContext(
                    code=country_config.code,
                    name=country_config.name,
                    currency=country_config.currency,
                    default_fx_rate=country_config.default_fx_rate,
                    cohort_start_year=country_config.cohort_start_year,
                    cohort_end_year=country_config.cohort_end_year
                )
                
                # ========== 5. INICIALIZAR FX ENGINE ==========
                fx_path = paths.inputs_dir / paths.fx_file
                fx_engine = FXEngine(country_context, fx_path)
                
                # ========== 6. CREAR CONTROLADOR Y EJECUTAR ==========
                controller = MenuController(paths, country_context, fx_engine)
                controller.set_auth(auth)
                
                # Ejecutar menú principal - recibe señal
                result = controller.run()
                
                if result == BACK_TO_COUNTRY:
                    print("\n🔙 Volviendo al selector de países...")
                    continue  # Volver a seleccionar país
                elif result == EXIT_SYSTEM:
                    print("\n👋 Saliendo del sistema...")
                    logger.info("Usuario eligió salir desde menú principal")
                    sys.exit(0)
                else:
                    # Error o salida normal
                    break
                    
        except KeyboardInterrupt:
            print("\n\n⚠️ Sistema interrumpido por el usuario")
            logger.info("KeyboardInterrupt - sistema detenido")
            sys.exit(0)
        except Exception as e:
            print(f"\n❌ Error fatal: {e}")
            logger.error(f"Error fatal: {e}", exc_info=True)
            traceback.print_exc()
            
            print("\n" + "=" * 50)
            print("   OPCIONES DE RECUPERACIÓN".center(50))
            print("=" * 50)
            print("1. 🔄 Reiniciar el sistema")
            print("2. 🔙 Volver al menú de autenticación")
            print("3. ❌ Salir")
            print("-" * 50)
            
            opcion = input("\n👉 Opción (1/2/3): ").strip()
            
            if opcion == '1':
                print("\n🔄 Reiniciando sistema...")
                continue
            elif opcion == '2':
                print("\n🔙 Volviendo al menú de autenticación...")
                continue
            else:
                print("\n👋 Saliendo...")
                sys.exit(1)


if __name__ == "__main__":
    main()