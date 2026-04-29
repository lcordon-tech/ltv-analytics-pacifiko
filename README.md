# LTV Analytics Pipeline

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Sistema profesional para calcular y analizar métricas de Lifetime Value (LTV) en entornos de e-commerce multi-país (Guatemala y Costa Rica).

## Descripción

El sistema extrae datos de órdenes desde MySQL, aplica transformaciones financieras (IVA, tipo de cambio), calcula contribution profit y genera reportes detallados segmentados por cohortes, categorías, marcas y productos.

**¿Para quién?** Analistas de datos, equipos de marketing y finanzas en e-commerce que necesitan medir el valor real de sus clientes y optimizar la inversión en adquisición y retención.

**¿Qué problema resuelve?** Permite responder preguntas clave como: ¿cuánto vale un cliente a largo plazo?, ¿qué categorías atraen a los mejores clientes?, ¿dónde debería invertir mi presupuesto de marketing?

## Características principales

- ✅ **Multi-país**: Soporte nativo para Guatemala (GTQ) y Costa Rica (CRC)
- ✅ **Cohortes dinámicas**: Granularidad trimestral, mensual, semanal, semestral o anual
- ✅ **Análisis multi-dimensión**: LTV por categoría, subcategoría, marca y producto
- ✅ **Unit Economics**: CAC, LTV neto, ROI, payback period
- ✅ **Sistema de scoring**: Penalización por muestra pequeña y normalización de métricas
- ✅ **Reportes automatizados**: Excel multi-hoja, gráficos, resúmenes ejecutivos
- ✅ **Buscador interactivo**: Consultas rápidas sin re-ejecutar pipelines
- ✅ **Seguridad**: Credenciales cifradas (Fernet) + autenticación de usuarios

## Estructura del proyecto

```
ltv-analytics-pacifiko/
├── DataRepository/         # Extracción y transformación (ETL)
│   ├── Data/               # QueryEngine, DataLoader, DataValidator, DataMerger
│   ├── Processing/         # CohortBuilder, RetentionApplier, AssumptionApplier, MetricsCalculator
│   └── Output/             # FinalDatasetBuilder, DataExporter
├── Model/                  # Dominio y análisis LTV
│   ├── Domain/             # Order, Customer, LTVController
│   ├── Analytics/          # CohortAnalyzer, UnitEconomicsAnalyzer, etc.
│   ├── Data/               # RealDataRepository, CACRepository
│   └── Output/             # DataExporter (model layer)
├── Category/               # Análisis multi-dimensión
│   ├── Grouping/           # CategoryGrouper, EntryBasedBehaviorGrouper
│   ├── Cohort/             # CohortConfig, CohortManager, CohortGrouper
│   ├── Analytics/          # Frequency/Time/Conversion analyzers, Scoring
│   ├── Orchestrators/      # BaseDimensionOrchestrator, GlobalLTVOrchestrator
│   ├── Reporting/          # BaseExporter, CategoryVisualizer, etc.
│   └── Query/              # DimensionQueryEngine
├── Run/                    # Configuración, autenticación, menús
│   ├── Config/             # Paths, Credentials, DevModeManager
│   ├── Security/           # AuthService, UserManager, CredentialStore
│   ├── Country/            # CountryContext, CountrySelector
│   ├── FX/                 # FXEngine
│   ├── Services/           # ScriptRunner, SSHService, CohortSupuestosManager
│   ├── Core/               # CohortContextManager, SSHManager
│   ├── Utils/              # SystemLogger, retry decorator, get_flexible_input
│   └── Menu/               # MenuAuth, MenuConfig, MenuExecutor, MenuController
├── docs/                   # Documentación
│   ├── technical/          # Documentación técnica (LaTeX)
│   └── user/               # Manual de usuario (LaTeX/PDF)
├── data_xlsx/              # Archivos Excel de entrada (SOIS, SUPUESTOS, catalogLTV)
├── config/                 # Configuraciones JSON (persistencia)
├── logs/                   # Logs del sistema
├── requirements.txt        # Dependencias Python
├── .gitignore              # Archivos ignorados
├── LICENSE                 # Licencia MIT
└── main.py                 # Punto de entrada
```

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/ltv-analytics-pacifiko.git
cd ltv-analytics-pacifiko
```

### 2. Crear entorno virtual (recomendado)

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configuración inicial

1. Coloca los archivos Excel en la carpeta `data_xlsx/`:
   - `SOIS.xlsx`
   - `SUPUESTOS.xlsx`
   - `catalogLTV.xlsx`
   - `CAC.xlsx` (opcional)
   - `TIPO_DE_CAMBIO.xlsx` (opcional)

2. Ejecuta el programa por primera vez:
   ```bash
   python main.py
   ```
   El sistema te guiará para crear un usuario y configurar las credenciales de base de datos.

## Requisitos de datos

### Archivos obligatorios

| Archivo | Hoja | Columnas clave |
|---------|------|----------------|
| SOIS.xlsx | GT o CR | PID, Fecha_inicio, Fecha_fin, SOI_USD |
| SUPUESTOS.xlsx | 1PGT, 3PGT, FBPGT, TMGT, DSGT | cohort, cogs, retention, cac, shipping_cost, ... |
| catalogLTV.xlsx | GT o CR | product_pid, b_unit, category, subcategory, brand, name |

### Archivos opcionales

| Archivo | Propósito |
|---------|-----------|
| CAC.xlsx | Costo de adquisición por cohorte |
| TIPO_DE_CAMBIO.xlsx | Tasas de cambio históricas |

> 📖 Para detalles completos de formato, consulta el Manual de Usuario.

## Uso rápido

```bash
# 1. Iniciar el sistema
python main.py

# 2. Autenticarse (crear usuario si es primera vez)
# 3. Seleccionar país (GT o CR)
# 4. En el menú principal:
#    - Opción 1: Pipeline Completo (extrae datos y analiza)
#    - Opción 3 > 7: Modelo Completo (solo análisis)
#    - Opción 4: Buscador interactivo
```

## Documentación

| Documento | Ubicación | Contenido |
|-----------|-----------|-----------|
| Documentación técnica | `docs/technical/documentation_technical.pdf` | Arquitectura, clases, cálculos, guía para desarrolladores |
| Manual de usuario | `docs/user/manual_usuario.pdf` | Instalación, uso, troubleshooting |

## Manejo de credenciales

- **Nunca subas credenciales reales a GitHub**
- Las credenciales se almacenan cifradas localmente en `Run/Config/secure/credentials.enc`
- La clave de cifrado está en `Run/Config/secure/.key` (no compartir)
- En desarrollo, usa el **Modo desarrollador** (auto-login sin credenciales DB)

**Configuración de credenciales:**

```bash
# Primera ejecución - el menú te guiará automáticamente
python main.py
# → Crear usuario → Ingresar credenciales DB por país
```

## Errores comunes

| Error | Solución |
|-------|----------|
| `Can't connect to MySQL server` | Verifica credenciales y túnel SSH |
| `No se encuentra SUPUESTOS.xlsx` | Coloca el archivo en `data_xlsx/` |
| `Cohortes nuevas no configuradas` | Usa "Configuraciones → Gestión de cohortes" para agregarlas |
| `LTV/CAC ratio = 0` | Agrega CAC en SUPUESTOS o en CAC.xlsx |
| `No hay datos en Data_LTV` | Ejecuta "Pipeline Completo" al menos una vez |

## Roadmap (mejoras futuras)

- [ ] Dashboard web interactivo (Streamlit)
- [ ] Exportación automática a Google Sheets / BigQuery
- [ ] Alertas automáticas de anomalías en métricas clave
- [ ] Soporte para más países (México, Colombia, Perú)

## Autoría

**Luis Cordón**  
- Email: guichocordon@gmail.com  
- Teléfono: +502 5364-8127

## Licencia

MIT license - ver archivo [LICENSE](LICENSE) para detalles.

