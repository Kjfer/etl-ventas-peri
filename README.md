# ETL Ventas - peri

Proyecto ETL para la ingestión, transformación y carga de datos de ventas de la plataforma peri.

Este repositorio contiene pipelines y utilidades para extraer datos de las fuentes de ventas, limpiar y transformar los registros, y cargarlos en el destino final (base de datos o data warehouse).

Características

- Extracción desde CSV, APIs y/o bases de datos.
- Transformaciones comunes: normalización de campos, deduplicado, enriquecimiento y agregaciones.
- Carga a destino relacional o analítico.
- Scripts y/o DAGs para ejecución programada.

Requisitos

- Python 3.8+
- pip
- (Opcional) Docker para ejecutar en contenedores
- Dependencias listadas en requirements.txt (si existe)

Instalación

1. Clona el repositorio:

   git clone https://github.com/proyectosperi/etl-ventas-peri.git
   cd etl-ventas-peri

2. Crea y activa un entorno virtual (recomendado):

   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate    # Windows

3. Instala las dependencias:

   pip install -r requirements.txt

Uso

- Ejecutar el pipeline principal (ejemplo):

  python run_etl.py --config configs/production.yml

- Ejecutar tareas individuales desde la carpeta `tasks` o `scripts` según su organización.

Estructura sugerida

- configs/         -> archivos de configuración por ambiente
- data/            -> datasets de ejemplo o salidas
- src/             -> código fuente (extracción, transformación, carga)
- scripts/         -> utilidades y scripts ejecutables
- dags/            -> DAGs de Airflow (si aplica)
- requirements.txt -> dependencias de Python

Buenas prácticas

- Añadir un archivo LICENSE si se desea abrir el código.
- Documentar configuraciones sensibles en variables de entorno y no en archivos en el repo.
- Añadir tests y CI (GitHub Actions) para validar cambios.

Contribuir

Si quieres contribuir:

1. Haz fork del repositorio.
2. Crea una rama con tu cambio: `git checkout -b feature/mi-cambio`.
3. Crea un PR describiendo la motivación y los cambios realizados.

Contacto

- Autor: proyectosperi

Licencia

- Este repositorio no contiene un archivo LICENSE por defecto. Si quieres, puedo añadir una licencia (por ejemplo MIT).
