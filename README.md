# ED.03.02. - Fundamentos de RDD en Apache Spark

Repositorio correspondiente a la práctica de **Manejo Masivo de Datos** sobre **Resilient Distributed Datasets (RDD)** utilizando **Apache Spark (PySpark 3.5.3)** en Ubuntu Linux.

---

## 1. Marco Teórico: ¿Qué es un RDD?

Un **RDD (Resilient Distributed Dataset)** es la abstracción fundamental de datos en Apache Spark. Se define como una colección de elementos **inmutable**, **tolerante a fallos** y **particionada** a través de los nodos de un clúster para ser procesada en paralelo.

### Características Principales
* **Inmutabilidad:** Un RDD no se modifica directamente; cada transformación genera un nuevo RDD.
* **Evaluación Perezosa (*Lazy Evaluation*):** Las transformaciones no se ejecutan al instante; Spark construye un grafo acíclico dirigido (**DAG**) y solo ejecuta el cálculo cuando se invoca una **acción**.
* **Tolerancia a Fallos (*Lineage*):** Si una partición se pierde, Spark la reconstruye automáticamente aplicando las transformaciones registradas en su linaje.
* **Persistencia en Memoria:** Permite almacenar RDDs reutilizados en RAM o disco mediante `.cache()` o `.persist()` para evitar recalcular el DAG.

### Transformaciones vs. Acciones
| Tipo | Descripción | Ejemplos utilizados en la práctica |
| :--- | :--- | :--- |
| **Transformaciones *Narrow*** | Cada partición padre es usada por máximo una partición hija (sin movimiento en red). | `map()`, `flatMap()`, `filter()`, `union()` |
| **Transformaciones *Wide*** | Requieren redistribuir datos entre particiones a través de la red (**Shuffle**). | `reduceByKey()`, `aggregateByKey()`, `sortBy()`, `distinct()`, `intersection()`, `subtract()` |
| **Acciones** | Disparan la ejecución del DAG y devuelven un resultado al *Driver* o lo escriben en disco. | `collect()`, `count()`, `saveAsTextFile()` |

---

## 2. Configuración del Entorno y Estructura del Proyecto

* **Sistema Operativo:** Ubuntu Linux
* **Entorno Virtual:** Python 3 (`../spark-practicas/venv`)
* **Motor:** Apache Spark / PySpark `3.5.3`
* **Entorno Interactivo:** JupyterLab

### Estructura del Repositorio
```text
ED.03.02.-RDD/
├── ED_03_02_RDD.ipynb      # Cuaderno principal con código, salidas y explicaciones
├── ED_03_02_RDD.pdf        # Documento exportado en PDF para entrega oficial
├── README.md               # Documentación completa de la práctica
├── .gitignore              # Exclusión de archivos temporales y checkpoints
├── datos/                  # Archivos de entrada y salidas generadas por Spark
│   ├── logs_simulados.txt
│   ├── notas_mates.txt
│   ├── notas_fisica.txt
│   ├── notas_ingles.txt
│   └── salida_ips/
└── capturas/               # Evidencias visuales de ejecución
```

### Inicialización Limpia de Spark
Se configuró `SparkSession` y `SparkContext` ajustando el nivel de registro a `ERROR` (`sc.setLogLevel("ERROR")`) y silenciando advertencias de consola para mantener salidas limpias y legibles:

```python
import os
import warnings

warnings.filterwarnings("ignore")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ED_03_02_Fundamentos_RDD") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
```

---

## 3. Desarrollo de Ejercicios Prácticos (1 al 8)

### Ejercicio 1: Word Count Básico
* **Objetivo:** Contar la frecuencia de cada palabra en una lista de frases (`["hola mundo", "hola spark", "mundo spark"]`).
* **Pipeline:** `parallelize()` $\rightarrow$ `flatMap(split)` $\rightarrow$ `map((palabra, 1))` $\rightarrow$ `reduceByKey(+)`.
* **Resultado:**
  * `mundo: 2`
  * `hola: 2`
  * `spark: 2`

### Ejercicio 2: Análisis de Ventas
* **Objetivo:** Sumar ventas por ciudad, filtrar aquellas mayores a $100$ y ordenar de mayor a menor.
* **Pipeline:** `reduceByKey(+)` $\rightarrow$ `filter(x[1] > 100)` $\rightarrow$ `sortBy(x[1], ascending=False)`.
* **Resultado:** `[('Barcelona', 200), ('Madrid', 150)]`

### Ejercicio 3: Filtrado y Transformación (Conversión de Divisas)
* **Objetivo:** Filtrar precios mayores o iguales a $20\text{ USD}$ y convertirlos a $\text{EUR}$ multiplicando por $0.9$.
* **Pipeline:** `filter(p >= 20)` $\rightarrow$ `map(round(p * 0.9, 2))`.
* **Resultado:** `[22.5, 36.0, 90.0]`

### Ejercicio 4: Procesamiento de CSV
* **Objetivo:** Extraer los productos únicos de registros separados por comas y contar el total.
* **Pipeline:** `flatMap(split(","))` $\rightarrow$ `distinct()` $\rightarrow$ `collect()` / `count()`.
* **Resultado:** `['Pan', 'Peras', 'Leche', 'Huevos', 'Manzanas', 'Chocolate']` (Total: `6` productos únicos).

### Ejercicio 5: Análisis de Logs
* **Objetivo:** Leer `datos/logs_simulados.txt`, contar peticiones por dirección IP, filtrar las que superan $1$ petición y guardar el resultado en disco.
* **Pipeline:** `sc.textFile()` $\rightarrow$ `map((ip, 1))` $\rightarrow$ `reduceByKey(+)` $\rightarrow$ `filter(x[1] > 1)` $\rightarrow$ `saveAsTextFile("datos/salida_ips")`.
* **Resultado:** `[('192.168.1.1', 2)]`

### Ejercicio 6: Pipeline y Shuffle
* **Objetivo:** Encadenar filtrado ($\ge 60$), agregación por clave y ordenamiento descendente sobre registros de ventas.
* **Explicación del Shuffle:** `filter` se ejecuta sin *shuffle* (operación *narrow*) reduciendo el volumen de datos antes de que `reduceByKey` y `sortBy` reorganicen las claves por la red.
* **Resultado:** `[('Barcelona', 200), ('Madrid', 100)]`

### Ejercicio 7: Diferencia entre `map()` y `flatMap()`
* **`map()`:** Aplica una transformación $1 \text{ a } 1$; al dividir frases devuelve una **lista anidada** (`[['Hola', 'mundo'], ['Apache', 'Spark', 'es', 'guay']]`).
* **`flatMap()`:** Aplica la transformación y **aplana** las colecciones internas en un único RDD de primer nivel (`['Hola', 'mundo', 'Apache', 'Spark', 'es', 'guay']`).

### Ejercicio 8: Optimización de RDD Reutilizados
* **Estrategia:** Si un RDD se reutiliza $5$ veces en distintas acciones, se debe invocar `.cache()` (o `.persist(StorageLevel.MEMORY_AND_DISK)`) antes de la primera acción.
* **Justificación:** Evita que Spark recompute todo el linaje (DAG) desde el origen en cada acción, manteniendo las particiones en memoria para las $4$ ejecuciones siguientes, y liberándolas al final con `.unpersist()`.

---

## 4. Proyecto Práctico: Análisis de Calificaciones

Se procesaron tres archivos independientes (`notas_mates.txt`, `notas_fisica.txt` y `notas_ingles.txt`) unificándolos mediante `.union()` y aplicando `.cache()` para resolver las métricas académicas:

1. **Nota media por alumno (`aggregateByKey` + `mapValues`):**
   * `Angel`: **6.33**
   * `Ramon`: **4.50**
   * `Maria`: **3.67**
2. **Alumnos matriculados en las 3 asignaturas (`intersection`):** `['Angel', 'Maria']`
3. **Alumnos con asignaturas faltantes (`subtract`):** `['Ramon']`
4. **Alumnos aprobados ($\text{media} \ge 5.0$):** `[('Angel', 6.33)]`
5. **Alumnos suspensos ($\text{media} < 5.0$):** `[('Ramon', 4.5), ('Maria', 3.67)]`

---

## 5. Pregunta de Reflexión: ¿Cuándo usar un RDD en lugar de un DataFrame?

Se debe optar por un **RDD** frente a un **DataFrame** cuando:
* Los datos de entrada **no tienen un esquema estructurado** o son altamente irregulares (por ejemplo, logs crudos de servidores web, texto libre sin delimitadores fijos o flujos binarios).
* Se requiere control de bajo nivel con funciones personalizadas en Python para limpiar y parsear cada registro antes de darle una estructura tabular.
* Una vez que los datos están limpios y estructurados (CSV, JSON, Parquet), es preferible convertirlos a **DataFrames** para aprovechar las optimizaciones automáticas del motor **Catalyst** y **Tungsten**.