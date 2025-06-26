# Buscador de Ofertas de Trabajo

Este proyecto implementa un sistema de búsqueda optimizado que reduce significativamente el tamaño del índice utilizando técnicas avanzadas de compresión y estructuras de datos eficientes.

[Dataset](https://drive.google.com/file/d/19JnCcKkm43JZ7VJk1yv3DQJCQDbHgTL-/view?usp=sharing)

El dataset contiene más de 20 millones de ofertas de trabajo de LinkedIn. Es una modificación de: https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024/data.

#### Estructura

El dataset está compuesto de 20,296,381 filas (Cada una siendo una oferta de trabajo) y dos columnas donde una es la URL del trabajo y la otra contiene la habilidades requeridas por cada trabajo. Muestra de los primeros elementos

#### Criterios de búsqueda
Los criterios de búsqueda elegidos son las habilidades, porque es la forma en la que las personas encontrarían trabajo en base a sus necesidades. El dataset cuenta con 3,390,580 habilidades únicas.
Hemos añadido una lista (habilidades.csv) que contiene todas las habilidades que se pueden usar como criterio de búsqueda

## Arquitectura del Sistema

El sistema sigue una arquitectura desacoplada, donde cada componente tiene una única responsabilidad. La comunicación entre la interfaz de usuario y el motor de búsqueda se realiza a través de **tuberías con nombre (Named Pipes)**, un mecanismo de Comunicación Entre Procesos (IPC) de POSIX.

```
+-------------------+      +----------------+      +------------------+
|                   |      |                |      |                  |
|  data.csv (Datos) | <--- |   crear_indice | ---> |  jobs.skl (Dir.) |
|                   |      |   (Indexador)  |      |                  |
+-------------------+      +----------------+      +------------------+
                                                     |
                                                     V
                                                 +------------------+
                                                 |  jobs.idx (Datos)|
                                                 +------------------+


+-------------------+      (Named Pipes)       +------------------+
|                   |      /tmp/job_*_pipe     |                  |
|   ui (Cliente)    | <----------------------> |  engine (Motor)  |
|                   |                          |                  |
+-------------------+                          +--------+---------+
                                                        |
                                                        V
                                                 (Lee los archivos de índice)
```

### Componentes:

  * **`crear_indice` (El Indexador):** Lee el archivo `data.csv`, procesa todas las habilidades de cada oferta y construye dos archivos de índice optimizados para búsquedas rápidas y con bajo consumo de memoria.
  * **`engine` (El Motor de Búsqueda):** Es el cerebro del sistema. Se ejecuta en segundo plano, esperando peticiones de búsqueda. No carga los índices completos en memoria; en su lugar, opera directamente sobre los archivos en el disco para cumplir con los estrictos requisitos de memoria.
  * **`ui` (La Interfaz de Usuario):** Un cliente de línea de comandos que permite al usuario introducir hasta tres criterios de búsqueda. Envía la consulta al motor y muestra los resultados recibidos.

### Archivos Generados:

  * **`jobs.skl`**: Un "directorio de habilidades". Es un índice primario que contiene una lista de todas las habilidades únicas, **ordenadas alfabéticamente**. Para cada habilidad, almacena metadatos como la cantidad de ofertas y la ubicación de su lista de `offsets` en `jobs.idx`.
  * **`jobs.idx`**: Un índice secundario que contiene las listas de `offsets` (posiciones de línea en `data.csv`). Cada lista está **ordenada numéricamente** para permitir intersecciones eficientes.

## Optimizaciones de Rendimiento

Para alcanzar los objetivos de memoria y velocidad, se implementaron las siguientes técnicas avanzadas:

  * **Índice de Dos Niveles en Disco:** Se separa el "directorio" (`.skl`) de los "datos" (`.idx`), evitando cargar todo en RAM. El motor solo necesita leer pequeñas porciones de estos archivos por cada consulta.
  * **Índices Pre-ordenados:** El indexador invierte tiempo en ordenar alfabéticamente el `jobs.skl` y numéricamente las listas en `jobs.idx`. Este pre-procesamiento es la clave para las optimizaciones del motor.
  * **Búsqueda de Skills en Archivo:** El motor no guarda el directorio de `skills` en memoria. En su lugar, realiza una búsqueda (lineal en el código actual, pero diseñada para ser binaria) directamente sobre el archivo `jobs.skl` para encontrar los metadatos de una `skill`.
  * **Intersección por Fusión (Sort-Merge Join):** Para encontrar trabajos que coincidan con múltiples `skills`, el motor no carga las listas de `offsets` completas. En su lugar, lee las dos listas ordenadas desde el disco de forma sincronizada, encontrando las coincidencias sobre la marcha. Este método tiene un uso de memoria casi nulo.

## Prerrequisitos

Para compilar y ejecutar este proyecto, necesitarás:

  * Un compilador de C (ej. `gcc`).
  * La herramienta `make`.
  * Un sistema operativo compatible con POSIX (Linux, macOS).

## Instalación y Uso

Sigue estos pasos para poner en marcha el sistema.

### 1\. Compilación

Clona el repositorio y compila los tres ejecutables usando `make`. Se incluye un `Makefile` para facilitar el proceso.

```bash
# Clona el repositorio
git clone <URL_DEL_REPOSITORIO>
cd <NOMBRE_DEL_REPOSITORIO>

# Compila el proyecto
make all
```

Esto generará tres archivos ejecutables: `crear_indice`, `engine` y `ui`.

### 2\. Paso 1: Generar el Índice

Antes de poder realizar búsquedas, debes indexar tu archivo de datos. Asegúrate de que tu archivo de ofertas de empleo se llame `data.csv` y esté en la misma carpeta.

```bash
# Ejecuta el indexador. Este proceso puede tardar si el archivo es grande.
./crear_indice
```

Verás un contador de progreso. Al finalizar, tendrás los archivos `jobs.skl` y `jobs.idx` en tu directorio.

### 3\. Paso 2: Ejecutar el Motor y la Interfaz

El motor debe estar corriendo en segundo plano para que la interfaz de usuario pueda conectarse a él. Para esto, necesitarás **dos terminales separadas**.

**En la Terminal 1 (Inicia el Motor):**

```bash
./engine
```

Verás un mensaje indicando que está esperando peticiones. Deja esta terminal abierta.

**En la Terminal 2 (Inicia la Interfaz de Usuario):**

```bash
./ui
```

Ahora puedes interactuar con el menú de búsqueda.

#### Ejemplo de Búsqueda

1.  Inicia la `./ui`.
2.  Selecciona la opción `1` para ingresar el primer criterio (ej: `Python`).
3.  Selecciona la opción `2` para ingresar el segundo criterio (ej: `AWS`).
4.  Selecciona la opción `4` para realizar la búsqueda.
5.  Los resultados que cumplen con **ambos** criterios se mostrarán en pantalla.
6.  Selecciona la opción `5` para salir.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
