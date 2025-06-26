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

El sistema sigue una arquitectura desacoplada, donde cada componente tiene una única responsabilidad. La comunicación entre la interfaz de usuario y el motor de búsqueda se realiza a través de **tuberías**, un mecanismo de Comunicación Entre Procesos (IPC) de POSIX.

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

### Compilación

Para compilar el proyecto debemos correr `make`. Esto compilará todos los archivos del proyecto guardandolos en la carpeta **dist**.

#### Ejemplo de Búsqueda

1.  Corre dist/main.
2.  Selecciona la opción `1` para ingresar el primer criterio (ej: `Python`).
3.  Selecciona la opción `2` para ingresar el segundo criterio (ej: `AWS`).
4.  Selecciona la opción `4` para realizar la búsqueda.
5.  Los resultados que cumplen con **ambos** criterios se mostrarán en pantalla.
6.  Selecciona la opción `5` para salir.
