# Buscador de Ofertas de Trabajo

Este proyecto implementa un sistema de búsqueda optimizado que reduce significativamente el tamaño del índice utilizando técnicas avanzadas de compresión y estructuras de datos eficientes.

[Dataset](https://drive.google.com/file/d/19JnCcKkm43JZ7VJk1yv3DQJCQDbHgTL-/view?usp=sharing)

El dataset contiene más de 20 millones de ofertas de trabajo de LinkedIn. Es una modificación de: https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024/data.

## Uso

```bash
yarn start
```

Usa el archivo habilidades.csv como referencia para tus busquedas


## Algoritmo de Indexado

El proceso de indexado consta de varias fases que transforman los datos CSV en un índice altamente optimizado:

#### Estructura

El dataset está compuesto de 20,296,381 filas (Cada una siendo una oferta de trabajo) y dos columnas donde una es la URL del trabajo y la otra contiene la habilidades requeridas por cada trabajo. Muestra de los primeros elementos

#### Criterios de búsqueda
Los criterios de búsqueda elegidos son las habilidades, porque es la forma en la que las personas encontrarían trabajo en base a sus necesidades. El dataset cuenta con 3,390,580 habilidades únicas.
Hemos añadido una lista (habilidades.csv) que contiene todas las habilidades que se pueden usar como criterio de búsqueda

### 1. Procesamiento Inicial
- **Entrada**: Archivo CSV con formato `id,skill1,skill2,...,skillN`
- **Proceso**:
  1. Lectura secuencial del archivo línea por línea
  2. Para cada línea:
     - Se guarda el offset de inicio de la línea
     - Se extraen las habilidades separadas por comas
     - Se normalizan los textos (eliminación de espacios en blanco)

### 2. Estructura de Datos Principal
- **Diccionario de Habilidades**:
  ```c
  typedef struct {
      char skill[MAX_SKILL_LENGTH];
      uint32_t* offsets;     // Array de offsets ordenados
      size_t count;           // Número de offsets
      size_t capacity;        // Capacidad actual del array
  } SkillEntry;
  ```

### 3. Técnicas de Compresión

#### a) Delta Encoding para Offsets
- Los offsets se almacenan como diferencias consecutivas
- Ejemplo:
  ```
  Offsets originales: [1000, 1050, 1100, 1200]
  Delta encoding:    [1000, 50, 50, 100]
  ```

#### b) Variable Byte Encoding
- Codificación de enteros usando 7 bits por byte
- El bit más alto indica si hay más bytes por leer
- Reduce el tamaño de números pequeños

#### c) Compresión con Zstandard
- Se aplica compresión Zstandard al índice final
- Nivel de compresión ajustable (actualmente 3)

### 4. Formato del Archivo de Índice

```
[Encabezado]
  - Tamaño descomprimido (8 bytes)

[Sección de Datos Comprimidos]
  - Número de entradas (size_t)
  - Para cada entrada:
    - Longitud de la habilidad (1 byte)
    - Texto de la habilidad (n bytes)
    - Conteo de offsets (variable byte encoding)
    - Offsets (delta + variable byte encoding)
```

### 5. Ventajas del Enfoque

1. **Eficiencia en Espacio**:
   - Reducción >90% en tamaño comparado con texto plano
   - Offsets almacenados de forma compacta

2. **Rendimiento en Búsqueda**:
   - Acceso O(log n) a las habilidades
   - Descompresión selectiva de bloques

3. **Escalabilidad**:
   - Manejo eficiente de grandes volúmenes de datos
   - Bajo consumo de memoria durante la búsqueda

## Requisitos para compilar

```bash
# Para sistemas basados en Debian/Ubuntu:
sudo apt-get update
sudo apt-get install -y build-essential libzstd-dev

# Para sistemas basados en RHEL/CentOS/Fedora:
sudo dnf install -y gcc make zstd-devel
# Para versiones más antiguas de RHEL/CentOS
sudo yum install -y gcc make zstd-devel

# Para Arch Linux:
sudo pacman -S --noconfirm gcc make zstd

# Para macOS (usando Homebrew):
brew install zstd
```

Si planeas usar los scripts de Yarn, necesitarás Node.js (v20+) y Yarn berry (v4.9.2+)
Yo recomiendo instalar nvm, el yarn viene con node y se activa con el corepack
