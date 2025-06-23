#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <zstd.h>

// Función de comparación para qsort
static int compare_uint32(const void *a, const void *b) {
    uint32_t arg1 = *(const uint32_t *)a;
    uint32_t arg2 = *(const uint32_t *)b;
    return (arg1 > arg2) - (arg1 < arg2);
}
#include <zstd.h>
#include "utils.h"

// --- Definiciones ---
#define INITIAL_CAPACITY 65536 // Tamaño inicial de la tabla hash (potencia de 2)
#define MAX_SKILL_LENGTH 256
// Comprimir con Zstandard (nivel 19 es el máximo)
#define COMPRESSION_LEVEL 6
#define HASH_LOAD_FACTOR 0.75f // Factor de carga máximo antes de redimensionar
#define MAX_CHAIN_LENGTH 8     // Longitud máxima de cadena antes de redimensionar

// Estructura para almacenar una habilidad con sus offsets
typedef struct SkillEntry
{
    char skill[MAX_SKILL_LENGTH];
    uint32_t *offsets;       // Array de offsets
    size_t count;            // Número de offsets
    size_t capacity;         // Capacidad actual del array
    struct SkillEntry *next; // Para manejar colisiones
} SkillEntry;

// Estructura principal de la tabla hash
typedef struct
{
    SkillEntry **entries; // Array de punteros a entradas
    size_t size;          // Número de entradas usadas
    size_t capacity;      // Tamaño total de la tabla
    size_t max_chain;     // Longitud máxima de cadena actual
} Dictionary;

// Inicializa un diccionario vacío
void dict_init(Dictionary *dict)
{
    dict->size = 0;
    dict->capacity = INITIAL_CAPACITY;
    dict->max_chain = 0;
    dict->entries = calloc(dict->capacity, sizeof(SkillEntry *));

    if (!dict->entries)
    {
        perror("Error al asignar memoria para el diccionario");
        exit(EXIT_FAILURE);
    }
}

// Función hash mejorada (MurmurHash3)
// Cada busqueda en el diccionario antes de agregar tomaba mucho tiempo.
// Por lo que se implemento esta funcion para optimizar el proceso.
static inline uint32_t hash_string(const char *key, size_t length)
{
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xe6546b64;

    uint32_t hash = 0xdeadbeef;
    const int nblocks = length / 4;
    const uint32_t *blocks = (const uint32_t *)key;
    int i;

    for (i = 0; i < nblocks; i++)
    {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;

        hash ^= k;
        hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
    }

    const uint8_t *tail = (const uint8_t *)(key + nblocks * 4);
    uint32_t k1 = 0;

    switch (length & 3)
    {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = (k1 << r1) | (k1 >> (32 - r1));
        k1 *= c2;
        hash ^= k1;
    }

    hash ^= length;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

// Redimensiona la tabla hash cuando sea necesario
static void dict_resize(Dictionary *dict)
{
    size_t old_capacity = dict->capacity;
    SkillEntry **old_entries = dict->entries;

    // Duplicar la capacidad
    dict->capacity *= 2;
    dict->size = 0;
    dict->max_chain = 0;
    dict->entries = calloc(dict->capacity, sizeof(SkillEntry *));

    if (!dict->entries)
    {
        perror("Error al redimensionar la tabla hash");
        exit(EXIT_FAILURE);
    }

    // Reinsertar todos los elementos
    for (size_t i = 0; i < old_capacity; i++)
    {
        SkillEntry *entry = old_entries[i];
        while (entry)
        {
            SkillEntry *next = entry->next;
            size_t index = hash_string(entry->skill, strlen(entry->skill)) & (dict->capacity - 1);

            // Insertar al principio de la lista
            entry->next = dict->entries[index];
            dict->entries[index] = entry;
            dict->size++;

            // Actualizar longitud máxima de cadena
            size_t chain_length = 0;
            SkillEntry *e = dict->entries[index];
            while (e)
            {
                chain_length++;
                e = e->next;
            }
            if (chain_length > dict->max_chain)
            {
                dict->max_chain = chain_length;
            }

            entry = next;
        }
    }

    free(old_entries);
}

// Añade un offset a una habilidad en el diccionario
void dict_add_offset(Dictionary *dict, const char *skill, uint32_t offset)
{
    // Redimensionar si el factor de carga es demasiado alto o la cadena es muy larga
    if (dict->size >= dict->capacity * HASH_LOAD_FACTOR ||
        dict->max_chain > MAX_CHAIN_LENGTH)
    {
        dict_resize(dict);
    }

    size_t skill_len = strlen(skill);
    uint32_t hash = hash_string(skill, skill_len);
    size_t index = hash & (dict->capacity - 1);

    // Buscar la habilidad en la tabla hash
    SkillEntry *entry = dict->entries[index];
    SkillEntry *prev = NULL;
    size_t chain_length = 0;

    while (entry)
    {
        chain_length++;
        if (strcmp(entry->skill, skill) == 0)
        {
            break; // Encontrada
        }
        prev = entry;
        entry = entry->next;
    }

    // Si no se encontró, crear una nueva entrada
    if (!entry)
    {
        entry = malloc(sizeof(SkillEntry));
        if (!entry)
        {
            perror("Error al asignar memoria para nueva entrada");
            exit(EXIT_FAILURE);
        }

        strncpy(entry->skill, skill, MAX_SKILL_LENGTH - 1);
        entry->skill[MAX_SKILL_LENGTH - 1] = '\0';
        entry->count = 0;
        entry->capacity = 16; // Capacidad inicial mayor para reducir reubicaciones
        entry->offsets = malloc(entry->capacity * sizeof(uint32_t));
        entry->next = NULL;

        if (!entry->offsets)
        {
            perror("Error al asignar memoria para offsets");
            exit(EXIT_FAILURE);
        }

        // Insertar en la tabla hash
        if (prev)
        {
            prev->next = entry;
        }
        else
        {
            dict->entries[index] = entry;
        }

        dict->size++;
        chain_length++;
    }

    // Actualizar longitud máxima de cadena
    if (chain_length > dict->max_chain)
    {
        dict->max_chain = chain_length;
    }

    // Asegurar capacidad para el nuevo offset
    if (entry->count >= entry->capacity)
    {
        entry->capacity *= 2;
        entry->offsets = realloc(entry->offsets, entry->capacity * sizeof(uint32_t));

        if (!entry->offsets)
        {
            perror("Error al redimensionar los offsets");
            exit(EXIT_FAILURE);
        }
    }

    // Añadir el offset (ordenado para mejor compresión)
    size_t j = entry->count;
    while (j > 0 && entry->offsets[j - 1] > offset)
    {
        entry->offsets[j] = entry->offsets[j - 1];
        j--;
    }

    entry->offsets[j] = offset;
    entry->count++;
}

// Escribe el diccionario en un archivo comprimido
int dict_write_compressed(Dictionary *dict, const char *filename)
{
    // Primero, contar el número total de entradas
    size_t total_entries = 0;
    for (size_t i = 0; i < dict->capacity; i++)
    {
        SkillEntry *entry = dict->entries[i];
        while (entry)
        {
            if (entry->count > 0)
            {
                total_entries++;
            }
            entry = entry->next;
        }
    }

    // Crear un archivo temporal
    FILE *temp = tmpfile();
    long uncompressed_size = 0;

    if (!temp)
    {
        perror("Error al crear archivo temporal");
        return -1;
    }

    // 1. Escribir número de entradas
    fwrite(&total_entries, sizeof(size_t), 1, temp);

    // 2. Para cada entrada: longitud, habilidad, conteo, offsets
    for (size_t i = 0; i < dict->capacity; i++)
    {
        SkillEntry *entry = dict->entries[i];
        while (entry)
        {
            if (entry->count == 0)
            {
                entry = entry->next;
                continue;
            }

            // Escribir la habilidad (sin el '\0' final para ahorrar espacio)
            size_t skill_len = strlen(entry->skill);
            fwrite(&skill_len, sizeof(uint16_t), 1, temp);  // Usamos uint16_t que es suficiente para la longitud
            fwrite(entry->skill, 1, skill_len, temp);

            // Comprimir los offsets usando codificación delta
            if (entry->count > 0) {
                // Ordenar los offsets para mejor compresión
                qsort(entry->offsets, entry->count, sizeof(uint32_t), compare_uint32);
                
                // Calcular deltas
                uint32_t prev = entry->offsets[0];
                uint32_t *deltas = malloc(entry->count * sizeof(uint32_t));
                deltas[0] = prev;
                for (size_t j = 1; j < entry->count; j++) {
                    uint32_t delta = entry->offsets[j] - prev;
                    deltas[j] = delta;
                    prev = entry->offsets[j];
                }
                
                // Escribir el conteo y los deltas
                fwrite(&entry->count, sizeof(uint16_t), 1, temp);  // Usamos uint16_t que es suficiente
                fwrite(deltas, sizeof(uint32_t), entry->count, temp);
                free(deltas);
            } else {
                uint16_t zero = 0;
                fwrite(&zero, sizeof(uint16_t), 1, temp);
            }

            entry = entry->next;
        }
    }

    // Obtener el tamaño actual del archivo
    fseeko(temp, 0, SEEK_END);
    uncompressed_size = ftello(temp);
    fseeko(temp, 0, SEEK_SET);
    
    // Asignar buffer para los datos sin comprimir
    void *uncompressed_data = malloc(uncompressed_size);

    if (!uncompressed_data) {
        perror("Error al asignar memoria para datos sin comprimir");
        fclose(temp);
        return -1;
    }

    printf("Tamaño sin comprimir: %ld bytes\n", uncompressed_size);
    
    // Leer los datos del archivo temporal
    size_t bytes_read = fread(uncompressed_data, 1, uncompressed_size, temp);
    
    // Cerrar el archivo temporal (se eliminará automáticamente)
    fclose(temp);
    
    if (bytes_read != (size_t)uncompressed_size) {
        fprintf(stderr, "Error al leer datos del archivo temporal: leídos %zu de %ld bytes\n", 
                bytes_read, uncompressed_size);
        free(uncompressed_data);
        return -1;
    }

    printf("Tamaño leido: %ld bytes\n", bytes_read);
    
    size_t compressed_bound = ZSTD_compressBound(uncompressed_size);
    void *compressed_data = malloc(compressed_bound);
    
    // Crear un contexto de compresión reutilizable para mejor rendimiento
    ZSTD_CCtx* const cctx = ZSTD_createCCtx();

    if (!cctx) {
        perror("Error al crear el contexto de compresión");
        free(uncompressed_data);
        free(compressed_data);

        return -1;
    }
    if (!compressed_data) {
        perror("Error al asignar memoria para compresión");
        free(uncompressed_data);

        return -1;
    }

    printf("Memoria asignada para compresión: %ld bytes\n", compressed_bound);

    // Verificar que hay datos para comprimir
    if (uncompressed_size == 0)
    {
        fprintf(stderr, "Error: No hay datos para comprimir\n");
        free(compressed_data);
        free(uncompressed_data);

        return -1;
    }

    // Esta parte puede tomar mas de 20 minutos, dependiendo de la compresión
    printf("Tamaño de datos a comprimir: %ld bytes\n", uncompressed_size);
    printf("Iniciando compresión con nivel %d...\n", COMPRESSION_LEVEL);

    // Usar el contexto de compresión con el nivel deseado
    size_t compressed_size = ZSTD_compressCCtx(cctx,
                                             compressed_data, compressed_bound,
                                             uncompressed_data, uncompressed_size,
                                             COMPRESSION_LEVEL);
    
    // Liberar el contexto de compresión
    ZSTD_freeCCtx(cctx);

    // Liberar memoria de los datos sin comprimir
    free(uncompressed_data);

    if (ZSTD_isError(compressed_size))
    {
        fprintf(stderr, "Error en compresión: %s\n", ZSTD_getErrorName(compressed_size));
        fprintf(stderr, "Tamaño de entrada: %ld, Tamaño de salida máximo: %zu\n",
                uncompressed_size, compressed_bound);
        free(compressed_data);

        return -1;
    }

    printf("Compresión completada. Tamaño comprimido: %ld bytes\n", compressed_size);

    // El directorio de salida ya existe, no es necesario crearlo

    // Escribir el archivo final
    printf("Escribiendo archivo de salida: %s\n", filename);
    FILE *out = fopen(filename, "wb");

    if (!out)
    {
        perror("Error al crear archivo de salida");
        fprintf(stderr, "No se pudo abrir el archivo: %s\n", filename);
        free(compressed_data);

        return -1;
    }
    // Escribir cabecera: tamaño sin comprimir, tamaño comprimido, luego datos
    fwrite(&uncompressed_size, sizeof(uncompressed_size), 1, out);
    fwrite(compressed_data, 1, compressed_size, out);

    printf("Archivo comprimido guardado en %s\n", filename);

    // Limpieza
    printf("Liberando memoria...\n");
    free(compressed_data);
    printf("Cerrando archivos...\n");
    fclose(out);
    printf("Compresión completada.\n");

    return 0;
}

// Variables globales
Dictionary skill_dict;

int main()
{
    struct timespec start_time, end_time;
    unsigned int line_count = 0;
    // Calcular y mostrar estadísticas
    char time_str[100];

    clock_gettime(CLOCK_MONOTONIC, &start_time);
    printf("Iniciando proceso de indexado...\n");
    // Inicializar el diccionario
    dict_init(&skill_dict);
    printf("Procesando archivo CSV...\n");

    FILE *file_csv = fopen("data.csv", "r");

    if (!file_csv)
    {
        perror("Error al abrir data.csv");
        return 1;
    }

    char line_buffer[4096];
    uint32_t current_offset = 0;
    size_t total_lines = 0;
    size_t processed_lines = 0;

    // Primera pasada: contar líneas para estimar capacidad
    while (fgets(line_buffer, sizeof(line_buffer), file_csv))
    {
        total_lines++;
    }

    rewind(file_csv);
    printf("Procesando %zu ofertas...\n", total_lines);

    // Tiempo de inicio del procesamiento
    struct timespec process_start, process_end;

    clock_gettime(CLOCK_MONOTONIC, &process_start);

    // Pre-allocate a buffer for trimmed skills to avoid repeated allocations
    char skill_buffer[256];

    // Segunda pasada: procesar datos
    while (fgets(line_buffer, sizeof(line_buffer), file_csv))
    {
        uint32_t line_start = current_offset;
        size_t line_len = strlen(line_buffer);

        // Find the first comma (separator between ID and skills)
        char *skills_part = strchr(line_buffer, ',');
        if (!skills_part)
        {
            current_offset += line_len;
            line_count++;
            continue;
        }

        skills_part++; // Avanzar tras el separador

        // Parsear manualmente los campos de habilidades
        char *current = skills_part;
        while (*current)
        {
            // Salta espacios en blanco al inicio
            while (*current && isspace((unsigned char)*current))
                current++;
            if (!*current)
                break;

            char *start = current;
            int in_quotes = 0;

            // Encuentra el final del campo actual
            while (*current)
            {
                if (*current == '"')
                {
                    in_quotes = !in_quotes;
                }
                else if ((*current == ',' && !in_quotes) || *current == '\n' || *current == '\r')
                {
                    break;
                }
                current++;
            }

            // Limpiar espacios en blanco al final
            char *end = current;
            while (end > start && isspace((unsigned char)*(end - 1)))
                end--;

            // Validar longitud del skill
            if (end > start)
            {
                size_t skill_len = end - start;
                if (skill_len >= sizeof(skill_buffer))
                    skill_len = sizeof(skill_buffer) - 1;

                // Copiar y terminar en nulo
                strncpy(skill_buffer, start, skill_len);
                skill_buffer[skill_len] = '\0';

                // Eliminar comillas dobles al inicio y final si existen
                char *skill = skill_buffer;
                size_t len = strlen(skill);
                if (len >= 2 && skill[0] == '"' && skill[len-1] == '"') {
                    // Quitar comillas dobles
                    skill[len-1] = '\0';
                    skill++;
                }

                // Añadir la habilidad al diccionario con el offset actual
                if (*skill != '\0') {  // Solo añadir si no está vacío después de quitar comillas
                    dict_add_offset(&skill_dict, skill, line_start);
                }
            }

            if (*current == ',')
                current++;
            if (*current == '\0')
                break;
        }

        current_offset += line_len;

        // Mostrar progreso cada 1,100,000 líneas
        if (++processed_lines % 1000000 == 0)
        {
            printf("%zu ofertas procesadas...\n", processed_lines);
        }
    }

    fclose(file_csv);
    // Fin del procesamiento
    clock_gettime(CLOCK_MONOTONIC, &process_end);
    format_time(time_str, sizeof(time_str), &process_start, &process_end);
    printf("Tiempo de procesamiento: %s\n", time_str);

    struct timespec write_start, write_end;

    // Inicio compresion
    printf("Escribiendo índice comprimido...\n");
    clock_gettime(CLOCK_MONOTONIC, &write_start);

    int compressed = dict_write_compressed(&skill_dict, "dist/jobs.idx.zst");

    // Fin de escritura
    clock_gettime(CLOCK_MONOTONIC, &write_end);
    format_time(time_str, sizeof(time_str), &write_start, &write_end);
    printf("Tiempo de escritura del índice: %s\n", time_str);

    if (compressed != 0)
    {
        fprintf(stderr, "Error al escribir el índice comprimido\n");
        return 1;
    }

    // Fin total
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    format_time(time_str, sizeof(time_str), &start_time, &end_time);
    printf("Tiempo total de indexado: %s\n", time_str);

    // Liberar memoria
    for (size_t i = 0; i < skill_dict.capacity; i++)
    {
        SkillEntry *entry = skill_dict.entries[i];

        while (entry)
        {
            SkillEntry *next = entry->next;

            if (entry->offsets)
            {
                free(entry->offsets);
            }

            free(entry);
            entry = next;
        }
    }

    free(skill_dict.entries);
    skill_dict.size = 0;
    skill_dict.capacity = 0;

    printf("Proceso completado. Índice optimizado creado en 'dist/jobs.idx.zst'\n");

    return 0;
}
