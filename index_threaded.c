#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>
#include <zstd.h>
#include "utils.h"

// --- Definiciones ---
#define INITIAL_CAPACITY 1000000 // Capacidad inicial del diccionario
#define MAX_SKILL_LENGTH 256
#define COMPRESSION_LEVEL 3
#define NUM_THREADS 4  // Número de hilos a utilizar

// Estructura para almacenar una habilidad con sus offsets
typedef struct
{
    char skill[MAX_SKILL_LENGTH];
    uint32_t *offsets; // Array de offsets
    size_t count;      // Número de offsets
    size_t capacity;   // Capacidad actual del array
} SkillEntry;

// Estructura principal del diccionario
typedef struct
{
    SkillEntry *entries; // Array de entradas
    size_t size;         // Número de entradas usadas
    size_t capacity;     // Capacidad total del array
} Dictionary;

// Estructura para los argumentos de los hilos
typedef struct {
    FILE* file;
    off_t start_offset;
    off_t end_offset;
    size_t lines_processed;
    Dictionary* local_dict;
} ThreadArgs;

// Prototipos de funciones
void dict_init(Dictionary *dict);
void dict_add_offset(Dictionary *dict, const char *skill, uint32_t offset);
void dict_write_compressed(Dictionary *dict, const char *filename);
void dict_free(Dictionary *dict);
void* process_chunk(void* args);
unsigned long hash_string(const char *str);

// Variables globales
Dictionary skill_dict;

int main()
{
    struct timespec start_time, end_time;

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

    // Segunda pasada: procesar datos en paralelo
    const int num_threads = NUM_THREADS;
    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];
    Dictionary thread_dicts[num_threads];
    
    // Obtener el tamaño total del archivo
    fseek(file_csv, 0, SEEK_END);
    off_t file_size = ftell(file_csv);
    rewind(file_csv);
    
    // Inicializar diccionarios locales para cada hilo
    for (int i = 0; i < num_threads; i++) {
        dict_init(&thread_dicts[i]);
    }
    
    // Crear hilos
    for (int i = 0; i < num_threads; i++) {
        thread_args[i].file = file_csv;
        thread_args[i].start_offset = (i * file_size) / num_threads;
        thread_args[i].end_offset = ((i + 1) * file_size) / num_threads;
        thread_args[i].local_dict = &thread_dicts[i];
        thread_args[i].lines_processed = 0;
        
        pthread_create(&threads[i], NULL, process_chunk, &thread_args[i]);
    }
    
    // Esperar a que terminen todos los hilos
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        processed_lines += thread_args[i].lines_processed;
    }
    
    // Combinar los diccionarios de los hilos en el diccionario principal
    for (int i = 0; i < num_threads; i++) {
        for (size_t j = 0; j < thread_dicts[i].size; j++) {
            SkillEntry *entry = &thread_dicts[i].entries[j];
            if (entry->count > 0) {
                // Copiar las entradas al diccionario global
                for (size_t k = 0; k < entry->count; k++) {
                    dict_add_offset(&skill_dict, entry->skill, entry->offsets[k]);
                }
            }
        }
    }
    
    // Liberar memoria de los diccionarios locales
    for (int i = 0; i < num_threads; i++) {
        dict_free(&thread_dicts[i]);
    }
    
    printf("Se procesaron %zu ofertas en total.\n", processed_lines);
    
    // Cerrar el archivo
    fclose(file_csv);

    printf("Escribiendo índice comprimido...\n");
    // Tiempo de fin del procesamiento
    clock_gettime(CLOCK_MONOTONIC, &process_end);
    printf("Escribiendo índice comprimido...\n");

    struct timespec write_start, write_end;

    clock_gettime(CLOCK_MONOTONIC, &write_start);
    dict_write_compressed(&skill_dict, "dist/jobs.idx.zst");
    // Tiempo de fin de escritura
    clock_gettime(CLOCK_MONOTONIC, &write_end);
    // Tiempo total
    clock_gettime(CLOCK_MONOTONIC, &end_time);

    // Calcular y mostrar estadísticas
    char time_str[100];

    format_time(time_str, sizeof(time_str), &process_start, &process_end);
    printf("Tiempo de procesamiento: %s\n", time_str);
    format_time(time_str, sizeof(time_str), &write_start, &write_end);
    printf("Tiempo de escritura del índice: %s\n", time_str);
    format_time(time_str, sizeof(time_str), &start_time, &end_time);
    printf("Tiempo total de indexado: %s\n", time_str);
    // Liberar memoria
    dict_free(&skill_dict);
    printf("Proceso completado. Índice optimizado creado en 'dist/jobs.idx.zst'\n");

    return 0;
}

// Inicializa un diccionario vacío
void dict_init(Dictionary *dict)
{
    dict->size = 0;
    dict->capacity = INITIAL_CAPACITY;
    dict->entries = calloc(dict->capacity, sizeof(SkillEntry));

    if (!dict->entries)
    {
        perror("Error al inicializar el diccionario");
        exit(EXIT_FAILURE);
    }
}

// Añade un offset a una habilidad en el diccionario
void dict_add_offset(Dictionary *dict, const char *skill, uint32_t offset)
{
    // Buscar la habilidad en el diccionario
    size_t i;
    int found = 0;

    for (i = 0; i < dict->size; i++)
    {
        if (strcmp(dict->entries[i].skill, skill) == 0)
        {
            found = 1;

            break;
        }
    }

    // Si no se encontró, agregar una nueva entrada
    if (!found)
    {
        // Redimensionar si es necesario
        if (dict->size >= dict->capacity)
        {
            dict->capacity *= 2;
            dict->entries = realloc(dict->entries, dict->capacity * sizeof(SkillEntry));

            if (!dict->entries)
            {
                perror("Error al redimensionar el diccionario");
                exit(EXIT_FAILURE);
            }
        }

        // Inicializar nueva entrada
        i = dict->size++;
        strncpy(dict->entries[i].skill, skill, MAX_SKILL_LENGTH - 1);
        dict->entries[i].skill[MAX_SKILL_LENGTH - 1] = '\0';
        dict->entries[i].count = 0;
        dict->entries[i].capacity = 8; // Capacidad inicial para offsets
        dict->entries[i].offsets = malloc(dict->entries[i].capacity * sizeof(uint32_t));

        if (!dict->entries[i].offsets)
        {
            perror("Error al asignar memoria para offsets");
            exit(EXIT_FAILURE);
        }
    }

    // Asegurar capacidad para el nuevo offset
    if (dict->entries[i].count >= dict->entries[i].capacity)
    {
        dict->entries[i].capacity *= 2;
        dict->entries[i].offsets = realloc(dict->entries[i].offsets, dict->entries[i].capacity * sizeof(uint32_t));

        if (!dict->entries[i].offsets)
        {
            perror("Error al redimensionar los offsets");
            exit(EXIT_FAILURE);
        }
    }

    // Añadir el offset (ordenado para mejor compresión)
    size_t j = dict->entries[i].count;

    while (j > 0 && dict->entries[i].offsets[j - 1] > offset)
    {
        dict->entries[i].offsets[j] = dict->entries[i].offsets[j - 1];
        j--;
    }

    dict->entries[i].offsets[j] = offset;
    dict->entries[i].count++;
}

// Escribe el diccionario en un archivo comprimido
void dict_write_compressed(Dictionary *dict, const char *filename)
{
    // Primero escribir a un buffer en memoria
    FILE *temp = open_memstream(NULL, &(size_t){0});

    if (!temp)
    {
        perror("Error al crear buffer temporal");

        return;
    }

    // 1. Escribir número de entradas
    fwrite(&dict->size, sizeof(size_t), 1, temp);

    // 2. Para cada entrada: longitud, habilidad, conteo, offsets
    for (size_t i = 0; i < dict->size; i++)
    {
        uint8_t skill_len = (uint8_t)strnlen(dict->entries[i].skill, MAX_SKILL_LENGTH);

        fwrite(&skill_len, sizeof(uint8_t), 1, temp);
        fwrite(dict->entries[i].skill, 1, skill_len, temp);

        // Escribir conteo de offsets (usando variable byte encoding)
        uint32_t count = dict->entries[i].count;

        do
        {
            uint8_t byte = count & 0x7F;
            count >>= 7;
            if (count)
                byte |= 0x80;
            fputc(byte, temp);
        } while (count);

        // Escribir offsets (usando delta encoding)
        uint32_t prev = 0;

        for (size_t j = 0; j < dict->entries[i].count; j++)
        {
            uint32_t delta = dict->entries[i].offsets[j] - prev;

            prev = dict->entries[i].offsets[j];

            // Variable byte encoding para delta
            do
            {
                uint8_t byte = delta & 0x7F;

                delta >>= 7;

                if (delta)
                {
                    byte |= 0x80;
                }

                fputc(byte, temp);
            } while (delta);
        }
    }

    // Obtener el buffer de memoria
    fflush(temp);

    long uncompressed_size = ftell(temp);

    rewind(temp);

    // Comprimir con Zstandard
    size_t compressed_bound = ZSTD_compressBound(uncompressed_size);
    void *compressed_data = malloc(compressed_bound);

    if (!compressed_data)
    {
        perror("Error al asignar memoria para compresión");
        fclose(temp);
        return;
    }

    size_t compressed_size = ZSTD_compress(
        compressed_data, compressed_bound,
        temp, uncompressed_size,
        COMPRESSION_LEVEL);

    if (ZSTD_isError(compressed_size))
    {
        fprintf(stderr, "Error en compresión: %s\n", ZSTD_getErrorName(compressed_size));
        free(compressed_data);
        fclose(temp);

        return;
    }

    // Escribir el archivo final
    FILE *out = fopen(filename, "wb");

    if (!out)
    {
        perror("Error al crear archivo de salida");
        free(compressed_data);
        fclose(temp);

        return;
    }

    // Escribir cabecera: tamaño sin comprimir, tamaño comprimido, luego datos
    fwrite(&uncompressed_size, sizeof(uncompressed_size), 1, out);
    fwrite(compressed_data, 1, compressed_size, out);
    // Limpieza
    free(compressed_data);
    fclose(temp);
    fclose(out);
}

// Libera la memoria del diccionario
void dict_free(Dictionary *dict)
{
    for (size_t i = 0; i < dict->size; i++)
    {
        free(dict->entries[i].offsets);
    }

    free(dict->entries);
    dict->size = 0;
    dict->capacity = 0;
}

// Función para procesar un chunk del archivo en un hilo
void* process_chunk(void* args) {
    ThreadArgs* targs = (ThreadArgs*)args;
    
    // Cada hilo necesita su propio FILE* para evitar problemas de concurrencia
    FILE* file = fdopen(dup(fileno(targs->file)), "r");
    if (!file) {
        perror("Error al duplicar el descriptor de archivo");
        return NULL;
    }
    
    char line_buffer[4096];
    size_t lines_processed = 0;
    off_t current_offset = targs->start_offset;
    
    // Mover el puntero al inicio del chunk
    fseeko(file, targs->start_offset, SEEK_SET);
    
    // Si no es el primer chunk, avanzar hasta la siguiente línea completa
    if (targs->start_offset != 0) {
        while (fgets(line_buffer, sizeof(line_buffer), file)) {
            current_offset = ftello(file);
            size_t len = strlen(line_buffer);
            if (len > 0 && line_buffer[len-1] == '\n') {
                break;
            }
        }
    }
    
    // Procesar líneas hasta alcanzar el final del chunk
    while (current_offset < targs->end_offset && fgets(line_buffer, sizeof(line_buffer), file)) {
        char *current = line_buffer;
        char *line_start = current;
        size_t line_len = strlen(line_buffer);
        
        // Procesar cada habilidad en la línea
        while (*current) {
            // Saltar espacios iniciales
            while (isspace((unsigned char)*current)) current++;
            if (*current == '"') current++;
            
            char *start = current;
            while (*current && *current != ',' && *current != '"') current++;
            
            if (current > start) {
                size_t skill_len = current - start;
                char skill_buffer[256];
                
                strncpy(skill_buffer, start, skill_len);
                skill_buffer[skill_len] = '\0';
                
                // Añadir al diccionario local
                dict_add_offset(targs->local_dict, skill_buffer, current_offset + (start - line_start));
            }
            
            if (*current == ',') current++;
            if (*current == '\0') break;
        }
        
        current_offset = ftello(file);
        lines_processed++;

        // Mostrar progreso cada 10,000 líneas
        if (lines_processed % 10000 == 0) {
            printf("Procesadas %zu líneas en el chunk %ld...\n", lines_processed, targs->start_offset);
        } 
    }
    
    targs->lines_processed = lines_processed;
    fclose(file);
    return NULL;
}

// Función hash para cadenas (djb2)
unsigned long hash_string(const char *str)
{
    unsigned long hash = 5381;
    int c;

    while ((c = *str++))
    {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }

    return hash;
}
