#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <zstd.h>

// --- Definiciones ---
#define INITIAL_CAPACITY 1000000  // Capacidad inicial del diccionario
#define MAX_SKILL_LENGTH 256
#define COMPRESSION_LEVEL 3

// Estructura para almacenar una habilidad con sus offsets
typedef struct {
    char skill[MAX_SKILL_LENGTH];
    uint32_t* offsets;     // Array de offsets
    size_t count;          // Número de offsets
    size_t capacity;       // Capacidad actual del array
} SkillEntry;

// Estructura principal del diccionario
typedef struct {
    SkillEntry* entries;   // Array de entradas
    size_t size;           // Número de entradas usadas
    size_t capacity;       // Capacidad total del array
} Dictionary;

// Prototipos de funciones
void dict_init(Dictionary* dict);
void dict_add_offset(Dictionary* dict, const char* skill, uint32_t offset);
void dict_write_compressed(Dictionary* dict, const char* filename);
void dict_free(Dictionary* dict);
char* trim_whitespace(char* str);
unsigned long hash_string(const char* str);

// Variables globales
Dictionary skill_dict;

// Implementación

int main() {
    // Inicializar el diccionario
    dict_init(&skill_dict);
    
    printf("Procesando archivo CSV...\n");
    
    FILE* file_csv = fopen("data.csv", "r");
    if (!file_csv) {
        perror("Error al abrir data.csv");
        return 1;
    }

    char line_buffer[4096];
    uint32_t current_offset = 0;
    size_t line_count = 0;

    // Primera pasada: contar líneas para estimar capacidad
    while (fgets(line_buffer, sizeof(line_buffer), file_csv)) {
        line_count++;
    }
    rewind(file_csv);
    
    printf("Procesando %zu ofertas...\n", line_count);

    // Segunda pasada: procesar datos
    while (fgets(line_buffer, sizeof(line_buffer), file_csv)) {
        // 1. Guarda la posición inicial de la línea actual
        uint32_t line_start = current_offset;
        size_t line_len = strlen(line_buffer);
        
        // 2. Encuentra la primera coma (separador entre ID y habilidades)
        char* skills_part = strchr(line_buffer, ',');
        if (skills_part) {
            skills_part++; // Avanza más allá de la coma
            
            // 3. Divide la cadena usando comas y saltos de línea como delimitadores
            char* token = strtok(skills_part, "\",\n");
            while (token != NULL) {
                // 4. Elimina espacios en blanco al inicio y final
                char* trimmed_skill = trim_whitespace(token);
                if (strlen(trimmed_skill) > 0) {
                    // 5. Añade la habilidad al diccionario con el offset de la línea
                    dict_add_offset(&skill_dict, trimmed_skill, line_start);
                }
                token = strtok(NULL, "\",\n"); // Siguiente token
            }
        }
        
        // 6. Actualiza el offset para la siguiente línea
        current_offset += line_len;
        
        // 7. Muestra el progreso cada 10,000 líneas
        if (line_count > 0 && (line_count % 10000 == 0)) {
            size_t remaining = line_count - (line_count / 10000) * 10000;
            
            printf("Procesadas %zu ofertas (%zu faltan)...\n", line_count, remaining);
        }

        line_count++;
    }
    
    fclose(file_csv);
    
    printf("Escribiendo índice comprimido...\n");
    dict_write_compressed(&skill_dict, "dist/jobs.idx.zst");
    
    // Liberar memoria
    dict_free(&skill_dict);
    
    printf("Proceso completado. Índice optimizado creado en 'dist/jobs.idx.zst'\n");
    return 0;
}

// Inicializa un diccionario vacío
void dict_init(Dictionary* dict) {
    dict->size = 0;
    dict->capacity = INITIAL_CAPACITY;
    dict->entries = calloc(dict->capacity, sizeof(SkillEntry));
    if (!dict->entries) {
        perror("Error al inicializar el diccionario");
        exit(EXIT_FAILURE);
    }
}

// Añade un offset a una habilidad en el diccionario
void dict_add_offset(Dictionary* dict, const char* skill, uint32_t offset) {
    // Buscar la habilidad en el diccionario
    size_t i;
    int found = 0;
    
    for (i = 0; i < dict->size; i++) {
        if (strcmp(dict->entries[i].skill, skill) == 0) {
            found = 1;
            break;
        }
    }
    
    // Si no se encontró, agregar una nueva entrada
    if (!found) {
        // Redimensionar si es necesario
        if (dict->size >= dict->capacity) {
            dict->capacity *= 2;
            dict->entries = realloc(dict->entries, dict->capacity * sizeof(SkillEntry));
            if (!dict->entries) {
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
        if (!dict->entries[i].offsets) {
            perror("Error al asignar memoria para offsets");
            exit(EXIT_FAILURE);
        }
    }
    
    // Asegurar capacidad para el nuevo offset
    if (dict->entries[i].count >= dict->entries[i].capacity) {
        dict->entries[i].capacity *= 2;
        dict->entries[i].offsets = realloc(dict->entries[i].offsets, 
                                         dict->entries[i].capacity * sizeof(uint32_t));
        if (!dict->entries[i].offsets) {
            perror("Error al redimensionar los offsets");
            exit(EXIT_FAILURE);
        }
    }
    
    // Añadir el offset (ordenado para mejor compresión)
    size_t j = dict->entries[i].count;
    while (j > 0 && dict->entries[i].offsets[j-1] > offset) {
        dict->entries[i].offsets[j] = dict->entries[i].offsets[j-1];
        j--;
    }
    dict->entries[i].offsets[j] = offset;
    dict->entries[i].count++;
}

// Escribe el diccionario en un archivo comprimido
void dict_write_compressed(Dictionary* dict, const char* filename) {
    // Primero escribir a un buffer en memoria
    FILE* temp = open_memstream(NULL, &(size_t){0});
    if (!temp) {
        perror("Error al crear buffer temporal");
        return;
    }
    
    // 1. Escribir número de entradas
    fwrite(&dict->size, sizeof(size_t), 1, temp);
    
    // 2. Para cada entrada: longitud, habilidad, conteo, offsets
    for (size_t i = 0; i < dict->size; i++) {
        uint8_t skill_len = (uint8_t)strnlen(dict->entries[i].skill, MAX_SKILL_LENGTH);
        fwrite(&skill_len, sizeof(uint8_t), 1, temp);
        fwrite(dict->entries[i].skill, 1, skill_len, temp);
        
        // Escribir conteo de offsets (usando variable byte encoding)
        uint32_t count = dict->entries[i].count;
        do {
            uint8_t byte = count & 0x7F;
            count >>= 7;
            if (count) byte |= 0x80;
            fputc(byte, temp);
        } while (count);
        
        // Escribir offsets (usando delta encoding)
        uint32_t prev = 0;
        for (size_t j = 0; j < dict->entries[i].count; j++) {
            uint32_t delta = dict->entries[i].offsets[j] - prev;
            prev = dict->entries[i].offsets[j];
            
            // Variable byte encoding para delta
            do {
                uint8_t byte = delta & 0x7F;
                delta >>= 7;
                if (delta) byte |= 0x80;
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
    void* compressed_data = malloc(compressed_bound);
    if (!compressed_data) {
        perror("Error al asignar memoria para compresión");
        fclose(temp);
        return;
    }
    
    size_t compressed_size = ZSTD_compress(
        compressed_data, compressed_bound,
        temp, uncompressed_size,
        COMPRESSION_LEVEL
    );
    
    if (ZSTD_isError(compressed_size)) {
        fprintf(stderr, "Error en compresión: %s\n", ZSTD_getErrorName(compressed_size));
        free(compressed_data);
        fclose(temp);
        return;
    }
    
    // Escribir el archivo final
    FILE* out = fopen(filename, "wb");
    if (!out) {
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
void dict_free(Dictionary* dict) {
    for (size_t i = 0; i < dict->size; i++) {
        free(dict->entries[i].offsets);
    }
    free(dict->entries);
    dict->size = 0;
    dict->capacity = 0;
}

// Función hash para cadenas (djb2)
unsigned long hash_string(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}


// Elimina espacios en blanco al inicio y final de una cadena
char* trim_whitespace(char* str) {
    if (!str || !*str) return str;
    
    // Trim leading space
    while (isspace((unsigned char)*str)) str++;
    if (*str == '\0') return str;
    
    // Trim trailing space
    char* end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    
    return str;
}
