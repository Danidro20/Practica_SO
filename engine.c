#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <zstd.h>

// --- Definiciones ---
#define MAX_SKILL_LENGTH 256
#define INITIAL_OFFSETS_CAPACITY 1000

// Estructura para almacenar una habilidad con sus offsets
typedef struct {
    char skill[MAX_SKILL_LENGTH];
    uint32_t* offsets;     // Array de offsets
    size_t count;          // Número de offsets
} SkillEntry;

// Estructura principal del diccionario
typedef struct {
    SkillEntry* entries;   // Array de entradas
    size_t size;           // Número de entradas
} Dictionary;

// Variables globales
Dictionary skill_dict;

// --- Nombres de las tuberías ---
#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

// --- Prototipos ---
void load_compressed_index(const char* filename);
void search_and_respond(int query_fd, int result_fd);
void cleanup(int signum);
void free_dictionary();
int find_skill_offsets(const char* skill, uint32_t** offsets, size_t* count);

// --- Implementación del Motor ---
int main() {
    printf("Motor de búsqueda multi-criterio optimizado iniciando...\n");
    signal(SIGINT, cleanup);

    // Inicializar diccionario
    skill_dict.entries = NULL;
    skill_dict.size = 0;

    // Cargar el índice comprimido
    load_compressed_index("dist/jobs.idx.zst");
    printf("Índice optimizado cargado en memoria. %zu habilidades indexadas.\n", skill_dict.size);

    // Crear tuberías para comunicación
    umask(0);
    mkfifo(QUERY_PIPE, 0666);
    mkfifo(RESULT_PIPE, 0666);
    printf("Tuberías creadas. Esperando peticiones...\n");

    while (1) {
        int query_fd = open(QUERY_PIPE, O_RDONLY);
        int result_fd = open(RESULT_PIPE, O_WRONLY);
        if (query_fd == -1 || result_fd == -1) {
            perror("Error al abrir las tuberías");
            cleanup(0);
            exit(EXIT_FAILURE);
        }
        search_and_respond(query_fd, result_fd);
        close(query_fd);
        close(result_fd);
    }
    
    return 0;
}

// Función para leer un entero con codificación variable
static uint32_t read_varint(FILE* f) {
    uint32_t result = 0;
    uint8_t byte;
    int shift = 0;
    
    do {
        if (fread(&byte, 1, 1, f) != 1) {
            perror("Error al leer entero variable");
            exit(EXIT_FAILURE);
        }
        result |= (byte & 0x7F) << shift;
        shift += 7;
    } while (byte & 0x80);
    
    return result;
}

// Carga el índice comprimido en memoria
void load_compressed_index(const char* filename) {
    printf("Cargando índice comprimido...\n");
    
    // Leer el archivo completo
    FILE* f = fopen(filename, "rb");
    if (!f) {
        perror("No se pudo abrir el archivo de índice");
        exit(EXIT_FAILURE);
    }
    
    // Obtener tamaño del archivo
    fseek(f, 0, SEEK_END);
    size_t file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (file_size < sizeof(size_t)) {
        fprintf(stderr, "Archivo de índice corrupto (demasiado pequeño)");
        exit(EXIT_FAILURE);
    }
    
    // Leer el tamaño descomprimido
    size_t uncompressed_size;
    if (fread(&uncompressed_size, sizeof(uncompressed_size), 1, f) != 1) {
        perror("Error al leer el tamaño descomprimido");
        fclose(f);
        exit(EXIT_FAILURE);
    }
    
    // Leer los datos comprimidos
    size_t compressed_size = file_size - sizeof(uncompressed_size);
    void* compressed_data = malloc(compressed_size);
    if (!compressed_data) {
        perror("Error al asignar memoria para datos comprimidos");
        fclose(f);
        exit(EXIT_FAILURE);
    }
    
    if (fread(compressed_data, 1, compressed_size, f) != compressed_size) {
        perror("Error al leer datos comprimidos");
        free(compressed_data);
        fclose(f);
        exit(EXIT_FAILURE);
    }
    fclose(f);
    
    // Descomprimir los datos
    void* decompressed_data = malloc(uncompressed_size);
    if (!decompressed_data) {
        perror("Error al asignar memoria para datos descomprimidos");
        free(compressed_data);
        exit(EXIT_FAILURE);
    }
    
    size_t decompressed_size = ZSTD_decompress(
        decompressed_data, uncompressed_size,
        compressed_data, compressed_size
    );
    
    free(compressed_data);
    
    if (ZSTD_isError(decompressed_size)) {
        fprintf(stderr, "Error al descomprimir: %s\n", ZSTD_getErrorName(decompressed_size));
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }
    
    // Procesar el buffer descomprimido
    FILE* mem = fmemopen(decompressed_data, decompressed_size, "rb");
    if (!mem) {
        perror("Error al crear stream de memoria");
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }
    
    // Leer número de entradas
    size_t num_entries;
    if (fread(&num_entries, sizeof(size_t), 1, mem) != 1) {
        perror("Error al leer número de entradas");
        fclose(mem);
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }
    
    // Asignar memoria para las entradas
    skill_dict.entries = calloc(num_entries, sizeof(SkillEntry));
    if (!skill_dict.entries) {
        perror("Error al asignar memoria para entradas");
        fclose(mem);
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }
    skill_dict.size = num_entries;
    
    // Leer cada entrada
    for (size_t i = 0; i < num_entries; i++) {
        // Leer longitud de la habilidad
        uint8_t skill_len;
        if (fread(&skill_len, sizeof(uint8_t), 1, mem) != 1) {
            fprintf(stderr, "Error al leer longitud de habilidad %zu\n", i);
            goto cleanup_error;
        }
        
        // Leer la habilidad
        if (fread(skill_dict.entries[i].skill, 1, skill_len, mem) != skill_len) {
            fprintf(stderr, "Error al leer habilidad %zu\n", i);
            goto cleanup_error;
        }
        skill_dict.entries[i].skill[skill_len] = '\0';
        
        // Leer número de offsets
        uint32_t num_offsets = read_varint(mem);
        
        // Reservar memoria para los offsets
        skill_dict.entries[i].count = num_offsets;
        skill_dict.entries[i].offsets = malloc(num_offsets * sizeof(uint32_t));
        if (!skill_dict.entries[i].offsets) {
            perror("Error al asignar memoria para offsets");
            goto cleanup_error;
        }
        
        // Leer offsets (delta encoding)
        uint32_t prev = 0;
        for (uint32_t j = 0; j < num_offsets; j++) {
            uint32_t delta = read_varint(mem);
            prev += delta;
            skill_dict.entries[i].offsets[j] = prev;
        }
    }
    
    fclose(mem);
    free(decompressed_data);
    return;
    
clean_error:
    fclose(mem);
    free(decompressed_data);
    free_dictionary();
    exit(EXIT_FAILURE);
}

// Busca una habilidad en el diccionario y devuelve sus offsets
int find_skill_offsets(const char* skill, uint32_t** offsets, size_t* count) {
    // Búsqueda lineal (podría mejorarse con búsqueda binaria si el diccionario está ordenado)
    for (size_t i = 0; i < skill_dict.size; i++) {
        if (strcmp(skill_dict.entries[i].skill, skill) == 0) {
            *offsets = skill_dict.entries[i].offsets;
            *count = skill_dict.entries[i].count;
            return 1; // Encontrado
        }
    }
    return 0; // No encontrado
}

// Encuentra la intersección de dos listas ordenadas de offsets
size_t intersect_offsets(const uint32_t* a, size_t a_len, 
                        const uint32_t* b, size_t b_len,
                        uint32_t** result) {
    size_t i = 0, j = 0, k = 0;
    uint32_t* intersection = malloc((a_len < b_len ? a_len : b_len) * sizeof(uint32_t));
    
    while (i < a_len && j < b_len) {
        if (a[i] < b[j]) {
            i++;
        } else if (a[i] > b[j]) {
            j++;
        } else {
            // Para evitar duplicados
            if (k == 0 || intersection[k-1] != a[i]) {
                intersection[k++] = a[i];
            }
            i++;
            j++;
        }
    }
    
    *result = intersection;
    return k;
}

// Lógica principal de búsqueda con intersección
void search_and_respond(int query_fd, int result_fd) {
    char query_buffer[1024];
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);

    if (bytes_read <= 0) return;
    query_buffer[bytes_read] = '\0';
    printf("Petición recibida: '%s'\n", query_buffer);

    char* criteria[3] = {NULL, NULL, NULL};
    int criteria_count = 0;

    // Dividir la consulta por punto y coma
    char* token = strtok(query_buffer, ";");
    while (token != NULL && criteria_count < 3) {
        // Eliminar espacios en blanco al inicio y final
        while (isspace((unsigned char)*token)) token++;
        char* end = token + strlen(token) - 1;
        while (end > token && isspace((unsigned char)*end)) end--;
        *(end + 1) = '\0';
        
        if (*token) {  // Solo agregar si no está vacío
            criteria[criteria_count++] = token;
        }
        token = strtok(NULL, ";");
    }

    if (criteria_count == 0) {
        write(result_fd, "NA", 2);
        return;
    }
    
    // Variables para almacenar los resultados de la intersección
    uint32_t* current_result = NULL;
    size_t current_count = 0;
    int first_criterion = 1;
    
    // Procesar cada criterio
    for (int i = 0; i < criteria_count; i++) {
        uint32_t* skill_offsets;
        size_t skill_count;
        
        // Buscar la habilidad actual
        if (!find_skill_offsets(criteria[i], &skill_offsets, &skill_count)) {
            // Habilidad no encontrada, la intersección es vacía
            if (current_result) free(current_result);
            current_result = NULL;
            current_count = 0;
            break;
        }
        
        if (first_criterion) {
            // Primera habilidad, copiar sus offsets
            current_count = skill_count;
            current_result = malloc(current_count * sizeof(uint32_t));
            if (!current_result) {
                perror("Error al asignar memoria para resultados");
                return;
            }
            memcpy(current_result, skill_offsets, current_count * sizeof(uint32_t));
            first_criterion = 0;
        } else {
            // Intersección con los resultados actuales
            uint32_t* new_result;
            size_t new_count = intersect_offsets(
                current_result, current_count,
                skill_offsets, skill_count,
                &new_result
            );
            
            // Reemplazar el resultado actual
            free(current_result);
            current_result = new_result;
            current_count = new_count;
            
            // Si en algún punto la intersección es vacía, terminar
            if (current_count == 0) break;
        }
    }
    
    // Preparar la respuesta
    if (current_count == 0 || !current_result) {
        write(result_fd, "NA", 2);
    } else {
        char final_response[8192] = "";
        // Leer el archivo CSV y enviar las líneas correspondientes
        FILE* csv_file = fopen("data.csv", "r");
        if (!csv_file) {
            perror("Error al abrir data.csv");
            write(result_fd, "NA", 2);
            free(current_result);
            return;
        }

        // Ordenar los offsets para lectura secuencial del disco
        qsort(current_result, current_count, sizeof(uint32_t), 
             (int (*)(const void*, const void*))strcmp);
        
        size_t response_len = 0;
        int first = 1;
        
        for (size_t i = 0; i < current_count; i++) {
            if (fseek(csv_file, current_result[i], SEEK_SET) != 0) {
                perror("Error al buscar en el archivo");
                continue;
            }
            
            char line_buffer[4096];
            if (!fgets(line_buffer, sizeof(line_buffer), csv_file)) {
                perror("Error al leer línea del archivo");
                continue;
            }
            
            // Eliminar salto de línea
            line_buffer[strcspn(line_buffer, "\n")] = '\0';
            
            // Calcular espacio disponible
            size_t line_len = strlen(line_buffer);
            size_t remaining = sizeof(final_response) - response_len - 2; // Espacio para '\n' y '\0'
            
            if (line_len >= remaining) {
                // No hay suficiente espacio, truncar
                strncpy(final_response + response_len, "\n... (más resultados disponibles)", remaining);
                response_len = sizeof(final_response) - 1; // Asegurar terminación nula
                break;
            }
            
            if (!first) {
                final_response[response_len++] = '\n';
            } else {
                first = 0;
            }
            
            // Copiar la línea
            strcpy(final_response + response_len, line_buffer);
            response_len += line_len;
        }
        
        fclose(csv_file);
        
        // Asegurar terminación nula
        if (response_len >= sizeof(final_response)) {
            response_len = sizeof(final_response) - 1;
        }
        final_response[response_len] = '\0';
        
        // Enviar respuesta
        write(result_fd, final_response, strlen(final_response));
        
        // Liberar memoria
        free(current_result);
    }
    
    while(intersection_list) {
         OffsetNode* temp = intersection_list;
         intersection_list = intersection_list->next;
         free(temp);
     }
}

// --- Funciones Auxiliares (Previamente Faltantes) ---

/**
 * @brief Función hash djb2. Convierte una cadena a un índice de la tabla.
 */
unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % TABLE_SIZE;
}

/**
 * @brief Libera toda la memoria de la tabla hash, incluyendo las listas de offsets.
 */
void free_full_hash_table() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current_node = hashTable[i];
        while (current_node != NULL) {
            // 1. Liberar la lista de offsets interna
            OffsetNode* current_offset = current_node->offsets;
            while(current_offset != NULL) {
                OffsetNode* temp_offset = current_offset;
                current_offset = current_offset->next;
                free(temp_offset);
            }

            // 2. Liberar el nodo principal
            HashNode* temp_node = current_node;
            current_node = current_node->next;
            free(temp_node->skill); // Liberar la cadena de la habilidad
            free(temp_node);        // Liberar el nodo HashNode
        }
        hashTable[i] = NULL;
    }
}

/**
 * @brief Libera la memoria del diccionario
 */
void free_dictionary() {
    for (size_t i = 0; i < skill_dict.size; i++) {
        free(skill_dict.entries[i].offsets);
    }
    free(skill_dict.entries);
    skill_dict.entries = NULL;
    skill_dict.size = 0;
}

/**
 * @brief Limpia los recursos (tuberías y memoria) antes de salir.
 */
void cleanup(int signum) {
    (void)signum; 
    
    printf("\nCerrando el motor de búsqueda...\n");
    free_full_hash_table();
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}
