#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <zstd.h>
#include <time.h>
#include <ctype.h>
#include <signal.h>
#include <stdint.h>
#include <getopt.h>
#include "utils.h"

// --- Definiciones ---
#define MAX_SKILL_LENGTH 256
#define INITIAL_OFFSETS_CAPACITY 1000
#define TABLE_SIZE 1024  // Tamaño de la tabla hash

// Variable global para el modo depuración (activado por defecto)
int debug_mode = 1;

// Estructura para almacenar una habilidad con sus offsets
typedef struct
{
    char skill[MAX_SKILL_LENGTH];
    uint32_t *offsets; // Array de offsets
    size_t count;      // Número de offsets
} SkillEntry;

// Estructura principal del diccionario
typedef struct
{
    SkillEntry *entries; // Array de entradas
    size_t size;         // Número de entradas
} Dictionary;

// Estructura para la lista enlazada de offsets
struct OffsetNode {
    uint32_t offset;
    struct OffsetNode* next;
};
typedef struct OffsetNode OffsetNode;

// Estructura para los nodos de la tabla hash
struct HashNode {
    char* skill;          // Nombre de la habilidad
    OffsetNode* offsets;   // Lista de offsets
    struct HashNode* next; // Para manejar colisiones
};
typedef struct HashNode HashNode;

// Variables globales
Dictionary skill_dict;
HashNode* hashTable[TABLE_SIZE];  // Tabla hash

// --- Nombres de las tuberías ---
#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

// Función para leer un entero con codificación variable
static uint32_t read_varint(FILE *f)
{
    uint32_t result = 0;
    uint8_t byte;
    int shift = 0;

    do
    {
        if (fread(&byte, 1, 1, f) != 1)
        {
            perror("Error al leer entero variable");
            exit(EXIT_FAILURE);
        }
        result |= (byte & 0x7F) << shift;
        shift += 7;
    } while (byte & 0x80);

    return result;
}

/**
 * @brief Libera la memoria del diccionario
 */
void free_dictionary()
{
    for (size_t i = 0; i < skill_dict.size; i++)
    {
        free(skill_dict.entries[i].offsets);
    }
    free(skill_dict.entries);
    skill_dict.entries = NULL;
    skill_dict.size = 0;
}

// Carga el índice comprimido en memoria
void load_compressed_index(const char *filename, int debug_mode)
{
    printf("Cargando índice comprimido...\n");

    // Leer el archivo completo
    FILE *f = fopen(filename, "rb");
    if (!f)
    {
        perror("No se pudo abrir el archivo de índice");
        exit(EXIT_FAILURE);
    }

    // Obtener tamaño del archivo
    fseek(f, 0, SEEK_END);
    size_t file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size < sizeof(size_t))
    {
        fprintf(stderr, "Archivo de índice corrupto (demasiado pequeño)");
        exit(EXIT_FAILURE);
    }

    // Leer el tamaño descomprimido
    size_t uncompressed_size;
    if (fread(&uncompressed_size, sizeof(uncompressed_size), 1, f) != 1)
    {
        perror("Error al leer el tamaño descomprimido");
        fclose(f);
        exit(EXIT_FAILURE);
    }

    // Leer los datos comprimidos
    size_t compressed_size = file_size - sizeof(uncompressed_size);
    void *compressed_data = malloc(compressed_size);
    if (!compressed_data)
    {
        perror("Error al asignar memoria para datos comprimidos");
        fclose(f);
        exit(EXIT_FAILURE);
    }

    if (fread(compressed_data, 1, compressed_size, f) != compressed_size)
    {
        perror("Error al leer datos comprimidos");
        free(compressed_data);
        fclose(f);
        exit(EXIT_FAILURE);
    }
    fclose(f);

    // Descomprimir los datos
    void *decompressed_data = malloc(uncompressed_size);
    if (!decompressed_data)
    {
        perror("Error al asignar memoria para datos descomprimidos");
        free(compressed_data);
        exit(EXIT_FAILURE);
    }

    size_t decompressed_size = ZSTD_decompress(
        decompressed_data, uncompressed_size,
        compressed_data, compressed_size);

    free(compressed_data);

    if (ZSTD_isError(decompressed_size))
    {
        fprintf(stderr, "Error al descomprimir: %s\n", ZSTD_getErrorName(decompressed_size));
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }

    // Procesar el buffer descomprimido
    FILE *mem = fmemopen(decompressed_data, decompressed_size, "rb");
    if (!mem)
    {
        perror("Error al crear stream de memoria");
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }

    // Leer número de entradas
    size_t num_entries;
    if (fread(&num_entries, sizeof(size_t), 1, mem) != 1)
    {
        perror("Error al leer número de entradas");
        fclose(mem);
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }

    // Asignar memoria para las entradas
    skill_dict.entries = calloc(num_entries, sizeof(SkillEntry));
    if (!skill_dict.entries)
    {
        perror("Error al asignar memoria para entradas");
        fclose(mem);
        free(decompressed_data);
        exit(EXIT_FAILURE);
    }
    skill_dict.size = num_entries;

    printf("Cargando %zu entradas del diccionario...\n", num_entries);
    
    // Seleccionar 3 índices aleatorios para mostrar
    srand(time(NULL));
    size_t sample_indices[3];
    for (int s = 0; s < 3 && s < num_entries; s++) {
        sample_indices[s] = rand() % num_entries;
    }

    // Leer cada entrada
    for (size_t i = 0; i < num_entries; i++)
    {
        // Leer longitud de la habilidad (ahora usamos uint16_t)
        uint16_t skill_len;
        if (fread(&skill_len, sizeof(uint16_t), 1, mem) != 1)
        {
            fprintf(stderr, "Error al leer longitud de habilidad %zu\n", i);
            goto cleanup_error;
        }

        // Leer la habilidad
        if (fread(skill_dict.entries[i].skill, 1, skill_len, mem) != skill_len)
        {
            fprintf(stderr, "Error al leer habilidad %zu\n", i);
            goto cleanup_error;
        }
        skill_dict.entries[i].skill[skill_len] = '\0';
        
        // Eliminar comillas dobles al inicio y final si existen
        char *skill = skill_dict.entries[i].skill;
        size_t len = strlen(skill);
        if (len >= 2 && skill[0] == '"' && skill[len-1] == '"') {
            // Mover el contenido un carácter a la izquierda
            memmove(skill, skill + 1, len - 2);
            skill[len-2] = '\0';
        }

        // Mostrar información de muestra para entradas seleccionadas
        if (debug_mode) {
            for (int s = 0; s < 3 && s < num_entries; s++) {
                if (i == sample_indices[s]) {
                    printf("\n--- Entrada de muestra %d (índice %zu) ---\n", s+1, i);
                    printf("  Habilidad: '%s' (longitud: %u)\n", skill_dict.entries[i].skill, skill_len);
                }
            }
        }

        // Leer número de offsets (ahora usamos uint16_t)
        uint16_t num_offsets;
        if (fread(&num_offsets, sizeof(uint16_t), 1, mem) != 1)
        {
            fprintf(stderr, "Error al leer número de offsets para habilidad %zu\n", i);
            goto cleanup_error;
        }

        // Reservar memoria para los offsets
        skill_dict.entries[i].count = num_offsets;
        skill_dict.entries[i].offsets = malloc(num_offsets * sizeof(uint32_t));
        if (!skill_dict.entries[i].offsets)
        {
            perror("Error al asignar memoria para offsets");
            goto cleanup_error;
        }

        if (num_offsets > 0) {
            // Leer los deltas (ahora están en formato binario completo, no en varint)
            if (fread(skill_dict.entries[i].offsets, sizeof(uint32_t), num_offsets, mem) != num_offsets)
            {
                fprintf(stderr, "Error al leer los offsets para habilidad %zu\n", i);
                goto cleanup_error;
            }

            // Mostrar primeros 5 offsets para las entradas de muestra
            if (debug_mode) {
                for (int s = 0; s < 3 && s < num_entries; s++) {
                    if (i == sample_indices[s]) {
                        printf("  Primeros 5 offsets: ");
                        int max_show = num_offsets > 5 ? 5 : num_offsets;
                        for (int j = 0; j < max_show; j++) {
                            printf("%u ", skill_dict.entries[i].offsets[j]);
                        }
                        if (num_offsets > 5) printf("...");
                        printf("\n  Total offsets: %u\n", num_offsets);
                        printf("------------------------\n");
                    }
                }
            }

            // Aplicar el delta decoding
            uint32_t prev = skill_dict.entries[i].offsets[0];
            for (uint32_t j = 1; j < num_offsets; j++)
            {
                uint32_t current = skill_dict.entries[i].offsets[j];
                skill_dict.entries[i].offsets[j] = prev + current;
                prev = skill_dict.entries[i].offsets[j];
            }
        }
    }

    fclose(mem);
    free(decompressed_data);
    return;

cleanup_error:
    fclose(mem);
    free(decompressed_data);
    free_dictionary();
    exit(EXIT_FAILURE);
}

// Función auxiliar para comparación insensible a mayúsculas/minúsculas
static int str_icmp(const char *s1, const char *s2) {
    int c1, c2;
    do {
        c1 = tolower((unsigned char)*s1++);
        c2 = tolower((unsigned char)*s2++);
    } while (c1 == c2 && c1 != '\0');
    return c1 - c2;
}

// Función auxiliar para verificar si una cadena contiene otra (insensible a mayúsculas/minúsculas)
// y maneja términos múltiples (separados por comas) en el haystack
static int str_icontains_terms(const char *haystack, const char *needle) {
    if (!*needle) return 1; // Cadena vacía siempre coincide
    
    // Hacer una copia del needle para no modificar el original
    char needle_copy[256];
    strncpy(needle_copy, needle, sizeof(needle_copy) - 1);
    needle_copy[sizeof(needle_copy) - 1] = '\0';
    
    // Hacer una copia del haystack para no modificar el original
    char haystack_copy[256];
    strncpy(haystack_copy, haystack, sizeof(haystack_copy) - 1);
    haystack_copy[sizeof(haystack_copy) - 1] = '\0';
    
    // Buscar coincidencia directa (caso más rápido)
    char *match = strcasestr(haystack_copy, needle_copy);
    if (match) {
        // Verificar que sea un término completo (rodeado por comas o al inicio/fin)
        if ((match == haystack_copy || *(match-1) == ',' || isspace(*(match-1))) && 
            (*(match + strlen(needle_copy)) == ',' || 
             *(match + strlen(needle_copy)) == '\0' || 
             isspace(*(match + strlen(needle_copy))))) {
            return 1;
        }
    }
    
    // Si no hay coincidencia directa, buscar en cada término individual
    char *term = strtok(haystack_copy, ",");
    while (term != NULL) {
        // Eliminar espacios en blanco al inicio/fin del término
        while (isspace((unsigned char)*term)) term++;
        char *end = term + strlen(term) - 1;
        while (end > term && isspace((unsigned char)*end)) end--;
        *(end + 1) = '\0';
        
        // Comparar términos (insensible a mayúsculas/minúsculas)
        if (strcasecmp(term, needle_copy) == 0) {
            return 1;
        }
        
        term = strtok(NULL, ",");
    }
    
    return 0;
}

// Busca una habilidad en el diccionario y devuelve sus offsets
// Modo de búsqueda:
// 0: exacta (case sensitive)
// 1: insensible a mayúsculas/minúsculas
// 2: búsqueda parcial (contiene)
int find_skill_offsets_ex(const char *skill, uint32_t **offsets, size_t *count, int mode) {
    for (size_t i = 0; i < skill_dict.size; i++) {
        int match = 0;
        
        switch (mode) {
            case 0: // exacta
                match = (strcmp(skill_dict.entries[i].skill, skill) == 0);
                break;
            case 1: // insensible a mayúsculas/minúsculas
                match = (str_icmp(skill_dict.entries[i].skill, skill) == 0);
                break;
            case 2: // búsqueda en términos múltiples
                match = str_icontains_terms(skill_dict.entries[i].skill, skill);
                break;
        }
        
        if (match) {
            *offsets = skill_dict.entries[i].offsets;
            *count = skill_dict.entries[i].count;
            return 1; // Encontrado
        }
    }
    return 0; // No encontrado
}

// Versión original para compatibilidad
int find_skill_offsets(const char *skill, uint32_t **offsets, size_t *count) {
    return find_skill_offsets_ex(skill, offsets, count, 0); // Modo exacto por defecto
}

// Encuentra la intersección de dos listas ordenadas de offsets
size_t intersect_offsets(const uint32_t *a, size_t a_len,
                         const uint32_t *b, size_t b_len,
                         uint32_t **result)
{
    size_t i = 0, j = 0, k = 0;
    uint32_t *intersection = malloc((a_len < b_len ? a_len : b_len) * sizeof(uint32_t));

    while (i < a_len && j < b_len)
    {
        if (a[i] < b[j])
        {
            i++;
        }
        else if (a[i] > b[j])
        {
            j++;
        }
        else
        {
            // Para evitar duplicados
            if (k == 0 || intersection[k - 1] != a[i])
            {
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
void search_and_respond(int query_fd, int result_fd)
{
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    char query_buffer[1024];
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);

    if (bytes_read <= 0)
        return;
    query_buffer[bytes_read] = '\0';
    printf("Petición recibida: '%s'\n", query_buffer);

    char *criteria[3] = {NULL, NULL, NULL};
    int criteria_count = 0;

    // Dividir la consulta por punto y coma
    char *token = strtok(query_buffer, ";");
    while (token != NULL && criteria_count < 3)
    {
        // Eliminar espacios en blanco al inicio y final
        while (isspace((unsigned char)*token))
            token++;
        char *end = token + strlen(token) - 1;
        while (end > token && isspace((unsigned char)*end))
            end--;
        *(end + 1) = '\0';

        if (*token)
        { // Solo agregar si no está vacío
            criteria[criteria_count++] = token;
        }
        token = strtok(NULL, ";");
    }

    if (criteria_count == 0)
    {
        write(result_fd, "NA", 2);
        return;
    }

    // Variables para almacenar los resultados de la intersección
    uint32_t *current_result = NULL;
    size_t current_count = 0;
    int first_criterion = 1;

    // Procesar cada criterio
    for (int i = 0; i < criteria_count; i++)
    {
        uint32_t *skill_offsets;
        size_t skill_count;

        // Buscar la habilidad actual con búsqueda flexible
        // Modo 0: exacto (case sensitive)
        // Modo 1: insensible a mayúsculas/minúsculas
        // Modo 2: búsqueda parcial (contiene)
        int search_mode = 2; // Por defecto, búsqueda parcial
        
        // Si el criterio está entre comillas, usar búsqueda exacta
        if (criteria[i][0] == '"' && criteria[i][strlen(criteria[i])-1] == '"') {
            // Eliminar comillas
            char *trimmed = strdup(criteria[i] + 1);
            trimmed[strlen(trimmed)-1] = '\0';
            int found = find_skill_offsets_ex(trimmed, &skill_offsets, &skill_count, 0);
            free(trimmed);
            if (found) goto found_skill;
        }
        
        // Si no se encontró con búsqueda exacta, intentar con búsqueda flexible
        if (!find_skill_offsets_ex(criteria[i], &skill_offsets, &skill_count, search_mode))
        {
            // No se encontró esta habilidad, limpiar resultados anteriores
            if (current_result) free(current_result);
            current_result = NULL;
            current_count = 0;
            break;
        }
        
        found_skill:
        // Continuar con el procesamiento normal...

        if (first_criterion)
        {
            // Primera habilidad, copiar sus offsets
            current_count = skill_count;
            current_result = malloc(current_count * sizeof(uint32_t));
            if (!current_result)
            {
                perror("Error al asignar memoria para resultados");
                return;
            }
            memcpy(current_result, skill_offsets, current_count * sizeof(uint32_t));
            first_criterion = 0;
        }
        else
        {
            // Intersección con los resultados actuales
            uint32_t *new_result;
            size_t new_count = intersect_offsets(
                current_result, current_count,
                skill_offsets, skill_count,
                &new_result);

            // Reemplazar el resultado actual
            free(current_result);
            current_result = new_result;
            current_count = new_count;

            // Si en algún punto la intersección es vacía, terminar
            if (current_count == 0)
                break;
        }
    }

    // Preparar la respuesta
    if (current_count == 0 || !current_result)
    {
        write(result_fd, "NA", 2);
    }
    else
    {
        char final_response[8192] = "";
        // Leer el archivo CSV y enviar las líneas correspondientes
        FILE *csv_file = fopen("data.csv", "r");
        if (!csv_file)
        {
            perror("Error al abrir data.csv");
            write(result_fd, "NA", 2);
            free(current_result);
            return;
        }

        // Ordenar los offsets para lectura secuencial del disco
        qsort(current_result, current_count, sizeof(uint32_t),
              (int (*)(const void *, const void *))strcmp);

        size_t response_len = 0;
        int first = 1;

        for (size_t i = 0; i < current_count; i++)
        {
            if (fseek(csv_file, current_result[i], SEEK_SET) != 0)
            {
                perror("Error al buscar en el archivo");
                continue;
            }

            char line_buffer[4096];
            if (!fgets(line_buffer, sizeof(line_buffer), csv_file))
            {
                perror("Error al leer línea del archivo");
                continue;
            }

            // Eliminar salto de línea
            line_buffer[strcspn(line_buffer, "\n")] = '\0';

            // Calcular espacio disponible
            size_t line_len = strlen(line_buffer);
            size_t remaining = sizeof(final_response) - response_len - 2; // Espacio para '\n' y '\0'

            if (line_len >= remaining)
            {
                // No hay suficiente espacio, truncar
                strncpy(final_response + response_len, "\n... (más resultados disponibles)", remaining);
                response_len = sizeof(final_response) - 1; // Asegurar terminación nula
                break;
            }

            if (!first)
            {
                final_response[response_len++] = '\n';
            }
            else
            {
                first = 0;
            }

            // Copiar la línea
            strcpy(final_response + response_len, line_buffer);
            response_len += line_len;
        }

        fclose(csv_file);

        // Asegurar terminación nula
        if (response_len >= sizeof(final_response))
        {
            response_len = sizeof(final_response) - 1;
        }
        final_response[response_len] = '\0';

        // Medir tiempo total
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        char time_str[100];
        format_time(time_str, sizeof(time_str), &start_time, &end_time);
        fprintf(stderr, "[DEBUG] Tiempo total de búsqueda: %s\n", time_str);

        // Enviar respuesta
        write(result_fd, final_response, strlen(final_response));

        // Liberar memoria
        free(current_result);
    }

    // La memoria ya fue liberada con free(current_result) anteriormente
}

// --- Funciones Auxiliares (Previamente Faltantes) ---

/**
 * @brief Función hash djb2. Convierte una cadena a un índice de la tabla.
 */
unsigned long hash_function(const char *str)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
    {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % TABLE_SIZE;
}

/**
 * @brief Libera toda la memoria de la tabla hash, incluyendo las listas de offsets.
 */
void free_full_hash_table()
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        HashNode *current_node = hashTable[i];

        while (current_node != NULL)
        {
            // 1. Liberar la lista de offsets interna
            OffsetNode *current_offset = current_node->offsets;

            while (current_offset != NULL)
            {
                OffsetNode *temp_offset = current_offset;
                current_offset = current_offset->next;
                free(temp_offset);
            }

            // 2. Liberar el nodo principal
            HashNode *temp_node = current_node;
            current_node = current_node->next;
            free(temp_node->skill); // Liberar la cadena de la habilidad
            free(temp_node);        // Liberar el nodo HashNode
        }
        hashTable[i] = NULL;
    }
}

/**
 * @brief Limpia los recursos (tuberías y memoria) antes de salir.
 */
void cleanup(int signum)
{
    (void)signum;

    printf("\nCerrando el motor de búsqueda...\n");
    free_full_hash_table();
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}

// --- Implementación del Motor ---
int main(int argc, char *argv[])
{
    // Procesar argumentos de línea de comandos
    int opt;
    while ((opt = getopt(argc, argv, "n")) != -1) {
        switch (opt) {
            case 'n':
                debug_mode = 0;  // Desactivar modo debug
                break;
            default:
                fprintf(stderr, "Uso: %s [-n]\n", argv[0]);
                fprintf(stderr, "  -n  Desactiva el modo depuración (ejecuta todas las comprobaciones)\n");
                return EXIT_FAILURE;
        }
    }

    if (debug_mode) {
        printf("Modo depuración activado (por defecto). Algunas comprobaciones serán omitidas.\n");
        printf("Motor de búsqueda multi-criterio optimizado iniciando...\n");
    }
    
    signal(SIGINT, cleanup);

    // Inicializar diccionario
    skill_dict.entries = NULL;
    skill_dict.size = 0;

    // Cargar el índice comprimido
    load_compressed_index("dist/jobs.idx.zst", debug_mode);

    if (debug_mode) {
        printf("Índice optimizado cargado en memoria. %zu habilidades indexadas.\n", skill_dict.size);
    }

    // Crear tuberías para comunicación
    umask(0);
    if (!debug_mode || access(QUERY_PIPE, F_OK) != 0) {
        mkfifo(QUERY_PIPE, 0666);
    }
    if (!debug_mode || access(RESULT_PIPE, F_OK) != 0) {
        mkfifo(RESULT_PIPE, 0666);
    }

    if (debug_mode) {
        printf("Tuberías creadas. Esperando peticiones...\n");
    }

    while (1)
    {
        int query_fd = open(QUERY_PIPE, O_RDONLY);
        int result_fd = open(RESULT_PIPE, O_WRONLY);
        if (query_fd == -1 || result_fd == -1)
        {
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
