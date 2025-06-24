#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


// Estructura para el directorio de skills que se carga en memoria.
// Es ligera y contiene solo metadatos.
typedef struct SkillDirEntry {
    char* skill;
    size_t offset_count; // Cuántos trabajos tienen esta skill.
    long file_offset;    // Dónde empieza su lista de offsets en jobs.idx.
} SkillDirEntry;

// El nodo de la tabla hash ahora almacena entradas del directorio.
typedef struct HashNode {
    SkillDirEntry entry;
    struct HashNode* next;
} HashNode;

#define TABLE_SIZE 5003
HashNode* skillDirectory[TABLE_SIZE];

// Estructura para gestionar los criterios de una consulta.
typedef struct QueryCriterion {
    const SkillDirEntry* entry;
} QueryCriterion;


// --- Nombres de archivos y tuberías ---
#define SKILL_DIR_FILE "jobs.skl"
#define INDEX_FILE "jobs.idx"
#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

// --- Prototipos ---
unsigned long hash_function(const char* str);
void load_skill_directory(const char* filename); // Carga el índice .skl
void search_and_respond(int query_fd, int result_fd); // Lógica de búsqueda
void free_skill_directory();
void cleanup(int signum);
int compare_criteria(const void* a, const void* b); // Para ordenar criterios
int compare_longs(const void* a, const void* b);    // Para ordenar offsets

// --- Implementación del Motor ---
int main() {
    printf("Motor de búsqueda multi-criterio iniciando (modo optimizado)...\n");
    signal(SIGINT, cleanup);

    load_skill_directory(SKILL_DIR_FILE);
    printf("Directorio de skills cargado en memoria.\n");

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

/**
 * @brief Carga únicamente el directorio de skills (.skl) en la tabla hash.
 */
void load_skill_directory(const char* filename) {
    for(int i=0; i<TABLE_SIZE; i++) skillDirectory[i] = NULL;

    FILE* file_skl = fopen(filename, "rb");
    if (!file_skl) {
        perror("Error fatal: no se pudo abrir jobs.skl");
        exit(EXIT_FAILURE);
    }
    
    size_t skill_len;
    while (fread(&skill_len, sizeof(size_t), 1, file_skl) == 1) {
        HashNode* new_node = (HashNode*)malloc(sizeof(HashNode));
        if (!new_node) { perror("malloc HashNode"); exit(EXIT_FAILURE); }

        new_node->entry.skill = malloc(skill_len + 1);
        fread(new_node->entry.skill, 1, skill_len, file_skl);
        new_node->entry.skill[skill_len] = '\0';
        
        fread(&new_node->entry.offset_count, sizeof(size_t), 1, file_skl);
        fread(&new_node->entry.file_offset, sizeof(long), 1, file_skl);

        unsigned long index = hash_function(new_node->entry.skill);
        new_node->next = skillDirectory[index];
        skillDirectory[index] = new_node;
    }
    fclose(file_skl);
}

/**
 * @brief Busca usando el índice de dos niveles y una estrategia optimizada
 * de intersección por orden de rareza.
 */
void search_and_respond(int query_fd, int result_fd) {
    char query_buffer[1024];
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);

    if (bytes_read <= 0) return;
    query_buffer[bytes_read] = '\0';
    printf("Petición recibida: '%s'\n", query_buffer);

    char* tokens[3];
    int criteria_count = 0;
    char* token = strtok(query_buffer, ";");
    while (token != NULL && criteria_count < 3) {
        tokens[criteria_count++] = token;
        token = strtok(NULL, ";");
    }
    if (criteria_count == 0) { write(result_fd, "NA", 2); return; }

    // 1. Obtener metadatos para cada criterio desde el directorio en memoria.
    QueryCriterion criteria[3];
    for (int i = 0; i < criteria_count; i++) {
        unsigned long index = hash_function(tokens[i]);
        HashNode* node = skillDirectory[index];
        while(node && strcmp(node->entry.skill, tokens[i]) != 0) node = node->next;

        if (!node) { write(result_fd, "NA", 2); return; }
        criteria[i].entry = &node->entry;
    }

    // 2. Ordenar criterios del más raro (menos offsets) al más común.
    qsort(criteria, criteria_count, sizeof(QueryCriterion), compare_criteria);

    // 3. Cargar y ordenar la lista de offsets del criterio MÁS RARO.
    FILE* idx_file = fopen(INDEX_FILE, "rb");
    if (!idx_file) { perror("Error al abrir jobs.idx"); write(result_fd, "NA", 2); return; }
    
    size_t intersection_size = criteria[0].entry->offset_count;
    long* intersection_list = malloc(intersection_size * sizeof(long));
    if (!intersection_list) { fclose(idx_file); write(result_fd, "NA", 2); return; }
    fseek(idx_file, criteria[0].entry->file_offset, SEEK_SET);
    fread(intersection_list, sizeof(long), intersection_size, idx_file);
    qsort(intersection_list, intersection_size, sizeof(long), compare_longs);

    // 4. Intersecar con las listas de los demás criterios de forma eficiente.
    for (int i = 1; i < criteria_count; i++) {
        if (intersection_size == 0) break;

        size_t next_list_size = criteria[i].entry->offset_count;
        long* next_list = malloc(next_list_size * sizeof(long));
        if (!next_list) break;
        fseek(idx_file, criteria[i].entry->file_offset, SEEK_SET);
        fread(next_list, sizeof(long), next_list_size, idx_file);
        qsort(next_list, next_list_size, sizeof(long), compare_longs);

        long* new_intersection = malloc(intersection_size * sizeof(long));
        if (!new_intersection) { free(next_list); break; }
        size_t new_size = 0;
        
        // Algoritmo de intersección de dos listas ordenadas (O(N+M))
        size_t ptr1 = 0, ptr2 = 0;
        while (ptr1 < intersection_size && ptr2 < next_list_size) {
            if (intersection_list[ptr1] < next_list[ptr2]) ptr1++;
            else if (next_list[ptr2] < intersection_list[ptr1]) ptr2++;
            else {
                new_intersection[new_size++] = intersection_list[ptr1];
                ptr1++; ptr2++;
            }
        }
        
        free(intersection_list);
        free(next_list);
        intersection_list = new_intersection;
        intersection_size = new_size;
    }
    fclose(idx_file);

    // 5. Construir y enviar la respuesta.
    if (intersection_size == 0) {
        write(result_fd, "NA", 2);
    } else {
        char final_response[8192] = "";
        char line_buffer[4096];
	    FILE* csv_file = fopen("data.csv", "r");
        if (!csv_file) {
            write(result_fd, "NA", 2);
        } else {
            for(size_t i = 0; i < intersection_size; i++) {
                if (fseek(csv_file, intersection_list[i], SEEK_SET) == 0) {
                    if (fgets(line_buffer, sizeof(line_buffer), csv_file)) {
                        if (strlen(final_response) + strlen(line_buffer) < sizeof(final_response) - 30) {
                            strcat(final_response, line_buffer);
                        } else {
                            strcat(final_response, "\n... (resultados truncados) ...");
                            break;
                        }
                    }
                }
            }
            fclose(csv_file);
            write(result_fd, final_response, strlen(final_response));
        }       
    }
    free(intersection_list);
}


// --- Funciones Auxiliares ---

// Compara dos criterios de búsqueda por su popularidad (offset_count).
int compare_criteria(const void* a, const void* b) {
    QueryCriterion* critA = (QueryCriterion*)a;
    QueryCriterion* critB = (QueryCriterion*)b;
    if (critA->entry->offset_count < critB->entry->offset_count) return -1;
    if (critA->entry->offset_count > critB->entry->offset_count) return 1;
    return 0;
}

// Compara dos longs para usar con qsort.
int compare_longs(const void* a, const void* b) {
    long la = *(const long*)a;
    long lb = *(const long*)b;
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
}

unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) hash = ((hash << 5) + hash) + c;
    return hash % TABLE_SIZE;
}

void free_skill_directory() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current_node = skillDirectory[i];
        while (current_node != NULL) {
            HashNode* temp_node = current_node;
            current_node = current_node->next;
            free(temp_node->entry.skill);
            free(temp_node);
        }
        skillDirectory[i] = NULL;
    }
}

void cleanup(int signum) {
    (void)signum; 
    printf("\nCerrando el motor de búsqueda...\n");
    free_skill_directory();
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}
