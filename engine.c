#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// --- Estructuras de Datos ---
typedef struct OffsetNode {
    long offset;
    struct OffsetNode* next;
} OffsetNode;

typedef struct HashNode {
    char* skill;
    OffsetNode* offsets;
    struct HashNode* next;
} HashNode;

#define TABLE_SIZE 5003
HashNode* hashTable[TABLE_SIZE];

// --- Nombres de las tuberías ---
#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

// --- Prototipos ---
unsigned long hash_function(const char* str);
void load_full_index(const char* filename); // Lógica modificada
void search_and_respond(int query_fd, int result_fd);
void free_full_hash_table();
void cleanup(int signum);

// --- Implementación del Motor ---
int main() {
    printf("Motor de búsqueda multi-criterio iniciando...\n");
    signal(SIGINT, cleanup);

    load_full_index("jobs.idx");
    printf("Índice completo cargado en memoria.\n");

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
 * @brief (MODIFICADA) Carga el índice desde el nuevo formato optimizado.
 */
void load_full_index(const char* filename) {
    for(int i=0; i<TABLE_SIZE; i++) hashTable[i] = NULL;

    FILE* file_idx = fopen(filename, "rb");
    if (!file_idx) {
        perror("Error fatal: no se pudo abrir jobs.idx");
        exit(EXIT_FAILURE);
    }

    size_t skill_len;
    while (fread(&skill_len, sizeof(size_t), 1, file_idx) == 1) {
        // Leer la habilidad
        char* skill_buffer = malloc(skill_len + 1);
        if (!skill_buffer) { perror("malloc skill_buffer"); exit(EXIT_FAILURE); }
        fread(skill_buffer, 1, skill_len, file_idx);
        skill_buffer[skill_len] = '\0';

        // Crear el HashNode para esta habilidad
        HashNode* new_node = (HashNode*)malloc(sizeof(HashNode));
        if (!new_node) { perror("malloc HashNode"); exit(EXIT_FAILURE); }
        new_node->skill = skill_buffer; // Transferir propiedad del buffer
        new_node->offsets = NULL;
        
        // Insertar en la tabla hash
        unsigned long index = hash_function(new_node->skill);
        new_node->next = hashTable[index];
        hashTable[index] = new_node;

        // Leer la cantidad de offsets que vienen a continuación
        size_t offset_count;
        fread(&offset_count, sizeof(size_t), 1, file_idx);

        // Leer cada offset y añadirlo a la lista
        for (size_t i = 0; i < offset_count; i++) {
            long offset_val;
            fread(&offset_val, sizeof(long), 1, file_idx);
            
            OffsetNode* new_offset_node = (OffsetNode*)malloc(sizeof(OffsetNode));
            if (!new_offset_node) { perror("malloc OffsetNode"); exit(EXIT_FAILURE); }
            new_offset_node->offset = offset_val;
            new_offset_node->next = new_node->offsets; // Apila al inicio de la lista
            new_node->offsets = new_offset_node;
        }
    }
    fclose(file_idx);
}

// Lógica principal de búsqueda con intersección (SIN CAMBIOS)
void search_and_respond(int query_fd, int result_fd) {
    char query_buffer[1024];
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);

    if (bytes_read <= 0) return;
    query_buffer[bytes_read] = '\0';
    printf("Petición recibida: '%s'\n", query_buffer);

    char* criteria[3] = {NULL, NULL, NULL};
    int criteria_count = 0;

    char* token = strtok(query_buffer, ";");
    while (token != NULL && criteria_count < 3) {
        criteria[criteria_count++] = token;
        token = strtok(NULL, ";");
    }

    if (criteria_count == 0) {
        write(result_fd, "NA", 2);
        return;
    }
    
    OffsetNode* intersection_list = NULL;
    int first_criterion = 1;

    for (int i = 0; i < criteria_count; i++) {
        unsigned long index = hash_function(criteria[i]);
        HashNode* node = hashTable[index];
        while(node && strcmp(node->skill, criteria[i]) != 0) {
            node = node->next;
        }

        if (!node) {
            while(intersection_list) {
                 OffsetNode* temp = intersection_list;
                 intersection_list = intersection_list->next;
                 free(temp);
             }
            intersection_list = NULL;
            break;
        }

        if (first_criterion) {
            for(OffsetNode* o_node = node->offsets; o_node != NULL; o_node = o_node->next) {
                OffsetNode* new_intersect_node = (OffsetNode*)malloc(sizeof(OffsetNode));
                new_intersect_node->offset = o_node->offset;
                new_intersect_node->next = intersection_list;
                intersection_list = new_intersect_node;
            }
            first_criterion = 0;
        } else {
            OffsetNode* current_intersect = intersection_list;
            OffsetNode* prev_intersect = NULL;
            while(current_intersect != NULL) {
                int found = 0;
                for(OffsetNode* o_node = node->offsets; o_node != NULL; o_node = o_node->next) {
                    if (o_node->offset == current_intersect->offset) {
                        found = 1;
                        break;
                    }
                }
                
                if (found) {
                    prev_intersect = current_intersect;
                    current_intersect = current_intersect->next;
                } else {
                    OffsetNode* to_delete = current_intersect;
                    if (prev_intersect) {
                        prev_intersect->next = current_intersect->next;
                    } else {
                        intersection_list = current_intersect->next;
                    }
                    current_intersect = current_intersect->next;
                    free(to_delete);
                }
            }
        }
    }

    if (intersection_list == NULL) {
        write(result_fd, "NA", 2);
    } else {
        char final_response[8192] = "";
        char line_buffer[4096];
	    FILE* csv_file = fopen("data.csv", "r");

        if (csv_file == NULL) {
            perror("Error: No se pudo abrir data.csv en el motor");
            write(result_fd, "NA", 2);
        } else {
            for(OffsetNode* result_node = intersection_list; result_node != NULL; result_node = result_node->next) {
                if (fseek(csv_file, result_node->offset, SEEK_SET) == 0) {
                    if (fgets(line_buffer, sizeof(line_buffer), csv_file)) {
                        
                        if (strlen(final_response) + strlen(line_buffer) < sizeof(final_response)) {
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

     while(intersection_list) {
         OffsetNode* temp = intersection_list;
         intersection_list = intersection_list->next;
         free(temp);
     }
}

// --- Funciones Auxiliares (SIN CAMBIOS) ---

unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % TABLE_SIZE;
}

void free_full_hash_table() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current_node = hashTable[i];
        while (current_node != NULL) {
            OffsetNode* current_offset = current_node->offsets;
            while(current_offset != NULL) {
                OffsetNode* temp_offset = current_offset;
                current_offset = current_offset->next;
                free(temp_offset);
            }
            HashNode* temp_node = current_node;
            current_node = current_node->next;
            free(temp_node->skill);
            free(temp_node);
        }
        hashTable[i] = NULL;
    }
}

void cleanup(int signum) {
    (void)signum; 
    printf("\nCerrando el motor de búsqueda...\n");
    free_full_hash_table();
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}
