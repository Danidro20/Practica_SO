#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define TABLE_SIZE 4520789

typedef struct OffsetNode {
    long offset;
    struct OffsetNode* next;
} OffsetNode;

typedef struct HashNode {
    char* skill;
    OffsetNode* offsets;
    size_t offset_count; // Contaremos los offsets aquí
    struct HashNode* next;
} HashNode;

HashNode* hashTable[TABLE_SIZE];

// Prototipos
unsigned long hash_function(const char* str);
void insert_skill(const char* skill, long offset);
void write_sorted_indices(const char* skl_filename, const char* idx_filename);
void free_hash_table();
char* trim_whitespace(char* str);
int compare_hash_nodes_alpha(const void* a, const void* b);
int compare_longs(const void* a, const void* b);

int main() {
    for (int i = 0; i < TABLE_SIZE; i++) hashTable[i] = NULL;

    FILE* file_csv = fopen("data.csv", "r");
    if (!file_csv) {
        perror("Error al abrir data.csv");
        return 1;
    }

    char line_buffer[4096];
    long current_offset = ftell(file_csv);
    if (fgets(line_buffer, sizeof(line_buffer), file_csv)) {
        current_offset = ftell(file_csv);
    }
    
    long line_count = 0;
    while (fgets(line_buffer, sizeof(line_buffer), file_csv)) {
        if (line_count++ % 10000 == 0) {
            printf("Procesando línea del CSV: %ld\r", line_count);
            fflush(stdout);
        }
        char* skills_part = strchr(line_buffer, ',');
        if (skills_part) {
            skills_part++;
            char* token = strtok(skills_part, "\",\n");
            while (token != NULL) {
                char* trimmed_skill = trim_whitespace(token);
                if (strlen(trimmed_skill) > 0) {
                    insert_skill(trimmed_skill, current_offset);
                }
                token = strtok(NULL, "\",\n");
            }
        }
        current_offset = ftell(file_csv);
    }
    printf("\nProcesamiento de CSV finalizado. Ordenando y escribiendo índices...\n");
    fclose(file_csv);

    write_sorted_indices("jobs.skl", "jobs.idx");
    printf("Archivos de índice ordenados 'jobs.skl' y 'jobs.idx' creados.\n");

    free_hash_table();
    printf("Memoria liberada.\n");
    return 0;
}

void insert_skill(const char* skill, long offset) {
    unsigned long index = hash_function(skill);
    HashNode* current_node = hashTable[index];
    while (current_node != NULL) {
        if (strcmp(current_node->skill, skill) == 0) break;
        current_node = current_node->next;
    }
    if (current_node == NULL) {
        current_node = (HashNode*)malloc(sizeof(HashNode));
        current_node->skill = strdup(skill);
        current_node->offsets = NULL;
        current_node->offset_count = 0;
        current_node->next = hashTable[index];
        hashTable[index] = current_node;
    }
    OffsetNode* new_offset_node = (OffsetNode*)malloc(sizeof(OffsetNode));
    new_offset_node->offset = offset;
    new_offset_node->next = current_node->offsets;
    current_node->offsets = new_offset_node;
    current_node->offset_count++;
}


// (NUEVA) Función para escribir los índices completamente ordenados
void write_sorted_indices(const char* skl_filename, const char* idx_filename) {
    // 1. Contar el número total de skills únicas.
    size_t total_skills = 0;
    for (int i = 0; i < TABLE_SIZE; i++) {
        for (HashNode* node = hashTable[i]; node != NULL; node = node->next) {
            total_skills++;
        }
    }

    // 2. Crear un array de punteros a todos los HashNodes para ordenarlos.
    HashNode** sorted_nodes = malloc(total_skills * sizeof(HashNode*));
    size_t current_skill = 0;
    for (int i = 0; i < TABLE_SIZE; i++) {
        for (HashNode* node = hashTable[i]; node != NULL; node = node->next) {
            sorted_nodes[current_skill++] = node;
        }
    }

    // 3. Ordenar el array de nodos alfabéticamente por 'skill'.
    qsort(sorted_nodes, total_skills, sizeof(HashNode*), compare_hash_nodes_alpha);

    // 4. Abrir archivos para escritura.
    FILE* file_skl = fopen(skl_filename, "wb");
    FILE* file_idx = fopen(idx_filename, "wb");
    if (!file_skl || !file_idx) {
        perror("Error al crear archivos de índice");
        return;
    }
    
    // Escribir el número total de skills al inicio del archivo .skl (útil para la búsqueda binaria)
    fwrite(&total_skills, sizeof(size_t), 1, file_skl);

    // 5. Iterar a través de los nodos ORDENADOS.
    for (size_t i = 0; i < total_skills; i++) {
        HashNode* current_node = sorted_nodes[i];
        
        // Convertir la lista enlazada de offsets a un array para ordenarla.
        long* offset_array = malloc(current_node->offset_count * sizeof(long));
        OffsetNode* o_node = current_node->offsets;
        for (size_t j = 0; j < current_node->offset_count; j++) {
            offset_array[j] = o_node->offset;
            o_node = o_node->next;
        }

        // Ordenar el array de offsets numéricamente.
        qsort(offset_array, current_node->offset_count, sizeof(long), compare_longs);

        // Escribir en los archivos de índice.
        long idx_offset = ftell(file_idx);
        size_t skill_len = strlen(current_node->skill);

        // Formato .skl: [len, skill, count, offset_en_idx]
        fwrite(&skill_len, sizeof(size_t), 1, file_skl);
        fwrite(current_node->skill, 1, skill_len, file_skl);
        fwrite(&current_node->offset_count, sizeof(size_t), 1, file_skl);
        fwrite(&idx_offset, sizeof(long), 1, file_skl);

        // Escribir la lista de offsets YA ORDENADA en .idx
        fwrite(offset_array, sizeof(long), current_node->offset_count, file_idx);
        
        free(offset_array);
    }
    
    fclose(file_skl);
    fclose(file_idx);
    free(sorted_nodes);
}

// --- Funciones auxiliares y de liberación (incluyendo nuevas funciones de comparación) ---
int compare_hash_nodes_alpha(const void* a, const void* b) {
    HashNode* nodeA = *(HashNode**)a;
    HashNode* nodeB = *(HashNode**)b;
    return strcmp(nodeA->skill, nodeB->skill);
}

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

void free_hash_table() {
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
    }
}

char* trim_whitespace(char* str) {
    char *end;
    while(isspace((unsigned char)*str)) str++;
    if(*str == 0) return str;
    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}
