#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// --- Definiciones de la Tabla Hash (MODIFICADAS) ---
#define TABLE_SIZE 4520789 // Puedes ajustar este valor si es necesario

// Nodo para la lista enlazada de offsets
typedef struct OffsetNode {
    long offset;
    struct OffsetNode* next;
} OffsetNode;

// Nodo de la tabla hash que ahora contiene una lista de offsets
typedef struct HashNode {
    char* skill;
    OffsetNode* offsets;
    struct HashNode* next;
} HashNode;

HashNode* hashTable[TABLE_SIZE];

// --- Prototipos de Funciones ---
unsigned long hash_function(const char* str);
void insert_skill(const char* skill, long offset); // Lógica modificada
void write_index_file(const char* filename);     // Lógica modificada
void free_hash_table();                           // Lógica modificada
char* trim_whitespace(char* str);

// --- Implementación ---

int main() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        hashTable[i] = NULL;
    }

    FILE* file_csv = fopen("data.csv", "r");
    if (!file_csv) {
        perror("Error al abrir data.csv");
        return 1;
    }

    char line_buffer[4096];
    long current_offset = ftell(file_csv);

    // Ignorar la cabecera del CSV si existe
    if (fgets(line_buffer, sizeof(line_buffer), file_csv) != NULL) {
        current_offset = ftell(file_csv);
    }

    while (fgets(line_buffer, sizeof(line_buffer), file_csv)) {
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
    fclose(file_csv);
    printf("El archivo CSV ha sido procesado. Construyendo el índice...\n");

    write_index_file("jobs.idx");
    printf("Archivo de índice 'jobs.idx' creado exitosamente.\n");

    free_hash_table();
    printf("Memoria liberada.\n");

    return 0;
}

/**
 * @brief (MODIFICADA) Inserta una habilidad. Si ya existe, añade el offset a su lista.
 * Si no existe, crea un nuevo nodo para la habilidad y añade el offset.
 */
void insert_skill(const char* skill, long offset) {
    unsigned long index = hash_function(skill);
    HashNode* current_node = hashTable[index];

    // 1. Buscar si la habilidad ya existe
    while (current_node != NULL) {
        if (strcmp(current_node->skill, skill) == 0) {
            break; // La encontramos
        }
        current_node = current_node->next;
    }

    // 2. Si la habilidad no existe, crear un nuevo nodo principal
    if (current_node == NULL) {
        current_node = (HashNode*)malloc(sizeof(HashNode));
        if (!current_node) { perror("malloc HashNode"); exit(1); }
        current_node->skill = strdup(skill);
        if (!current_node->skill) { perror("strdup skill"); exit(1); }
        current_node->offsets = NULL;
        current_node->next = hashTable[index];
        hashTable[index] = current_node;
    }

    // 3. Añadir el nuevo offset a la lista de offsets del nodo
    OffsetNode* new_offset_node = (OffsetNode*)malloc(sizeof(OffsetNode));
    if (!new_offset_node) { perror("malloc OffsetNode"); exit(1); }
    new_offset_node->offset = offset;
    new_offset_node->next = current_node->offsets;
    current_node->offsets = new_offset_node;
}

/**
 * @brief (MODIFICADA) Escribe el índice en el nuevo formato optimizado.
 * formato: [longitud_skill, skill, num_offsets, offset1, offset2, ...]
 */
void write_index_file(const char* filename) {
    FILE* file_idx = fopen(filename, "wb");
    if (!file_idx) {
        perror("Error al crear el archivo de índice");
        return;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current_node = hashTable[i];
        while (current_node != NULL) {
            // Escribir la habilidad una vez
            size_t skill_len = strlen(current_node->skill);
            fwrite(&skill_len, sizeof(size_t), 1, file_idx);
            fwrite(current_node->skill, sizeof(char), skill_len, file_idx);

            // Contar y luego escribir todos sus offsets
            size_t offset_count = 0;
            OffsetNode* temp_offset = current_node->offsets;
            while (temp_offset != NULL) {
                offset_count++;
                temp_offset = temp_offset->next;
            }

            // Escribir la cantidad de offsets
            fwrite(&offset_count, sizeof(size_t), 1, file_idx);

            // Escribir cada offset
            temp_offset = current_node->offsets;
            while (temp_offset != NULL) {
                fwrite(&temp_offset->offset, sizeof(long), 1, file_idx);
                temp_offset = temp_offset->next;
            }
            
            current_node = current_node->next;
        }
    }
    fclose(file_idx);
}

/**
 * @brief (MODIFICADA) Libera la tabla hash, incluyendo las listas de offsets anidadas.
 */
void free_hash_table() {
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
            free(temp_node->skill);
            free(temp_node);
        }
    }
}

unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % TABLE_SIZE;
}

char* trim_whitespace(char* str) {
    char* end;
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return str;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}
