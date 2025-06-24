#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// --- Definiciones de la Tabla Hash ---
// Un tamaño razonable para minimizar colisiones durante la indexación.
#define TABLE_SIZE 4520789

// Nodo para la lista enlazada de offsets
typedef struct OffsetNode {
    long offset;
    struct OffsetNode* next;
} OffsetNode;

// Nodo de la tabla hash
typedef struct HashNode {
    char* skill;
    OffsetNode* offsets;
    struct HashNode* next;
} HashNode;

HashNode* hashTable[TABLE_SIZE];

// --- Prototipos de Funciones ---
unsigned long hash_function(const char* str);
void insert_skill(const char* skill, long offset);
void write_index_files(const char* skl_filename, const char* idx_filename); // Lógica modificada
void free_hash_table();
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

    // Ignorar la cabecera del CSV
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
    printf("El archivo CSV ha sido procesado. Construyendo los índices...\n");

    // Llama a la función para escribir los dos archivos de índice.
    write_index_files("jobs.skl", "jobs.idx");
    printf("Archivos de índice 'jobs.skl' y 'jobs.idx' creados exitosamente.\n");

    free_hash_table();
    printf("Memoria liberada.\n");

    return 0;
}

/**
 * @brief Escribe la tabla hash en dos archivos separados:
 * - .skl: Un directorio de habilidades (índice primario para carga en memoria).
 * - .idx: Los datos brutos de las listas de offsets (índice secundario en disco).
 *
 * Formato .skl: [longitud_skill, skill, num_offsets, offset_en_idx] para cada habilidad.
 * Formato .idx: [offset1, offset2, ...] para cada habilidad, una tras otra.
 */
void write_index_files(const char* skl_filename, const char* idx_filename) {
    FILE* file_skl = fopen(skl_filename, "wb");
    FILE* file_idx = fopen(idx_filename, "wb");
    if (!file_skl || !file_idx) {
        perror("Error al crear archivos de índice");
        if(file_skl) fclose(file_skl);
        if(file_idx) fclose(file_idx);
        return;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current_node = hashTable[i];
        while (current_node != NULL) {
            // 1. Contar los offsets para la habilidad actual.
            size_t offset_count = 0;
            OffsetNode* temp_offset = current_node->offsets;
            while (temp_offset != NULL) {
                offset_count++;
                temp_offset = temp_offset->next;
            }

            if (offset_count > 0) {
                // 2. Obtener la posición actual en el archivo .idx donde se guardará la lista.
                long idx_offset = ftell(file_idx);

                // 3. Escribir la entrada del directorio en el archivo .skl (skill, conteo y offset).
                size_t skill_len = strlen(current_node->skill);
                fwrite(&skill_len, sizeof(size_t), 1, file_skl);
                fwrite(current_node->skill, sizeof(char), skill_len, file_skl);
                fwrite(&offset_count, sizeof(size_t), 1, file_skl);
                fwrite(&idx_offset, sizeof(long), 1, file_skl);

                // 4. Escribir la lista de offsets completa en el archivo .idx.
                temp_offset = current_node->offsets;
                while (temp_offset != NULL) {
                    fwrite(&temp_offset->offset, sizeof(long), 1, file_idx);
                    temp_offset = temp_offset->next;
                }
            }
            current_node = current_node->next;
        }
    }
    fclose(file_skl);
    fclose(file_idx);
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
        if (!current_node) { perror("malloc HashNode"); exit(1); }
        current_node->skill = strdup(skill);
        if (!current_node->skill) { perror("strdup skill"); exit(1); }
        current_node->offsets = NULL;
        current_node->next = hashTable[index];
        hashTable[index] = current_node;
    }
    OffsetNode* new_offset_node = (OffsetNode*)malloc(sizeof(OffsetNode));
    if (!new_offset_node) { perror("malloc OffsetNode"); exit(1); }
    new_offset_node->offset = offset;
    new_offset_node->next = current_node->offsets;
    current_node->offsets = new_offset_node;
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

unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
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
