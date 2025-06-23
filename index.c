#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

// --- Definiciones de la Tabla Hash ---
#define TABLE_SIZE 4520789

typedef struct HashNode {
    char* skill;
    long offset;
    struct HashNode* next;
} HashNode;

HashNode* hashTable[TABLE_SIZE];

// --- Prototipos de Funciones ---
unsigned long hash_function(const char* str);
void insert_skill(const char* skill, long offset); // Modificada
void write_index_file(const char* filename);
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
 * @brief Inserta una habilidad y su offset en la tabla hash.
 * ESTA VERSIÓN SIEMPRE INSERTA, PERMITIENDO MÚLTIPLES OFFSETS POR HABILIDAD.
 * @param skill La habilidad a insertar.
 * @param offset El byte de inicio de la línea en el CSV.
 */
void insert_skill(const char* skill, long offset) {
    unsigned long index = hash_function(skill);

    // No revisamos si ya existe. Simplemente creamos un nuevo nodo y lo añadimos.
    HashNode* newNode = (HashNode*)malloc(sizeof(HashNode));
    if (!newNode) {
        perror("Fallo al alocar memoria para HashNode");
        exit(1);
    }
    
    newNode->skill = strdup(skill);
    if(!newNode->skill) {
        perror("Fallo al alocar memoria para la habilidad");
        free(newNode);
        exit(1);
    }

    newNode->offset = offset;
    newNode->next = hashTable[index]; 
    hashTable[index] = newNode;
}


unsigned long hash_function(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % TABLE_SIZE;
}


void write_index_file(const char* filename) {
    FILE* file_idx = fopen(filename, "wb");
    if (!file_idx) {
        perror("Error al crear el archivo de índice");
        return;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current = hashTable[i];
        while (current != NULL) {
            size_t skill_len = strlen(current->skill);

            // Escribir la longitud de la cadena de la habilidad
            fwrite(&skill_len, sizeof(size_t), 1, file_idx);
            // Escribir la cadena de la habilidad
            fwrite(current->skill, sizeof(char), skill_len, file_idx);
            // Escribir el offset
            fwrite(&current->offset, sizeof(long), 1, file_idx);

            current = current->next;
        }
    }

    fclose(file_idx);
}

void free_hash_table() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        HashNode* current = hashTable[i];
        while (current != NULL) {
            HashNode* temp = current;
            current = current->next;
            free(temp->skill); // Liberar la cadena duplicada con strdup
            free(temp);        // Liberar el nodo
        }
    }
}


char* trim_whitespace(char* str) {
    char* end;

    // Trim leading space
    while (isspace((unsigned char)*str)) str++;

    if (*str == 0)
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;

    // Write new null terminator character
    end[1] = '\0';

    return str;
}
