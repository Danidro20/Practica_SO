#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define SKILL_DIR_FILE "dist/jobs.skl"
#define INDEX_FILE "dist/jobs.idx"
#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

// Estructura para guardar metadatos de un criterio de búsqueda
typedef struct {
    char* skill;
    size_t count;
    long offset;
} Criterion;

// Prototipos
int find_skill_metadata(FILE* skl_file, const char* skill, Criterion* meta);
void search_and_respond(int query_fd, int result_fd);
void cleanup(int signum);
int compare_criteria(const void* a, const void* b);

int main() {
    printf("Motor de búsqueda iniciando (modo de memoria mínima)...\n");
    signal(SIGINT, cleanup);

    // No se carga nada en memoria al inicio.
    
    umask(0);
    mkfifo(QUERY_PIPE, 0666);
    mkfifo(RESULT_PIPE, 0666);
    printf("Tuberías creadas. Esperando peticiones...\n");

    while (1) {
        int query_fd = open(QUERY_PIPE, O_RDONLY);
        int result_fd = open(RESULT_PIPE, O_WRONLY);
        if (query_fd == -1 || result_fd == -1) {
            perror("Error al abrir las tuberías");
            exit(EXIT_FAILURE);
        }
        search_and_respond(query_fd, result_fd);
        close(query_fd);
        close(result_fd);
    }
    return 0;
}

// Realiza búsqueda binaria en el archivo .skl para encontrar metadata.
int find_skill_metadata(FILE* skl_file, const char* skill, Criterion* meta) {
    fseek(skl_file, 0, SEEK_SET); // Ir al inicio
    size_t total_skills;
    // Leer total de skills
    if (fread(&total_skills, sizeof(size_t), 1, skl_file) != 1) {
        perror("Error al leer el número total de habilidades");
        return 0;
    }

    // NOTA: Una búsqueda binaria real en archivo es compleja.
    // Para simplificar, haremos una búsqueda lineal que es más lenta
    // pero igual de eficiente en memoria. Si la velocidad se vuelve un problema,
    // se puede implementar la búsqueda binaria aquí.
    
    for(size_t i = 0; i < total_skills; i++) {
        size_t skill_len;
        if (fread(&skill_len, sizeof(size_t), 1, skl_file) != 1) return 0; // Fin de archivo

        char* skill_buffer = malloc(skill_len + 1);
        if (fread(skill_buffer, 1, skill_len, skl_file) != skill_len) {
            free(skill_buffer);
            return 0;
        }
        skill_buffer[skill_len] = '\0';
        
        int is_match = (strcmp(skill, skill_buffer) == 0);
        free(skill_buffer);
        
        if (is_match) {
            meta->skill = strdup(skill);
            if (fread(&meta->count, sizeof(size_t), 1, skl_file) != 1) {
                return 0;
            }
            if (fread(&meta->offset, sizeof(long), 1, skl_file) != 1) {
                return 0;
            }
            return 1; // Encontrado
        } else {
            // Si no es, saltar el resto de los metadatos de esta entrada
            fseek(skl_file, sizeof(size_t) + sizeof(long), SEEK_CUR);
        }
    }
    return 0; // No encontrado
}

int compare_criteria(const void* a, const void* b) {
    Criterion* critA = (Criterion*)a;
    Criterion* critB = (Criterion*)b;
    if (critA->count < critB->count) return -1;
    if (critA->count > critB->count) return 1;
    return 0;
}

// Busca e interseca trabajando desde disco.
void search_and_respond(int query_fd, int result_fd) {
    char query_buffer[1024];
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);
    if (bytes_read <= 0) return;
    query_buffer[bytes_read] = '\0';
    printf("Petición recibida: '%s'\n", query_buffer);

    char* tokens[3];
    int n_criteria = 0;
    char* token = strtok(query_buffer, ";");
    while (token != NULL && n_criteria < 3) {
        tokens[n_criteria++] = token;
        token = strtok(NULL, ";");
    }
    if (n_criteria == 0) { 
        if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados"); 
        return; 
    }

    Criterion criteria[3] = {0};
    FILE* skl_file = fopen(SKILL_DIR_FILE, "rb");
    if (!skl_file) { 
        if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados"); 
        return; 
    }

    for (int i = 0; i < n_criteria; i++) {
        if (!find_skill_metadata(skl_file, tokens[i], &criteria[i])) {
            if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados");
            fclose(skl_file);
            for(int j = 0; j < i; j++) free(criteria[j].skill);
            return;
        }
    }
    fclose(skl_file);
    qsort(criteria, n_criteria, sizeof(Criterion), compare_criteria);

    // --- Intersección desde Disco ---
    FILE* idx_file = fopen(INDEX_FILE, "rb");
    
    // Cargar la primera lista (la más corta) en un búfer temporal.
    long* intersection_buffer = malloc(criteria[0].count * sizeof(long));
    fseek(idx_file, criteria[0].offset, SEEK_SET);
    if (fread(intersection_buffer, sizeof(long), criteria[0].count, idx_file) != criteria[0].count) {
        perror("Error al leer los datos de intersección");
        free(intersection_buffer);
        return;
    }
    size_t intersection_size = criteria[0].count;

    // Intersecar con las demás listas
    for (int i = 1; i < n_criteria; i++) {
        if (intersection_size == 0) break;

        long* next_list_buffer = malloc(criteria[i].count * sizeof(long));
        fseek(idx_file, criteria[i].offset, SEEK_SET);
        if (fread(next_list_buffer, sizeof(long), criteria[i].count, idx_file) != criteria[i].count) {
            perror("Error al leer la siguiente lista de offsets");
            free(next_list_buffer);
            free(intersection_buffer);
            return;
        }
        
        long* new_intersection_buffer = malloc(intersection_size * sizeof(long));
        size_t new_size = 0;
        
        size_t ptr1 = 0, ptr2 = 0;
        while(ptr1 < intersection_size && ptr2 < criteria[i].count) {
            if (intersection_buffer[ptr1] < next_list_buffer[ptr2]) ptr1++;
            else if (next_list_buffer[ptr2] < intersection_buffer[ptr1]) ptr2++;
            else {
                new_intersection_buffer[new_size++] = intersection_buffer[ptr1];
                ptr1++; ptr2++;
            }
        }
        
        free(intersection_buffer);
        free(next_list_buffer);
        intersection_buffer = new_intersection_buffer;
        intersection_size = new_size;
    }
    fclose(idx_file);

    // --- Construir Respuesta ---
    if (intersection_size == 0) {
        if (write(result_fd, "NA", 2) == -1) {
            perror("Error al escribir en el pipe de resultados");
        }
    } else {
        char final_response[8192] = "";
        char line_buffer[4096];
        FILE* csv_file = fopen("data.csv", "r");
        for(size_t i = 0; i < intersection_size; i++) {
             if (fseek(csv_file, intersection_buffer[i], SEEK_SET) == 0) {
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
        if (write(result_fd, final_response, strlen(final_response)) == -1) {
            perror("Error al escribir la respuesta final");
        }
    }

    free(intersection_buffer);
    for(int i = 0; i < n_criteria; i++) free(criteria[i].skill);
}


void cleanup(int signum) {
    (void)signum;
    printf("\nCerrando el motor de búsqueda...\n");
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}
