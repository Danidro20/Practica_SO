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

/**
 * Procesa una consulta de búsqueda y devuelve los resultados a través de un pipe.
 * 
 * @param query_fd  Descriptor de archivo del pipe de entrada para leer la consulta
 * @param result_fd Descriptor de archivo del pipe de salida para escribir los resultados
 * 
 * La función realiza los siguientes pasos:
 * 1. Recibe y parsea la consulta del usuario
 * 2. Busca los metadatos de cada criterio en el archivo de habilidades
 * 3. Ordena los criterios por frecuencia (menos frecuentes primero)
 * 4. Realiza la intersección de los resultados en memoria
 * 5. Recupera y devuelve las ofertas coincidentes del archivo CSV
 * 
 * @note La función asume que los archivos de índice (jobs.skl y jobs.idx) existen
 *       y están correctamente formateados.
 */
void search_and_respond(int query_fd, int result_fd) {
    // 1. LECTURA DE LA CONSULTA
    char query_buffer[1024];
    // Leer la consulta del pipe de entrada
    ssize_t bytes_read = read(query_fd, query_buffer, sizeof(query_buffer) - 1);
    if (bytes_read <= 0) return;  // Salir si hay error o cierre de conexión
    query_buffer[bytes_read] = '\0';  // Asegurar terminación de cadena
    
    // Registrar la consulta recibida
    printf("Petición recibida: '%s'\n", query_buffer);

    char* tokens[3];
    int n_criteria = 0;
    char* token = strtok(query_buffer, ";");

    // 2. PROCESAMIENTO DE LA CONSULTA
    // Dividir la consulta en tokens usando ';' como delimitador
    // Se admiten hasta 3 criterios de búsqueda
    while (token != NULL && n_criteria < 3) {
        tokens[n_criteria++] = token;
        token = strtok(NULL, ";");
    }

    // Si no hay criterios, devolvemos NA
    if (n_criteria == 0) { 
        if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados"); 
        return; 
    }

    // 3. BÚSQUEDA DE METADATOS
    // Inicializar estructura para almacenar los criterios de búsqueda
    Criterion criteria[3] = {0};
    
    // Abrir el archivo de habilidades (skills) para buscar los metadatos
    FILE* skl_file = fopen(SKILL_DIR_FILE, "rb");
    if (!skl_file) { 
        // Si no se puede abrir el archivo, responder con error
        if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados"); 
        return; 
    }

    // 4. OBTENCIÓN DE METADATOS
    // Para cada criterio de búsqueda, encontrar sus metadatos (conteo y offset)
    for (int i = 0; i < n_criteria; i++) {
        // Buscar los metadatos de la habilidad en el archivo .skl
        if (!find_skill_metadata(skl_file, tokens[i], &criteria[i])) {
            // Si no se encuentra la habilidad, responder con error
            if (write(result_fd, "NA", 2) == -1) perror("Error al escribir en el pipe de resultados");
            fclose(skl_file);
            // Liberar memoria de habilidades ya encontradas
            for(int j = 0; j < i; j++) free(criteria[j].skill);
            return;
        }
    }
    fclose(skl_file);
    
    // 5. OPTIMIZACIÓN: Ordenar criterios por frecuencia (menos frecuentes primero)
    // Esto mejora el rendimiento de la intersección
    qsort(criteria, n_criteria, sizeof(Criterion), compare_criteria);

    // 6. INTERSECCIÓN DE RESULTADOS
    // Abrir el archivo de índices que contiene los offsets de las ofertas
    FILE* idx_file = fopen(INDEX_FILE, "rb");
    
    // 6.1 Cargar la primera lista de offsets (la más corta) en memoria
    // Esto optimiza la intersección al reducir el espacio de búsqueda inicial
    long* intersection_buffer = malloc(criteria[0].count * sizeof(long));
    fseek(idx_file, criteria[0].offset, SEEK_SET);
    if (fread(intersection_buffer, sizeof(long), criteria[0].count, idx_file) != criteria[0].count) {
        perror("Error al leer los datos de intersección");
        free(intersection_buffer);
        return;
    }
    size_t intersection_size = criteria[0].count;

    // 6.2 Procesar cada criterio adicional
    // Realizar intersección con cada lista de offsets adicional
    for (int i = 1; i < n_criteria; i++) {
        // Si ya no hay elementos en la intersección, terminar temprano
        if (intersection_size == 0) break;

        // Cargar la siguiente lista de offsets a comparar
        long* next_list_buffer = malloc(criteria[i].count * sizeof(long));
        fseek(idx_file, criteria[i].offset, SEEK_SET);
        if (fread(next_list_buffer, sizeof(long), criteria[i].count, idx_file) != criteria[i].count) {
            perror("Error al leer la siguiente lista de offsets");
            free(next_list_buffer);
            free(intersection_buffer);
            return;
        }
        
        // Buffer para la nueva intersección
        long* new_intersection_buffer = malloc(intersection_size * sizeof(long));
        size_t new_size = 0;
        
        // 6.3 ALGORITMO DE DOS PUNTEROS
        // Eficiente para intersección de listas ordenadas (O(n+m) tiempo)
        size_t ptr1 = 0, ptr2 = 0;
        while(ptr1 < intersection_size && ptr2 < criteria[i].count) {
            if (intersection_buffer[ptr1] < next_list_buffer[ptr2]) {
                // Avanzar en la primera lista
                ptr1++;
            } else if (next_list_buffer[ptr2] < intersection_buffer[ptr1]) {
                // Avanzar en la segunda lista
                ptr2++;
            } else {
                // Coincidencia encontrada: guardar el offset y avanzar ambos punteros
                new_intersection_buffer[new_size++] = intersection_buffer[ptr1];
                ptr1++;
                ptr2++;
            }
        }
        
        // 6.4 ACTUALIZAR BUFFER DE INTERSECCIÓN
        // Liberar buffers antiguos y actualizar con la nueva intersección
        free(intersection_buffer);
        free(next_list_buffer);
        intersection_buffer = new_intersection_buffer;
        intersection_size = new_size;
    }
    fclose(idx_file);  // Cerrar archivo de índices

    // 7. CONSTRUCCIÓN DE LA RESPUESTA
    if (intersection_size == 0) {
        // 7.1 Caso: No hay resultados de búsqueda
        if (write(result_fd, "NA", 2) == -1) {
            perror("Error al escribir en el pipe de resultados");
        }
    } else {
        // 7.2 Caso: Hay resultados
        char final_response[8192] = "";  // Buffer para la respuesta final
        char line_buffer[4096];           // Buffer para leer líneas del CSV
        
        // Abrir el archivo CSV para leer las ofertas
        FILE* csv_file = fopen("data.csv", "r");
        
        // Para cada offset en la intersección
        for(size_t i = 0; i < intersection_size; i++) {
            // Saltar a la posición del offset en el archivo CSV
            if (fseek(csv_file, intersection_buffer[i], SEEK_SET) == 0) {
                // Leer la línea completa de la oferta
                if (fgets(line_buffer, sizeof(line_buffer), csv_file)) {
                    // Verificar que la respuesta no exceda el tamaño máximo
                    if (strlen(final_response) + strlen(line_buffer) < sizeof(final_response) - 30) {
                        strcat(final_response, line_buffer);
                    } else {
                        // Si se excede el tamaño, truncar y salir
                        strcat(final_response, "\n... (resultados truncados) ...");
                        break;
                    }
                }
            }
        }
        fclose(csv_file);
        
        // 7.3 Enviar la respuesta a través del pipe de salida
        if (write(result_fd, final_response, strlen(final_response)) == -1) {
            perror("Error al escribir la respuesta final");
        }
    }

    // 8. LIMPIEZA
    // Liberar la memoria asignada para los buffers
    free(intersection_buffer);
    
    // Liberar las cadenas de habilidades copiadas
    for(int i = 0; i < n_criteria; i++) {
        free(criteria[i].skill);
    }
}


void cleanup(int signum) {
    (void)signum;
    printf("\nCerrando el motor de búsqueda...\n");
    unlink(QUERY_PIPE);
    unlink(RESULT_PIPE);
    printf("Recursos liberados. Adiós.\n");
    exit(0);
}
