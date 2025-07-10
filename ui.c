#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include "utils.h"
#include <arpa/inet.h>
#include <signal.h>

#define PORT 5050
#define BUFFER_SIZE 1024
/**
 * Server IP address
 * 127.0.0.1 is localhost
 * Change this to the server's IP address
 */
#define HOST "127.0.0.1"

int serverFd = -1;

void clean_stdin() {
    int c;
    while ((c = getchar()) != '\n' && c != EOF);
}

void cleanup(int signal)
{
    printf("\nSeñal %d recibida\n", signal);
    close(serverFd);
    exit(0);
}

int main(void) {
    int check;

    // Usar señales para cerrar el servidor
    struct sigaction sa;
    sa.sa_handler = cleanup;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    // Configurar manejo de SIGINT (Ctrl+C)
    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("Error al configurar SIGINT");
        exit(1);
    }

    // Configurar manejo de SIGTERM
    if (sigaction(SIGTERM, &sa, NULL) == -1) {
        perror("Error al configurar SIGTERM");
        exit(1);
    }

    printf("Iniciando cliente en %s:%d\n", HOST, PORT);

    struct sockaddr_in server;
    
    // Creando descriptor de archivo del socket
    serverFd = socket(AF_INET, SOCK_STREAM, 0);

    if (serverFd < 0)
    {
        perror("Error al crear el socket");
        exit(1);
    }

    int opt = 1;

    // Permite reutilizar el socket
    check = setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (check < 0)
    {
        perror("Error al configurar el socket");
        close(serverFd);
        exit(1);
    }

    // Configurar el servidor
    server.sin_port = htons(PORT);
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(HOST);

    // Conectar al servidor
    check = connect(serverFd, (struct sockaddr *)&server, sizeof(server));

    if (check < 0)
    {
        perror("Error al conectar con el servidor");
        close(serverFd);
        exit(1);
    }

    printf("Conectado al servidor en %s:%d\n", HOST, PORT);

    char buffer[BUFFER_SIZE] = {0};

    // Recibir respuesta del servidor
    check = recv(serverFd, buffer, BUFFER_SIZE, 0);

    if (check <= 0)
    {
        if (check == 0) {
            printf("El servidor ha cerrado la conexión\n");
        } else {
            perror("Error al recibir la respuesta");
        }
        close(serverFd);
        exit(1);
    }
    
    buffer[check] = '\0'; // 0 al final
    printf("Respuesta del servidor: %s\n", buffer);

    char* criteria[3] = {NULL, NULL, NULL};
    int choice;

    while (1) {
        printf("\n--- Buscador de Empleos Multi-Criterio ---\n");
        printf("1. Ingresar primer criterio (Actual: %s)\n", criteria[0] ? criteria[0] : "Ninguno");
        printf("2. Ingresar segundo criterio (Actual: %s)\n", criteria[1] ? criteria[1] : "Ninguno");
        printf("3. Ingresar tercer criterio (Actual: %s)\n", criteria[2] ? criteria[2] : "Ninguno");
        printf("4. Realizar búsqueda\n");
        printf("5. Salir\n");
        printf("Seleccione una opción: ");

        if (scanf("%d", &choice) != 1) {
            printf("Entrada inválida.\n");
            clean_stdin();
            continue;
        }
        clean_stdin();

        if (choice >= 1 && choice <= 3) {
            int index = choice - 1;
            char buffer[256];
            printf("Ingrese el valor para el criterio %d: ", choice);
            if (!fgets(buffer, sizeof(buffer), stdin)) {
                printf("Error al leer la entrada. Intente de nuevo.\n");
                clean_stdin();
                continue;
            }
            buffer[strcspn(buffer, "\n")] = 0;

            if (criteria[index] != NULL) {
                free(criteria[index]); // Liberar criterio anterior si existe
            }
            criteria[index] = strdup(buffer);
        } else if (choice == 4) {
            char query_string[BUFFER_SIZE] = "";
            int first = 1;
            for (int i = 0; i < 3; i++) {
                if (criteria[i] != NULL && strlen(criteria[i]) > 0) {
                    if (!first) {
                        strcat(query_string, ";");
                    }
                    strcat(query_string, criteria[i]);
                    first = 0;
                }
            }

            if (strlen(query_string) == 0) {
                printf("Error: Debe ingresar al menos un criterio de búsqueda.\n");
                continue;
            }

            // Medir tiempo de respuesta
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);
            
            // Enviar la consulta al motor
            check = send(serverFd, query_string, strlen(query_string), 0);

            if (check < 0) {
                perror("Error al enviar la consulta al motor");
                close(serverFd);
                continue;
            }

            // Recibir respuesta
            char result_buffer[8192];
            ssize_t bytes_read = recv(serverFd, result_buffer, sizeof(result_buffer) - 1, 0);
            
            // Calcular tiempo transcurrido
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            char time_buffer[100];
            format_time(time_buffer, sizeof(time_buffer), &start_time, &end_time);

            result_buffer[bytes_read] = '\0';
            printf("\n--- Resultados de la Búsqueda ---\n");
            printf("Tiempo de respuesta: %s\n\n", time_buffer);
            if (strcmp(result_buffer, "NA") == 0) {
                // No se encontraron ofertas con TODOS los criterios especificados.
                printf("NA\n");
            } else {
                printf("%s", result_buffer);
            }
            printf("---------------------------------\n");

        } else if (choice == 5) {
            break;
        } else {
            printf("Opción no válida.\n");
        }
    }

    // Liberar memoria final
    for (int i = 0; i < 3; i++) {
        if (criteria[i]) free(criteria[i]);
    }

    printf("Saliendo... ¡Adiós!\n");

    // Cerrar el socket
    close(serverFd);
    exit(0);
}


