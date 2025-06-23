#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define QUERY_PIPE "/tmp/job_query_pipe"
#define RESULT_PIPE "/tmp/job_result_pipe"

void clean_stdin();

int main()
{
    char *criteria[3] = {NULL, NULL, NULL};
    int choice;

    while (1)
    {
        printf("\n--- Buscador de Empleos Multi-Criterio ---\n");
        printf("1. Ingresar primer criterio (Actual: %s)\n", criteria[0] ? criteria[0] : "Ninguno");
        printf("2. Ingresar segundo criterio (Actual: %s)\n", criteria[1] ? criteria[1] : "Ninguno");
        printf("3. Ingresar tercer criterio (Actual: %s)\n", criteria[2] ? criteria[2] : "Ninguno");
        printf("4. Realizar búsqueda\n");
        printf("5. Salir\n");
        printf("Seleccione una opción: ");

        if (scanf("%d", &choice) != 1)
        {
            printf("Entrada inválida.\n");
            clean_stdin();

            continue;
        }

        clean_stdin();

        if (choice >= 1 && choice <= 3)
        {
            int index = choice - 1;
            char buffer[256];

            printf("Ingrese el valor para el criterio %d: ", choice);
            fgets(buffer, sizeof(buffer), stdin);
            buffer[strcspn(buffer, "\n")] = 0;

            if (criteria[index] != NULL)
            {
                free(criteria[index]); // Liberar criterio anterior si existe
            }

            criteria[index] = strdup(buffer);
        }
        else if (choice == 4)
        {
            char query_string[1024] = "";
            int first = 1;

            for (int i = 0; i < 3; i++)
            {
                if (criteria[i] != NULL && strlen(criteria[i]) > 0)
                {
                    if (!first)
                    {
                        strcat(query_string, ";");
                    }

                    strcat(query_string, criteria[i]);
                    first = 0;
                }
            }

            if (strlen(query_string) == 0)
            {
                printf("Error: Debe ingresar al menos un criterio de búsqueda.\n");

                continue;
            }

            // Enviar la consulta al motor
            int query_fd = open(QUERY_PIPE, O_WRONLY);

            write(query_fd, query_string, strlen(query_string));
            close(query_fd);

            // Recibir respuesta
            char result_buffer[8192];
            int result_fd = open(RESULT_PIPE, O_RDONLY);
            ssize_t bytes_read = read(result_fd, result_buffer, sizeof(result_buffer) - 1);

            close(result_fd);

            result_buffer[bytes_read] = '\0';
            printf("\n--- Resultados de la Búsqueda ---\n");

            if (strcmp(result_buffer, "NA") == 0)
            {
                printf("No se encontraron ofertas con TODOS los criterios especificados.\n");
            }
            else
            {
                printf("%s", result_buffer);
            }

            printf("---------------------------------\n");
        }
        else if (choice == 5)
        {
            break;
        }
        else
        {
            printf("Opción no válida.\n");
        }
    }

    // Liberar memoria final
    for (int i = 0; i < 3; i++)
    {
        if (criteria[i])
        {
            free(criteria[i]);
        }
    }

    printf("Saliendo... ¡Adiós!\n");

    return 0;
}

void clean_stdin()
{
    int c;

    while ((c = getchar()) != '\n' && c != EOF)
    {
    }
}
