#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <signal.h>
#include <math.h>
#include "utils.h"

#define INDEX_FILE "dist/jobs.idx.zst"
#define DATA_FILE "data.csv"

int main()
{
    printf("=== Iniciando sistema de búsqueda de ofertas ===\n");

    // Verificar si el archivo de datos existe
    if (!file_exists(DATA_FILE))
    {
        fprintf(stderr, "Error: El archivo %s no existe en el directorio actual\n", DATA_FILE);

        return 1;
    }

    // Verificar si el índice existe, si no, generarlo
    if (!file_exists(INDEX_FILE))
    {
        printf("El índice no existe. Generando índice...\n");

        if (execute_command("yarn index") != 0)
        {
            fprintf(stderr, "Error al generar el índice\n");

            return 1;
        }
    }
    else
    {
        printf("Índice encontrado. Para regenerarlo, borre el archivo %s\n", INDEX_FILE);
    }

    // Crear pipes si no existen
    if (!file_exists("/tmp/job_query_pipe"))
    {
        if (mkfifo("/tmp/job_query_pipe", 0666) == -1)
        {
            perror("Error al crear el pipe de consultas");
            // Continuar, puede que ya exista
        }
    }

    if (!file_exists("/tmp/job_result_pipe"))
    {
        if (mkfifo("/tmp/job_result_pipe", 0666) == -1)
        {
            perror("Error al crear el pipe de resultados");
            // Continuar, puede que ya exista
        }
    }

    printf("\n=== Iniciando servicios ===\n");
    // Iniciar el motor en segundo plano
    printf("Iniciando motor de búsqueda...\n");

    pid_t engine_pid = fork();

    if (engine_pid == 0)
    {
        // Proceso hijo - motor de búsqueda
        execlp("./dist/engine", "engine", (char *)NULL);
        perror("Error al iniciar el motor");
        exit(1);
    }
    else if (engine_pid < 0)
    {
        perror("Error al crear proceso para el motor");

        return 1;
    }

    // Pequeña pausa para asegurar que el motor esté listo
    sleep(1);

    // Iniciar la interfaz de usuario
    printf("Iniciando interfaz de usuario...\n");
    printf("----------------------------------------\n");

    int ui_status = system("./dist/ui");

    if (ui_status != 0)
    {
        fprintf(stderr, "La interfaz de usuario terminó con un error\n");
    }

    // Limpieza
    printf("\nDeteniendo el motor de búsqueda...\n");
    kill(engine_pid, SIGTERM);
    printf("\n=== Sistema finalizado ===\n");

    return 0;
}