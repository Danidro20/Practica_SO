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

        if (!file_exists("dist/index")) {
            fprintf(stderr, "Error: El ejecutable 'dist/index' no existe. Ejecute 'make' primero.\n");
            return 1;
        }

        if (execute_command("./dist/index") != 0)
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
    if (!file_exists("dist/engine")) {
        fprintf(stderr, "Error: El ejecutable 'dist/engine' no existe. Ejecute 'make' primero.\n");
        return 1;
    }
    if (execute_command("./dist/engine") != 0)
    {
        fprintf(stderr, "Error al iniciar el motor de búsqueda\n");
        return 1;
    }

    // Pequeña pausa para asegurar que el motor esté listo
    sleep(1);

    // Iniciar la interfaz de usuario
    printf("Iniciando interfaz de usuario...\n");
    if (!file_exists("dist/ui")) {
        fprintf(stderr, "Error: El ejecutable 'dist/ui' no existe. Ejecute 'make' primero.\n");
        return 1;
    }
    if (execute_command("./dist/ui") != 0)
    {
        fprintf(stderr, "Error al iniciar la interfaz de usuario\n");
        return 1;
    }

    // Limpieza
    printf("\nFinalizando servicios...\n");
    system("pkill -f dist/engine");
    printf("\n=== Sistema finalizado ===\n");

    return 0;
}