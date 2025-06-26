#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <signal.h>
#include <math.h>
#include <sys/wait.h>  // Para WIFEXITED, WEXITSTATUS, fork, wait
#include <unistd.h>    // Para fork, exec, exit
#include <stdlib.h>    // Para exit, EXIT_FAILURE
#include <signal.h>    // Para kill, SIGTERM
#include "utils.h"

#define INDEX_FILE "dist/jobs.idx"
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

        // Crear el directorio dist si no existe
        mkdir("dist", 0755);
        
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
    
    pid_t engine_pid = fork();
    if (engine_pid == 0) {
        // Proceso hijo: ejecutar el motor
        if (execl("./dist/engine", "./dist/engine", (char *)NULL) == -1) {
            perror("Error al ejecutar el motor");
            exit(EXIT_FAILURE);
        }
    } else if (engine_pid < 0) {
        perror("Error al crear proceso hijo para el motor");
        return 1;
    }

    // Pequeña pausa para asegurar que el motor esté listo
    sleep(1);

    // Iniciar la interfaz de usuario en primer plano
    printf("Iniciando interfaz de usuario...\n");
    if (!file_exists("dist/ui")) {
        fprintf(stderr, "Error: El ejecutable 'dist/ui' no existe. Ejecute 'make' primero.\n");
        kill(engine_pid, SIGTERM);  // Detener el motor
        return 1;
    }
    
    // Ejecutar la UI en primer plano
    int ui_status = system("./dist/ui");
    if (ui_status == -1) {
        perror("Error al iniciar la interfaz de usuario");
        kill(engine_pid, SIGTERM);  // Detener el motor
        return 1;
    } else if (WIFEXITED(ui_status)) {
        int exit_code = WEXITSTATUS(ui_status);
        if (exit_code != 0) {
            fprintf(stderr, "La interfaz de usuario terminó con código de error: %d\n", exit_code);
        }
    }

    // Limpieza
    printf("\nFinalizando servicios...\n");
    int ret = system("pkill -f dist/engine");
    if (ret == -1) {
        perror("Error al intentar detener el motor");
    } else if (WIFEXITED(ret)) {
        int exit_status = WEXITSTATUS(ret);
        if (exit_status != 0 && exit_status != 1) {  // pkill devuelve 1 si no encontró procesos para matar
            fprintf(stderr, "Error al detener el motor (código %d)\n", exit_status);
        }
    }
    printf("\n=== Sistema finalizado ===\n");

    return 0;
}