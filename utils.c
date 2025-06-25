#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <math.h>

// Función para verificar si un archivo existe
bool file_exists(const char *filename)
{
    struct stat st;

    return (stat(filename, &st) == 0);
}

// Función para ejecutar un comando y verificar si tuvo éxito
int execute_command(const char *command)
{
    printf("Ejecutando: %s\n", command);
    int status = system(command);

    if (WIFEXITED(status))
    {
        return WEXITSTATUS(status);
    }

    return -1;
}

void format_time(char *buffer, size_t size, const struct timespec *start, const struct timespec *end) {
    double elapsed = (end->tv_sec - start->tv_sec) * 1000.0 + (end->tv_nsec - start->tv_nsec) / 1e6; // en milisegundos
    
    if (elapsed < 1000.0) {
        snprintf(buffer, size, "%.2f ms", elapsed);
    } else if (elapsed < 60000.0) {
        snprintf(buffer, size, "%.2f segundos", elapsed / 1000.0);
    } else {
        int minutes = (int)(elapsed / 60000);
        double seconds = fmod(elapsed / 1000.0, 60.0);
        snprintf(buffer, size, "%d minutos y %.2f segundos", minutes, seconds);
    }
}

// Implementa aquí más funciones de utilidad