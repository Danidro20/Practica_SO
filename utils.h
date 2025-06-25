#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <math.h>

bool file_exists(const char *filename);
int execute_command(const char *command);
void format_time(char *buffer, size_t size, const struct timespec *start, const struct timespec *end);


#endif