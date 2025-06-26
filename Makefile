all: dist dist/index dist/engine dist/ui dist/main

dist:
	@mkdir -p dist

dist/index: index.c utils.c | dist
	gcc -Wall -Wextra -O2 -o $@ $^ -lzstd -lm

dist/engine: engine.c | dist
	gcc -Wall -Wextra -O2 -o $@ $^ -lzstd -lm

dist/ui: ui.c utils.c | dist
	gcc -Wall -Wextra -O2 -o $@ $^ -lm

dist/main: p1-dataProgram.c utils.c | dist
	gcc -Wall -Wextra -O2 -o $@ $^ -lzstd -lm

clean:
	rm -rf dist

.PHONY: all clean
