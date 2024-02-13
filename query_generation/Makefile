PYTHON = python3
SUBSTRAITC = compile_substrait.py 

TEST_DIRS = simple_tests complex_graph
SQLS = $(foreach dir, $(TEST_DIRS), $(wildcard $(dir)/*.sql))
PROTOS = $(SQLS:.sql=.obj)

.PHONY = all clean

all: $(PROTOS)

%.obj: %.sql
	$(PYTHON) $(SUBSTRAITC) $^ $@

clean:
	rm -f $(PROTOS)