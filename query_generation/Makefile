PYTHON = python3
SUBSTRAITC = compile_substrait.py 

TEST_DIRS = simple_tests
SQLS = $(foreach dir, $(TEST_DIRS), $(wildcard $(dir)/*.sql))
PROTOS = $(SQLS:.sql=.proto)

.PHONY = all clean

all: $(PROTOS)

%.proto: %.sql
	$(PYTHON) $(SUBSTRAITC) $^ $@

clean:
	rm -f $(PROTOS)