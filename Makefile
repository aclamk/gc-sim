all: sim

CEPH_DIR ?= .
CEPH_CONF ?= $(CEPH_DIR)/build/ceph.conf

sim: sim.c
	gcc -Wall -g sim.c -I $(CEPH_DIR)/src/include -L$(CEPH_DIR)/build/lib -lrados -lpthread -o sim

run: sim
	CEPH_CONF=$(CEPH_CONF) LD_LIBRARY_PATH=$(CEPH_DIR)/build/lib ./sim replay-gc.9.short-2.txt

valgrind: sim
	CEPH_CONF=$(CEPH_CONF) LD_LIBRARY_PATH=$(CEPH_DIR)/build/lib valgrind --leak-check=full ./sim

prepare: 
	cd $(CEPH_DIR)/build; ./bin/ceph osd pool create test_pool
	cd $(CEPH_DIR)/build; ./bin/rados -p test_pool sim
