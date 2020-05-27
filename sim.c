#include <rados/librados.h>
#include <assert.h>
#include <stdio.h>
#include <semaphore.h>
#include <stdlib.h>
#include <stdbool.h>

static const char* pool_name="test_pool";
static const char* object_name="object";

int main(int argc, char** argv)
{
  uint8_t* object_data = NULL;
  size_t object_size = 0;
  uint8_t cycling_pattern = 2;
  
  rados_t cluster;
  rados_ioctx_t io_ctx;
  int r;
  r = rados_create(&cluster, NULL);
  assert(r == 0);
  r = rados_conf_read_file(cluster, NULL);
  assert(r == 0);
  r = rados_connect(cluster);
  assert(r == 0);
  r = rados_ioctx_create(cluster, pool_name, &io_ctx);
  assert(r == 0);

  char* line = NULL;
  size_t linen = 0;
  ssize_t s;
  FILE* f = fopen(argv[1], "r");

  rados_write_op_t write_op = NULL;
  rados_remove(io_ctx, object_name);
  
  
  for( ; (s = getline(&line, &linen, f)) > 0 ; ) {
    char* cmd = line;
    size_t offset = 0;
    size_t size = 0;
    char* p = strchr(line, ' ');
    char* pe;
    if(p) {
      *p = 0;
      p++;
      offset = strtol(p, &pe, 16);
      size = strtol(pe + 1, NULL, 16);
    } else {
      char* p = strchr(line, '\n');
      if (p) *p = 0;
    }
    printf("%s 0x%lx 0x%lx\n", cmd, offset, size);

    if(strcmp(cmd, "transaction") == 0) {
      if (write_op) {
	r = rados_write_op_operate(write_op, io_ctx, object_name, NULL, 0);
	assert(r == 0);
	rados_release_write_op(write_op);
	write_op = NULL;
      }     
      write_op = rados_create_write_op();
    } else if (strcmp(cmd, "read") == 0) {
      if (write_op) {
	r = rados_write_op_operate(write_op, io_ctx, object_name, NULL, 0);
	assert(r == 0);
	rados_release_write_op(write_op);
	write_op = NULL;
      }
      
      uint8_t* object_read = (uint8_t*) malloc(size);
      r = rados_read(io_ctx, object_name, (char*)object_read, size, offset);
      assert(r == size);

      assert(memcmp(object_read, object_data + offset, size) == 0);
      
      if (offset < 0x1000) {
	bool b = true;
	for (size_t i = 0; i < size; i++) {
	  b = b & (object_read[i] <= 1);
	}
	if (!b) printf("ERROR: gc gets invalid head\n");
      } else {
	bool b = true;
	for (size_t i = 0; i < size; i++) {
	  b = b & (object_read[i] >= 2);
	}
	if (!b) printf("ERROR: entry gets some of gc head\n");
      }
      
    } else if (strcmp(cmd, "_touch") == 0) {
      //do nothing
      assert(write_op);      
    } else if (strcmp(cmd, "_zero") == 0) {

      assert(write_op);
      if (offset + size > object_size) {
	object_data = (uint8_t*)realloc(object_data, offset + size);
	memset(object_data + object_size, 0, offset + size - object_size);
	object_size = offset + size;
      }
      memset(object_data + offset, 0, size);
      rados_write_op_zero(write_op, offset, size);
            
    } else if (strcmp(cmd, "_write") == 0) {

      assert(write_op);
      if (offset + size > object_size) {
	object_data = (uint8_t*)realloc(object_data, offset + size);
	memset(object_data + object_size, 0, offset + size - object_size);
	object_size = offset + size;
      }
      uint8_t pattern = 1;
      if (offset != 0) {
	pattern = cycling_pattern;
	cycling_pattern++;
	if (cycling_pattern == 0)
	  cycling_pattern = 2;
      }
      memset(object_data + offset, pattern, size);
      rados_write_op_write(write_op, (char*)object_data + offset, size, offset);

    } else if (strcmp(cmd, "_truncate") == 0) {

      assert(write_op);
      if (offset > object_size) {
	object_data = (uint8_t*)realloc(object_data, offset);
	object_size = offset;
      }
      rados_write_op_truncate(write_op, offset);
      
    } else {
      printf("unhandled command <%s>\n", cmd);
      break;
    }    
  }
  //730202020
  //612222222
  if (write_op)
    rados_release_write_op(write_op);
  rados_ioctx_destroy(io_ctx);
  rados_shutdown(cluster);
  return 0;
}
