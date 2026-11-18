#pragma once
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
void* ann_new(int dim,int m,int ef,char** error);
void ann_free(void* p);
char* ann_add(void* p,uint64_t id,const float* vector);
char* ann_search(void* p,const float* vector,int k,int ef,uint64_t* ids,int* count,uint64_t* computations);
char* ann_save(void* p,const char* path);
char* ann_load(void* p,const char* path);
char* ann_patch_begin(void* p);
char* ann_patch_finish(void* p,char** out,uint64_t* size);
char* ann_patch_apply(void* p,const char* data,uint64_t size,int reverse);
char* ann_meta_save(void* p,char** out,uint64_t* size);
char* ann_meta_load(void* p,const char* data,uint64_t size);
char* ann_vector(void* p,uint64_t label,float* out);
#ifdef __cplusplus
}
#endif
