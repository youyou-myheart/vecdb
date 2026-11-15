#include "bridge.h"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <thread>
#include <vector>
int main() {
 char* error=nullptr;
 void* graph=ann_new(2,16,200,&error); assert(graph && !error);
 for(int j=0;j<1000;++j){ float v[2]={float(j),float(j)}; error=ann_add(graph,j+1,v); assert(!error); }
 std::atomic<bool> failed{false}; std::vector<std::thread> threads;
 for(int w=0;w<8;++w) threads.emplace_back([&,w](){
  float q[2]={0,0}; for(int j=0;j<1000;++j){uint64_t id=0,work=0;int count=0;char* e=ann_search(graph,q,1,32+w*16,&id,&count,&work); if(e || count!=1 || id!=1 || work==0) failed=true;free(e);}
 });
 threads.emplace_back([&](){for(int j=1000;j<1200;++j){float v[2]={float(j),float(j)};char* e=ann_add(graph,j+1,v);if(e)failed=true;free(e);}});
 for(auto& thread:threads)thread.join(); ann_free(graph); assert(!failed);
}
