#include "bridge.h"
#include "hnswlib/hnswlib.h"
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <exception>
#include "journal.h"
struct Index {
 hnswlib::L2Space space;
 std::unique_ptr<hnswlib::HierarchicalNSW<float>> graph;
 std::shared_timed_mutex mu;
 std::unique_ptr<journal::Capture> capture;
 Index(int d,int m,int ef):space(d),graph(new hnswlib::HierarchicalNSW<float>(&space,256,m,ef,42)){}
};
static char* fail(const std::exception& e){return strdup(e.what());}
extern "C" {
char* ann_vector(void*p,uint64_t label,float*out){try{auto&x=*static_cast<Index*>(p);std::shared_lock<std::shared_timed_mutex>lock(x.mu);auto found=x.graph->label_lookup_.find(label);if(found==x.graph->label_lookup_.end())throw std::runtime_error("missing patch vector label");memcpy(out,x.graph->getDataByInternalId(found->second),x.graph->data_size_);return nullptr;}catch(const std::exception&e){return fail(e);}}
void* ann_new(int d,int m,int ef,char** error){try{return new Index(d,m,ef);}catch(const std::exception& e){*error=fail(e);return nullptr;}}
void ann_free(void* p){delete static_cast<Index*>(p);}
char* ann_add(void* p,uint64_t id,const float* v){try{auto& x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex> lock(x.mu);if(x.capture && x.graph->label_lookup_.count(id))throw std::runtime_error("journal requires new labels");if(x.graph->cur_element_count==x.graph->max_elements_)x.graph->resizeIndex(x.graph->max_elements_*2);x.graph->addPoint(v,id);return nullptr;}catch(const std::exception& e){return fail(e);}}
char* ann_search(void* p,const float* v,int k,int ef,uint64_t* ids,int* count,uint64_t* computations){try{auto& x=*static_cast<Index*>(p);std::shared_lock<std::shared_timed_mutex> lock(x.mu);size_t work=0;auto hits=x.graph->searchKnnQuery(v,k,nullptr,std::max(k,ef),&work);*count=hits.size();int j=0;while(!hits.empty()){ids[j++]=hits.top().second;hits.pop();}*computations=work;return nullptr;}catch(const std::exception& e){return fail(e);}}
char* ann_save(void* p,const char* path){try{auto& x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex> lock(x.mu);x.graph->saveIndex(path);return nullptr;}catch(const std::exception& e){return fail(e);}}
char* ann_load(void* p,const char* path){try{auto& x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex> lock(x.mu);x.graph.reset(new hnswlib::HierarchicalNSW<float>(&x.space,path));return nullptr;}catch(const std::exception& e){return fail(e);}}
}

extern "C" {
char* ann_patch_begin(void*p){try{auto&x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex> lock(x.mu);if(x.capture)throw std::runtime_error("patch already active");x.capture.reset(new journal::Capture{journal::meta(*x.graph),{}});x.graph->journal_before_change=[&x](uint32_t id){if(id<x.capture->before.count && !x.capture->old.count(id))x.capture->old.emplace(id,journal::image(*x.graph,id));};return nullptr;}catch(const std::exception&e){return fail(e);}}
char* ann_patch_finish(void*p,char**out,uint64_t*size){try{auto&x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex>lock(x.mu);if(!x.capture)throw std::runtime_error("patch not active");auto patch=journal::finish(*x.graph,*x.capture);auto bytes=journal::encode(patch);journal::apply(*x.graph,patch,true);x.graph->journal_before_change=nullptr;x.capture.reset();*size=bytes.size();*out=(char*)malloc(*size);if(!*out)throw std::bad_alloc();memcpy(*out,bytes.data(),*size);return nullptr;}catch(const std::exception&e){return fail(e);}}
char* ann_patch_apply(void*p,const char*data,uint64_t size,int reverse){try{auto&x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex>lock(x.mu);if(x.capture)throw std::runtime_error("active patch capture");auto patch=journal::decode(data,size);journal::apply(*x.graph,patch,reverse);return nullptr;}catch(const std::exception&e){return fail(e);}}
char* ann_meta_save(void*p,char**out,uint64_t*size){try{auto&x=*static_cast<Index*>(p);std::shared_lock<std::shared_timed_mutex>lock(x.mu);std::string bytes;journal::putmeta(bytes,journal::meta(*x.graph));*size=bytes.size();*out=(char*)malloc(*size);if(!*out)throw std::bad_alloc();memcpy(*out,bytes.data(),*size);return nullptr;}catch(const std::exception&e){return fail(e);}}
char* ann_meta_load(void*p,const char*data,uint64_t size){try{auto&x=*static_cast<Index*>(p);std::unique_lock<std::shared_timed_mutex>lock(x.mu);journal::Reader r{data,(size_t)size};auto wanted=journal::getmeta(r);journal::validateMeta(*x.graph,wanted);auto actual=journal::meta(*x.graph);actual.random=wanted.random;if(r.left||!journal::equal(actual,wanted))throw std::runtime_error("snapshot metadata mismatch");std::istringstream in(wanted.random);in>>x.graph->level_generator_>>x.graph->update_probability_generator_;return nullptr;}catch(const std::exception&e){return fail(e);}}
}
