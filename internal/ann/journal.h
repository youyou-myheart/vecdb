#pragma once
// Native graph patches contain final node images, never insertion commands.
// Format is pinned to the same little-endian 64-bit ABI as graph snapshots.
#include <map>
#include <set>
#include <unordered_map>
#include <sstream>
#include <string>
#include <vector>
#include <limits>
namespace journal {
using Graph = hnswlib::HierarchicalNSW<float>;
struct Meta {
 uint64_t count,capacity,stride,upper;
 int32_t level;
 uint32_t entry;
 std::string random;
};
struct Image {int32_t level=-1;std::string base,upper;};
struct Change {uint32_t id;Image before,after;};
struct Patch {Meta before,after;std::vector<Change> changes;};
struct Capture {Meta before;std::map<uint32_t,Image> old;};
inline Meta meta(Graph& g){std::ostringstream out;out<<g.level_generator_<<' '<<g.update_probability_generator_;return {g.cur_element_count.load(),g.max_elements_,g.size_data_per_element_,g.size_links_per_element_,g.maxlevel_,g.enterpoint_node_,out.str()};}
inline Image image(Graph& g,uint32_t id){if(id>=g.cur_element_count)return {};int level=g.element_levels_[id];return {level,std::string(g.data_level0_memory_+id*g.size_data_per_element_,g.size_data_per_element_),level?std::string(g.linkLists_[id],level*g.size_links_per_element_):std::string()};}
inline bool equal(const Image&a,const Image&b){return a.level==b.level&&a.base==b.base&&a.upper==b.upper;}
inline bool equal(const Meta&a,const Meta&b){return a.count==b.count&&a.capacity==b.capacity&&a.stride==b.stride&&a.upper==b.upper&&a.level==b.level&&a.entry==b.entry&&a.random==b.random;}
template<class T> inline void put(std::string& out,T v){out.append(reinterpret_cast<const char*>(&v),sizeof(v));}
inline void putstr(std::string& out,const std::string& v){put<uint64_t>(out,v.size());out+=v;}
struct Reader {
 const char* p;size_t left;
 template<class T>T get(){if(left<sizeof(T))throw std::runtime_error("truncated graph patch");T v;memcpy(&v,p,sizeof(v));p+=sizeof(v);left-=sizeof(v);return v;}
 std::string str(){auto n=get<uint64_t>();if(n>left||n>(512u<<20))throw std::runtime_error("graph patch length");std::string s(p,n);p+=n;left-=n;return s;}
};
inline void putmeta(std::string& out,const Meta&m){put(out,m.count);put(out,m.capacity);put(out,m.stride);put(out,m.upper);put(out,m.level);put(out,m.entry);putstr(out,m.random);}
inline Meta getmeta(Reader&r){Meta m;m.count=r.get<uint64_t>();m.capacity=r.get<uint64_t>();m.stride=r.get<uint64_t>();m.upper=r.get<uint64_t>();m.level=r.get<int32_t>();m.entry=r.get<uint32_t>();m.random=r.str();return m;}
inline void putimage(std::string&out,const Image&i){put(out,i.level);putstr(out,i.base);putstr(out,i.upper);}
inline Image getimage(Reader&r){Image i;i.level=r.get<int32_t>();i.base=r.str();i.upper=r.str();return i;}
inline std::string encode(const Patch&p){std::string out;put<uint32_t>(out,0x47504431);putmeta(out,p.before);putmeta(out,p.after);put<uint64_t>(out,p.changes.size());for(auto&c:p.changes){put(out,c.id);putimage(out,c.before);putimage(out,c.after);}return out;}
inline Patch decode(const char*b,size_t n){Reader r{b,n};if(r.get<uint32_t>()!=0x47504431)throw std::runtime_error("graph patch format");Patch p;p.before=getmeta(r);p.after=getmeta(r);auto count=r.get<uint64_t>();if(count>r.left/44)throw std::runtime_error("graph patch count");for(uint64_t i=0;i<count;i++){Change c;c.id=r.get<uint32_t>();c.before=getimage(r);c.after=getimage(r);p.changes.push_back(std::move(c));}if(r.left)throw std::runtime_error("graph patch trailing bytes");return p;}
// libstdc++ engine extraction disables skipws; consume the separator explicitly.
inline void validateMeta(Graph&g,const Meta&m){if(m.stride!=g.size_data_per_element_||m.upper!=g.size_links_per_element_||m.capacity<m.count||m.capacity> (512u<<20)/m.stride||m.count>UINT32_MAX||m.level>64||m.level< -1||m.random.size()>4096)throw std::runtime_error("graph patch metadata");if((m.count==0&&(m.level!=-1||m.entry!=UINT32_MAX))||(m.count>0&&(m.level<0||m.entry>=m.count)))throw std::runtime_error("graph patch entry");std::istringstream in(m.random);std::default_random_engine a,b;if(!(in>>a>>std::ws>>b))throw std::runtime_error("graph patch random state");}
inline void apply(Graph&g,const Patch&p,bool reverse){
 const Meta&from=reverse?p.after:p.before;const Meta&to=reverse?p.before:p.after;
 validateMeta(g,from);validateMeta(g,to);
 if(!equal(meta(g),from))throw std::runtime_error("graph patch base mismatch");
 std::unordered_map<uint32_t,const Image*> targets;
 targets.reserve(p.changes.size());
 for(auto&c:p.changes){const Image&src=reverse?c.after:c.before;const Image&dst=reverse?c.before:c.after;
  if(!targets.emplace(c.id,&dst).second||c.id>=std::max(from.count,to.count)||!equal(image(g,c.id),src))throw std::runtime_error("graph patch node mismatch");
  if(c.id>=to.count){if(dst.level!=-1||!dst.base.empty()||!dst.upper.empty())throw std::runtime_error("graph patch removed node");continue;}
  if(dst.level<0||dst.level>64||dst.base.size()!=to.stride||dst.upper.size()!=to.upper*dst.level)throw std::runtime_error("graph patch image size");
 }
 for(uint64_t id=from.count;id<to.count;id++)if(!targets.count(id))throw std::runtime_error("graph patch missing new node");
 auto levelOf=[&](uint32_t id){auto it=targets.find(id);return it==targets.end()?g.element_levels_[id]:it->second->level;};
 std::set<hnswlib::labeltype> labels;
 for(auto&item:targets){auto id=item.first;auto&dst=*item.second;if(id>=to.count)continue;
  hnswlib::labeltype label;memcpy(&label,dst.base.data()+g.label_offset_,sizeof(label));
  if(!labels.insert(label).second)throw std::runtime_error("graph patch duplicate label");
  if(id<from.count){if(label!=g.getExternalLabel(id))throw std::runtime_error("graph patch changed label");}else if(g.label_lookup_.count(label))throw std::runtime_error("graph patch duplicate label");
  for(int layer=0;layer<=dst.level;layer++){const char*data=layer?dst.upper.data()+(layer-1)*to.upper:dst.base.data();uint32_t count;memcpy(&count,data,4);if(count>(layer?g.maxM_:g.maxM0_))throw std::runtime_error("graph patch link count");for(uint32_t j=0;j<count;j++){uint32_t neighbor;memcpy(&neighbor,data+4+j*4,4);if(neighbor>=to.count||neighbor==id||levelOf(neighbor)<layer)throw std::runtime_error("graph patch link target");}}
 }
 if(to.count && levelOf(to.entry)!=to.level)throw std::runtime_error("graph patch entry level");
 // All structural validation precedes mutation. Allocation failure is fatal to
 // this in-memory replica; caller must discard it and restore durable state.
 if(to.capacity>g.max_elements_)g.resizeIndex(to.capacity);
 for(auto&item:targets){uint32_t id=item.first;const Image&dst=*item.second;
  if(id<from.count && g.element_levels_[id]>0){free(g.linkLists_[id]);g.linkLists_[id]=nullptr;}
  if(id>=to.count){g.label_lookup_.erase(g.getExternalLabel(id));g.element_levels_[id]=0;continue;}
  g.linkLists_[id]=nullptr;g.element_levels_[id]=dst.level;
  if(dst.level){g.linkLists_[id]=(char*)malloc(dst.upper.size());if(!g.linkLists_[id])throw std::bad_alloc();memcpy(g.linkLists_[id],dst.upper.data(),dst.upper.size());}
  memcpy(g.data_level0_memory_+id*to.stride,dst.base.data(),dst.base.size());
  g.label_lookup_[g.getExternalLabel(id)]=id;
 }
 g.cur_element_count=to.count;
 if(to.capacity<g.max_elements_)g.resizeIndex(to.capacity);
 g.maxlevel_=to.level;g.enterpoint_node_=to.entry;
 std::istringstream in(to.random);in>>g.level_generator_>>std::ws>>g.update_probability_generator_;
}
inline Patch finish(Graph& g, Capture& capture) {
 Patch p;
 p.before = capture.before;
 p.after = meta(g);
 p.changes.reserve(capture.old.size() + p.after.count - p.before.count);
 for (auto& entry : capture.old) {
  auto after = image(g, entry.first);
  if (!equal(entry.second, after))
   p.changes.push_back({entry.first, std::move(entry.second), std::move(after)});
 }
 for (uint32_t id = p.before.count; id < p.after.count; ++id)
  p.changes.push_back({id, Image{}, image(g, id)});
 return p;
}
}
