package vector

// postings contains only current live record IDs. Caller owns the index lock.
type payloadIndex map[string]map[string]map[string]struct{}

func (p payloadIndex) add(r Record) {
	if r.Deleted {
		return
	}
	for field, value := range r.Payload {
		if p[field] == nil {
			p[field] = map[string]map[string]struct{}{}
		}
		if p[field][value] == nil {
			p[field][value] = map[string]struct{}{}
		}
		p[field][value][r.ID] = struct{}{}
	}
}
func (p payloadIndex) remove(r Record) {
	for field, value := range r.Payload {
		delete(p[field][value], r.ID)
		if len(p[field][value]) == 0 {
			delete(p[field], value)
		}
		if len(p[field]) == 0 {
			delete(p, field)
		}
	}
}

// Start with the smallest posting, then intersect. Missing field != empty value.
func (p payloadIndex) selectIDs(filter map[string]string) map[string]struct{} {
	var smallest map[string]struct{}
	for field, value := range filter {
		set := p[field][value]
		if len(set) == 0 {
			return map[string]struct{}{}
		}
		if smallest == nil || len(set) < len(smallest) {
			smallest = set
		}
	}
	if smallest == nil {
		return nil
	}
	out := make(map[string]struct{}, len(smallest))
	for id := range smallest {
		match := true
		for field, value := range filter {
			if _, ok := p[field][value][id]; !ok {
				match = false
				break
			}
		}
		if match {
			out[id] = struct{}{}
		}
	}
	return out
}
