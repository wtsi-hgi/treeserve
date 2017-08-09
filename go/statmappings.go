package treeserve

// StatMappings maps keys to sets of group, user and tag
// All such sets have the same aggregate values for one key
// Eg a file can have the same costs saved under *.*.file, *.*.compressed, GID.UID.compressed etc
// But since they roll up differently .... check if OK
type StatMappings struct {
	m map[Md5Key]*StatMapping
}

func NewStatMappings() *StatMappings {
	keyMap := make(map[Md5Key]*StatMapping)
	return &StatMappings{m: keyMap}
}

func (statMappings *StatMappings) Add(smKey Md5Key, sm *StatMapping) {
	statMappings.m[smKey] = sm
}

func (statMappings *StatMappings) Get(k Md5Key) (statMapping *StatMapping, ok bool) {
	statMapping, ok = statMappings.m[k]
	return
}

func (statMappings *StatMappings) Keys() []Md5Key {
	var s []Md5Key
	for v := range statMappings.m {
		s = append(s, v)
	}
	return s
}

func (statMappings *StatMappings) Values() (s []*StatMapping) {

	for _, v := range statMappings.m {
		s = append(s, v)
	}
	return s
}

func (statMappings *StatMappings) Contains(s *StatMapping) (ok bool) {

	ok = false
	for _, v := range statMappings.m {
		if s.Group == v.Group && s.User == v.User && s.Tag == v.Tag {
			ok = true
			return
		}
	}
	return

}
