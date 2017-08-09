package treeserve

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
