package mapmemory

// MapMemoryProvider is a naive implementation of AuthzProvider using in-memory map-of-maps
type MapMemoryProvider struct {
	userRepos map[int]*map[int]struct{}
}

func (p *MapMemoryProvider) SetUserRepos(uid int, repoIds []int) error {
	rids := map[int]struct{}{}
	for _, rid := range repoIds {
		rids[rid] = struct{}{}
	}
	p.userRepos[uid] = &rids
	return nil
}

func (p *MapMemoryProvider) GetUserRepos(uid int) ([]int, error) {
	res := []int{}
	for u := range *p.userRepos[uid] {
		res = append(res, u)
	}
	return res, nil
}

func (p *MapMemoryProvider) GetRepoUsers(rid int) ([]int, error) {
	uids := []int{}
	for uid, ur := range p.userRepos {
		if _, ok := (*ur)[rid]; ok {
			uids = append(uids, uid)
		}
	}
	return uids, nil
}

func (p *MapMemoryProvider) CheckAccess(uid int, rid int) (bool, error) {
	if ur, ok := p.userRepos[uid]; ok {
		if _, rok := (*ur)[rid]; rok {
			return true, nil
		}
	}
	return false, nil
}

func NewProvider() *MapMemoryProvider {
	return &MapMemoryProvider{userRepos: map[int]*map[int]struct{}{}}
}
