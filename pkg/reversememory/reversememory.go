package reversememory

type ReverseMemoryProvider struct {
	userRepos map[int]*map[int]struct{}
	repoUsers map[int]*map[int]struct{}
}

func (p *ReverseMemoryProvider) SetUserRepos(uid int, repoIds []int) error {
	rids := map[int]struct{}{}
	for _, rid := range repoIds {
		rids[rid] = struct{}{}
	}
	p.userRepos[uid] = &rids
	for _, rid := range repoIds {
		ru, ok := p.repoUsers[rid]
		if !ok {
			p.repoUsers[rid] = &map[int]struct{}{uid: {}}
			continue
		}
		(*ru)[uid] = struct{}{}
	}
	return nil
}

func (p *ReverseMemoryProvider) GetUserRepos(uid int) ([]int, error) {
	res := []int{}
	if _, ok := (*p).userRepos[uid]; !ok {
		return nil, nil
	}
	for u := range *p.userRepos[uid] {
		res = append(res, u)
	}
	return res, nil
}

func (p *ReverseMemoryProvider) GetRepoUsers(rid int) ([]int, error) {
	uids := []int{}
	if _, ok := (*p).repoUsers[rid]; !ok {
		return nil, nil
	}
	for uid := range *p.repoUsers[rid] {
		uids = append(uids, uid)
	}
	return uids, nil
}

func (p *ReverseMemoryProvider) CheckAccess(uid int, rid int) (bool, error) {
	if ur, ok := p.userRepos[uid]; ok {
		if _, rok := (*ur)[rid]; rok {
			return true, nil
		}
	}
	return false, nil
}

func NewProvider() *ReverseMemoryProvider {
	return &ReverseMemoryProvider{userRepos: map[int]*map[int]struct{}{}, repoUsers: map[int]*map[int]struct{}{}}
}
