package slicememory

type SliceMemoryProvider struct {
	userRepos map[int][]int
}

func (p *SliceMemoryProvider) SetUserRepos(uid int, repoIds []int) error {
	rids := make([]int, len(repoIds))
	copy(rids, repoIds)
	p.userRepos[uid] = rids
	return nil
}

func (p *SliceMemoryProvider) GetUserRepos(uid int) ([]int, error) {
	return p.userRepos[uid], nil
}

func (p *SliceMemoryProvider) GetRepoUsers(rid int) ([]int, error) {
	uids := []int{}
	for uid, ur := range p.userRepos {
		for _, r := range ur {
			if r == rid {
				uids = append(uids, uid)
				break
			}
		}
	}
	return uids, nil
}

func (p *SliceMemoryProvider) CheckAccess(uid int, rid int) (bool, error) {
	if ur, ok := p.userRepos[uid]; ok {
		for _, r := range ur {
			if r == rid {
				return true, nil
			}
		}
	}
	return false, nil
}

func NewSliceMemoryProvider() *SliceMemoryProvider {
	return &SliceMemoryProvider{userRepos: map[int][]int{}}
}
