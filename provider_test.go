package main

import (
	"log"
	"os"
	"sort"
	"testing"

	"github.com/rafax/repopermperf/pkg/mapmemory"
	"github.com/rafax/repopermperf/pkg/pg"
	"github.com/rafax/repopermperf/pkg/reversememory"
	"github.com/rafax/repopermperf/pkg/slicememory"
)

func testProvider(t *testing.T, p AuthzProvider) {
	t.Run("empty repos for user", func(t *testing.T) {
		p.SetUserRepos(1, nil)
		r, err := p.GetUserRepos(1)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(r) != 0 {
			t.Error("expected err to be empty")
		}
		ok, err := p.CheckAccess(1, 1)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if ok != false {
			t.Error("expected ok to be false")
		}
	})
	t.Run("one repo for user", func(t *testing.T) {
		p.SetUserRepos(2, []int{2})
		r, err := p.GetUserRepos(2)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(r) != 1 && r[0] != 2 {
			t.Errorf("expected r to be [2], got: %v", r)
		}
		ru, err := p.GetRepoUsers(2)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(ru) != 1 && ru[0] != 2 {
			t.Errorf("expected ru to be [2], got: %v", ru)
		}
		ok, err := p.CheckAccess(2, 2)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(2, 3)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if ok {
			t.Errorf("expected ok to be false")
		}
	})
	t.Run("two repos for user", func(t *testing.T) {
		p.SetUserRepos(3, []int{3, 33})
		r, err := p.GetUserRepos(3)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		sort.Ints(r)
		if len(r) != 1 && r[0] != 3 && r[1] != 33 {
			t.Errorf("expected r to be [3,33], got: %v", r)
		}
		ru, err := p.GetRepoUsers(3)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(ru) != 1 && ru[0] != 2 {
			t.Errorf("expected ru to be [3], got: %v", ru)
		}
		ru, err = p.GetRepoUsers(33)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(ru) != 1 && ru[0] != 2 {
			t.Errorf("expected ru to be [3], got: %v", ru)
		}
		ok, err := p.CheckAccess(3, 3)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(3, 33)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(3, 44)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if ok {
			t.Errorf("expected ok to be false")
		}
	})
	t.Run("two repos for two users with intersection", func(t *testing.T) {
		p.SetUserRepos(4, []int{4, 44})
		p.SetUserRepos(5, []int{5, 44})
		r, err := p.GetUserRepos(4)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		sort.Ints(r)
		if len(r) != 1 && r[0] != 4 && r[1] != 44 {
			t.Errorf("expected r to be [4,44], got: %v", r)
		}
		r2, err := p.GetUserRepos(5)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		sort.Ints(r2)
		if len(r2) != 1 && r[0] != 5 && r[1] != 44 {
			t.Errorf("expected r to be [5,44], got: %v", r)
		}
		ru, err := p.GetRepoUsers(4)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(ru) != 1 && ru[0] != 2 {
			t.Errorf("expected ru to be [4], got: %v", ru)
		}
		ru44, err := p.GetRepoUsers(44)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		sort.Ints(ru44)
		if len(ru44) != 2 && ru[0] != 4 && ru44[1] != 5 {
			t.Errorf("expected ru to be [4,5], got: %v", ru)
		}
		ru5, err := p.GetRepoUsers(5)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if len(ru5) != 1 && ru[0] != 5 {
			t.Errorf("expected ru to be [5], got: %v", ru)
		}
		ok, err := p.CheckAccess(4, 4)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(4, 44)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(5, 44)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(5, 5)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if !ok {
			t.Errorf("expected ok to be true")
		}
		ok, err = p.CheckAccess(4, 5)
		if err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
		if ok {
			t.Errorf("expected ok to be false")
		}
	})
}

func Test_Providers(t *testing.T) {

	conn, ok := os.LookupEnv("RPPERF_PG")
	if !ok {
		log.Fatalf("Cannot initialize pg provider - put Postgres connection string in RPPERF_PG ")
	}
	pg, err := pg.NewProvider(conn)
	if err != nil {
		t.Fatalf("Could not initialize pg: %v", err)
	}
	for name, p := range map[string]AuthzProvider{
		"smem": slicememory.NewProvider(),
		"rmem": reversememory.NewProvider(),
		"mmem": mapmemory.NewProvider(),
		"pg":   pg,
	} {
		t.Run(name, func(t *testing.T) { testProvider(t, p) })
	}
}
