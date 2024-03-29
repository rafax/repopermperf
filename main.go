package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/rafax/repopermperf/pkg/mapmemory"
	"github.com/rafax/repopermperf/pkg/pg"
	"github.com/rafax/repopermperf/pkg/reversememory"
	"github.com/rafax/repopermperf/pkg/slicememory"
	"github.com/urfave/cli/v2"
)

var providers = map[string]func() AuthzProvider{
	"smem": func() AuthzProvider { return slicememory.NewProvider() },
	"mmem": func() AuthzProvider { return mapmemory.NewProvider() },
	"rmem": func() AuthzProvider { return reversememory.NewProvider() },
	"pg": func() AuthzProvider {
		p, err := pg.NewProvider(os.Getenv("RPPERF_PG"))
		if err != nil {
			log.Fatal("err initializing postgres provider", err)
		}
		return p
	},
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "backend",
			Usage:    "Backend to use - one of (pg,keto,opy,nmem,rmem)",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "users",
			Value: 10000,
		},
		&cli.IntFlag{
			Name:  "repos",
			Value: 10000,
		},
		&cli.IntFlag{
			Name:  "repos-per-user",
			Value: 5000,
		},
		&cli.Int64Flag{
			Name:  "seed",
			Value: 13,
		},
	}

	app.Action = func(c *cli.Context) error {
		users := c.Int("users")
		repos := c.Int("repos")
		reposPerUser := c.Int("repos-per-user")
		p := providers[c.String("backend")]()
		log.Printf("Will simulate %d users, %d repos, repos/user: %d, generating data...\n", users, repos, reposPerUser)
		gstart := time.Now()
		rand.Seed(c.Int64("seed"))
		userRepos := generateData(users, repos, reposPerUser)
		gelapsed := time.Since(gstart)
		log.Printf("Generated in %v", gelapsed)
		log.Printf("Starting write phase")
		start := time.Now()
		for u, ur := range userRepos {
			p.SetUserRepos(u, ur)
		}
		elapsed := time.Since(start)
		log.Printf("Write phase took: %s, %v per write", elapsed.String(), elapsed/time.Duration(len(userRepos)))
		log.Printf("Starting per-user read phase")
		ustart := time.Now()
		for u := range userRepos {
			_, err := p.GetUserRepos(u)
			if err != nil {
				log.Fatalf("error reading repos for user %d: %v", u, err)
			}
		}
		uelapsed := time.Since(ustart)
		log.Printf("per-user read phase phase took: %s, %v per user", uelapsed.String(), uelapsed/time.Duration(users))
		log.Printf("Starting per-repo read phase")
		rstart := time.Now()
		for i := 0; i < repos; i++ {
			_, err := p.GetRepoUsers(i)
			if err != nil {
				log.Fatalf("error reading repos for repo %d: %v", i, err)
			}
		}
		relapsed := time.Since(rstart)
		log.Printf("per-repo read phase phase took: %s, %v per repo ", relapsed.String(), relapsed/time.Duration(repos))
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func generateData(users, repos, reposPerUser int) [][]int {
	userRepos := [][]int{}
	a := make([]int, repos)
	for i := range a {
		a[i] = i
	}
	for i := 0; i < users; i++ {
		nrepos := rand.Intn(reposPerUser) + reposPerUser/2
		rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
		userRepos = append(userRepos, a[:nrepos])
	}
	return userRepos
}

type AuthzProvider interface {
	SetUserRepos(uid int, repoIds []int) error
	GetUserRepos(uid int) ([]int, error)
	GetRepoUsers(rid int) ([]int, error)
	CheckAccess(uid int, rid int) (bool, error)
}
