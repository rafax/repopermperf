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
		log.Printf("Will simulate %d users, %d repos, repos/user: %d\n", users, repos, reposPerUser)
		rand.Seed(c.Int64("seed"))
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
		p := providers[c.String("backend")]()
		log.Printf("Starting write phase")
		start := time.Now()
		for u, ur := range userRepos {
			p.SetUserRepos(u, ur)
		}
		elapsed := time.Since(start)
		log.Println("Write phase took: " + elapsed.String())
		log.Printf("Starting per-user read phase")
		ustart := time.Now()
		for u, _ := range userRepos {
			_, err := p.GetUserRepos(u)
			if err != nil {
				log.Fatalf("error reading repos for user %d: %v", u, err)
			}
		}
		uelapsed := time.Since(ustart)
		log.Println("per-user read phase phase took: " + uelapsed.String())
		log.Printf("Starting per-repo read phase")
		rstart := time.Now()
		for i := 0; i < repos; i++ {
			_, err := p.GetRepoUsers(i)
			if err != nil {
				log.Fatalf("error reading repos for repo %d: %v", i, err)
			}
		}
		relapsed := time.Since(rstart)
		log.Println("per-repo read phase phase took: " + relapsed.String())
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type AuthzProvider interface {
	SetUserRepos(uid int, repoIds []int) error
	GetUserRepos(uid int) ([]int, error)
	GetRepoUsers(rid int) ([]int, error)
	CheckAccess(uid int, rid int) (bool, error)
}
