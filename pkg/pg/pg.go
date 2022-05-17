package pg

import (
	"context"
	"log"

	"github.com/jackc/pgx/v4"
)

// PgProvider is an AuthzProvider implementation backed by a Postgres DB
type PgProvider struct {
	db *pgx.Conn
}

func (p *PgProvider) SetUserRepos(uid int, repoIds []int) error {
	insert := [][]interface{}{}
	for _, r := range repoIds {
		insert = append(insert, []interface{}{uid, r})
	}
	_, err := p.db.CopyFrom(context.Background(), pgx.Identifier{"users_repos"}, []string{"user_id", "repo_id"}, pgx.CopyFromRows(insert))
	return err
}

func (p *PgProvider) GetUserRepos(uid int) ([]int, error) {
	r, err := p.db.Query(context.Background(), "SELECT repo_id FROM users_repos WHERE user_id = $1", uid)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	res := []int{}
	for r.Next() {
		var n int
		err = r.Scan(&n)
		if err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (p *PgProvider) GetRepoUsers(rid int) ([]int, error) {
	u, err := p.db.Query(context.Background(), "SELECT user_id FROM users_repos WHERE repo_id = $1", rid)
	if err != nil {
		return nil, err
	}
	defer u.Close()
	res := []int{}
	for u.Next() {
		var n int
		err = u.Scan(&n)
		if err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (p *PgProvider) CheckAccess(uid int, rid int) (bool, error) {
	var exists bool
	err := p.db.QueryRow(context.Background(), "SELECT EXISTS(SELECT user_id FROM users_repos WHERE repo_id = $1 AND user_id = $2)", rid, uid).Scan(&exists)
	return exists, err
}

func NewProvider(conn string) (*PgProvider, error) {
	db, err := pgx.Connect(context.Background(), conn)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	_, err = db.Exec(context.Background(), schema)
	if err != nil {
		return nil, err
	}
	return &PgProvider{db: db}, nil
}

var schema = `
DROP TABLE IF EXISTS users_repos;

CREATE TABLE users_repos (user_id integer NOT NULL, repo_id integer NOT NULL, CONSTRAINT user_repo_unique UNIQUE(user_id, repo_id));

CREATE INDEX users_repos_repo_id_idx ON users_repos (repo_id,user_id );
`
