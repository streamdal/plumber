package plumber

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/github"
)

func (p *Plumber) githubLogin() error {
	gh, _ := github.New()
	cfg, err := gh.GetUserCode()
	if err != nil {
		return err
	}
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Please visit %s\nEnter in the code '%s' to allow Plumber access to your repositories\n",
		cfg.VerificationURL, cfg.UserCode)
	fmt.Println(strings.Repeat("-", 80))

	bearerToken, err := gh.PollForAccessToken(cfg)
	if err != nil {
		return err
	}

	p.PersistentConfig.GitHubToken = bearerToken
	p.PersistentConfig.Save()

	p.log.Info("GitHub access authorized. Plumber can now push/pull schemas from your repos!")

	return nil
}

func (p *Plumber) HandleGithubCmd() error {
	switch {
	case p.Cmd == "github login":
		return p.githubLogin()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}
