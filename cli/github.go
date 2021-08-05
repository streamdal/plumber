package cli

import "gopkg.in/alecthomas/kingpin.v2"

func HandleGithubFlags(cmd *kingpin.CmdClause, _ *Options) {
	cmd.Command("login", "Authorize plumber to access your GitHub Repositories")
}
