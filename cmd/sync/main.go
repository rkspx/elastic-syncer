package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var buildtime, version string

var rootCmd = &cobra.Command{
	Short:   "elasticsearch sync utility",
	Version: fmt.Sprintf("ver %s, build-time %s", version, buildtime),
}

func init() {

}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
