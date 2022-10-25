package main

import (
	"log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Short: "elasticsearch sync utility",
}

func init() {

}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
