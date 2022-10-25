package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/rkspx/elastic-syncer/syncer"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "copy documents from one elasticsearch server to another",
	Run:   sync,
}

func init() {
	syncCmd.Flags().Duration("since", syncer.DefaultSince, "copy all documents that dated since the specified duration, only works if the document has 'timesetamp' field with 'date' field type, default: 30d")
	syncCmd.Flags().Int("limit", syncer.DefaultLimit, "limit number of synced document, set to 0 to disable, default: 0")
	syncCmd.Flags().String("index", syncer.DefaultIndex, "index name")
	syncCmd.Flags().String("from-address", "", "source elasticsearch address")
	syncCmd.Flags().String("from-username", "", "source elasticsearch username, if using basic authentication")
	syncCmd.Flags().String("from-password", "", "source elasticsearch password, if using basic authentication")
	syncCmd.Flags().Bool("log-from-requests", false, "log source elasticsearch requests")
	syncCmd.Flags().Bool("log-from-responses", false, "log source elasticsearch requests")
	syncCmd.Flags().String("to-address", "", "destination elasticsearch address")
	syncCmd.Flags().String("to-username", "", "destination elasticsearch username, if using basic authentication")
	syncCmd.Flags().String("to-password", "", "destination elasticsearch password, if using basic authentication")
	syncCmd.Flags().Bool("log-to-requests", false, "log destination elasticsearch requests")
	syncCmd.Flags().Bool("log-to-responses", false, "log destination elasticsearch requests")

	rootCmd.AddCommand(syncCmd)
}

func sync(cmd *cobra.Command, args []string) {
	since, err := cmd.Flags().GetDuration("since")
	if err != nil {
		log.Fatalf("can not get 'since' value, %v", err)
	}

	limit, err := cmd.Flags().GetInt("limit")
	if err != nil {
		log.Fatalf("can not get 'limit' value, %v", err)
	}

	index, err := cmd.Flags().GetString("index")
	if err != nil {
		log.Fatalf("can not get 'index' value, %v", err)
	}

	fromAddress, err := cmd.Flags().GetString("from-address")
	if err != nil {
		log.Fatalf("can not get 'from-address' value, %v", err)
	}

	fromUsername, err := cmd.Flags().GetString("from-username")
	if err != nil {
		log.Fatalf("can not get 'from-username' value, %v", err)
	}

	fromPassword, err := cmd.Flags().GetString("from-password")
	if err != nil {
		log.Fatalf("can not get 'from-password' value, %v", err)
	}

	logFromRequests, err := cmd.Flags().GetBool("log-from-requests")
	if err != nil {
		log.Fatalf("can not get 'log-from-requests' value, %v", err)
	}

	logFromResponses, err := cmd.Flags().GetBool("log-from-responses")
	if err != nil {
		log.Fatalf("can not get 'log-from-responses' value, %v", err)
	}

	toAddress, err := cmd.Flags().GetString("to-address")
	if err != nil {
		log.Fatalf("can not get 'to-address' value, %v", err)
	}

	toUsername, err := cmd.Flags().GetString("to-username")
	if err != nil {
		log.Fatalf("can not get 'to-username' value, %v", err)
	}

	toPassword, err := cmd.Flags().GetString("to-password")
	if err != nil {
		log.Fatalf("can not get 'to-password' value, %v", err)
	}

	logToRequests, err := cmd.Flags().GetBool("log-to-requests")
	if err != nil {
		log.Fatalf("can not get 'log-to-requests' value, %v", err)
	}

	logToResponses, err := cmd.Flags().GetBool("log-to-responses")
	if err != nil {
		log.Fatalf("can not get 'log-to-responses' value, %v", err)
	}

	cl, err := syncer.New(syncer.Config{
		Since:            since,
		Limit:            limit,
		Index:            index,
		FromHost:         fromAddress,
		FromUsername:     fromUsername,
		FromPassword:     fromPassword,
		LogFromRequests:  logFromRequests,
		LogFromResponses: logFromResponses,
		ToHost:           toAddress,
		ToUsername:       toUsername,
		ToPassword:       toPassword,
		LogToRequests:    logToRequests,
		LogToResponses:   logToResponses,
	})

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := cl.Sync(ctx); err != nil {
		log.Fatalf("sync failed, %s", err.Error())
	}
}
