package cmd

import (
	"context"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/protocol/journalspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

var journalCmd = &cobra.Command{
	Use:   "journal",
	Short: "Commands for working with gazette journals",
}

var journalGetCmd = &cobra.Command{
	Use:   "get [journal-name-prefix]",
	Short: "Get specifications of one or more journals as a YAML journal tree",
	Long: `Get fetches current specifications for one or journals, using the
given journal selector. JournalSpecs are arranged into a journal tree
specification and written to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		// Load the complete KeySpace at the |clusterRoot|.
		// TODO(johnny): Parse and accept a pb.LabelSelector, apply it within the
		// KeySpace to filter desired journals.
		var ks = pb.NewKeySpace(clusterRoot)

		if err := ks.Load(context.Background(), etcd3Client(), 0); err != nil {
			log.WithField("err", err).Fatal("failed to load KeySpace")
		}

		// Initialize Nodes for each journal. Extract a journal specification tree,
		// and hoist common configuration to parent nodes to minimize config DRY.
		var nodes []journalspace.Node
		for _, kv := range ks.Prefixed(v3_allocator.ItemKey(ks, args[0])) {
			nodes = append(nodes, journalspace.Node{
				JournalSpec: *kv.Decoded.(v3_allocator.Item).ItemValue.(*pb.JournalSpec),
				Revision:    kv.Raw.ModRevision,
			})
		}

		var tree = journalspace.ExtractTree(nodes)
		tree.HoistSpecs()

		// Render journal specification tree.
		if b, err := yaml.Marshal(tree); err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("failed to encode resource")
		} else {
			os.Stdout.Write(b)
		}
	},
}

func init() {
	rootCmd.AddCommand(journalCmd)
	journalCmd.AddCommand(journalGetCmd)

	journalCmd.Flags().StringVarP(&clusterRoot, "clusterRoot", "r",
		"/gazette/cluster", "Etcd prefix of gazette cluster")
}

var clusterRoot string
