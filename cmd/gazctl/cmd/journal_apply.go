package cmd

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/LiveRamp/gazette/pkg/protocol/journalspace"
)

var journalApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply a YAML journal tree configuration to Etcd",
	Long: `Apply reads a YAML journal tree specification from stdin (or file,
if '-s' flag is used). The journal tree is validated and flattened to concrete
JournalSpecs. An all-or-nothing transaction is applied to Etcd, which verifies
the current ModRevision of each JournalSpec matches the provided, configured
Revision. In other words, if the input YAML is modified from that produced by a
prior "gazctl journal get", and one or more specs changed in Etcd since that
invocation, then "apply"" will fail and the user should try again after
performing another "gazctl journal get".`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		var buffer []byte

		if applyFile != "" {
			buffer, err = ioutil.ReadFile(applyFile)
		} else {
			buffer, err = ioutil.ReadAll(os.Stdin)
		}
		if err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("failed to read YAML input")
		}

		// Decode journal specification tree from YAML.
		var tree journalspace.Node
		if err = yaml.UnmarshalStrict(buffer, &tree); err == nil {
			err = tree.Validate()
		}
		if err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("failed to decode journal tree")
		}

		// Flatten into concrete JournalSpecs, and validate each.
		var nodes = tree.Flatten()

		for _, node := range nodes {
			if node.Delete {
				// We don't require that specs marked for deletion Validate successfully.
			} else if err = node.JournalSpec.Validate(); err != nil {
				log.WithFields(log.Fields{"err": err, "journal": node.Name}).Fatal("journal validation error")
			}
		}

		// Build a transaction which verifies the current ModRevision of each journal,
		// and if matched, updates to the provided spec.
		var ks = protocol.NewKeySpace(clusterRoot)
		var ifs []clientv3.Cmp
		var thens []clientv3.Op

		for _, node := range nodes {
			var key = v3_allocator.ItemKey(ks, node.JournalSpec.Name.String())

			ifs = append(ifs, clientv3.Compare(
				clientv3.ModRevision(key), "=", node.Revision))

			if node.Delete {
				thens = append(thens, clientv3.OpDelete(key))
				log.WithFields(log.Fields{"key": key, "rev": node.Revision}).Warn("deleting journal")
			} else {
				thens = append(thens, clientv3.OpPut(key, node.JournalSpec.MarshalString()))
				log.WithFields(log.Fields{"key": key, "rev": node.Revision}).Warn("updating journal")
			}
		}

		if !applyDryRun {
			resp, err := etcd3Client().Txn(context.Background()).
				If(ifs...).Then(thens...).Commit()
			if err != nil {
				log.WithFields(log.Fields{"err": err}).Fatal("failed to apply transaction")
			} else if resp.Succeeded == false {
				log.WithField("rev", resp.Header.Revision).Fatal("transaction compares failed")
			} else {
				log.WithField("rev", resp.Header.Revision).Info("successfully applied")
			}
		}
	},
}

func init() {
	journalCmd.AddCommand(journalApplyCmd)

	journalApplyCmd.Flags().StringVarP(&applyFile, "spec", "s",
		"", "Spec file to apply (instead of stdin).")

	journalApplyCmd.Flags().BoolVarP(&applyDryRun, "dry-run", "d",
		false, "Spec file to apply (instead of stdin).")
}

var (
	// Input file path of journalspace.Node YAML.
	applyFile string
	// Whether apply should simply print flattened JournalSpecs rather than applying them to Etcd.
	applyDryRun bool
)
