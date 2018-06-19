package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/LiveRamp/gazette/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		var fin = os.Stdin

		if applyFile != "" {
			var err error
			if fin, err = os.Open(applyFile); err != nil {
				log.WithFields(log.Fields{"file": applyFile, "err": err}).Fatal("failed to open")
			}
		}

		var decoder = yaml.NewDecoder(fin)
		decoder.SetStrict(true)

		for {
			var obj struct {
				Revision int64
				Version  int64

				Journal *protocol.JournalSpec
			}
			var err error

			if err = decoder.Decode(obj); err == io.EOF {
				break
			}

			switch {
			case obj.Journal != nil:
				err = obj.Journal.Validate()
			default:
				err = fmt.Errorf("no resource provided")
			}

			if err != nil {
				log.WithFields(log.Fields{"err": err}).Fatal("failed to decode resource")
			}
			log.WithField("obj", obj).Info("decoded object")
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	applyCmd.Flags().StringVarP(&applyFile, "file", "-f", "", "File to apply (instead of stdin).")
}

var applyFile string
