// Package shard provides tools for working with consumer shards and shard databases.
package shard

import "github.com/spf13/cobra"

var Cmd = &cobra.Command{
	Use:   "shard",
	Short: "Commands for working with a gazette consumer shard",
}
