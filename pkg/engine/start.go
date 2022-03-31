package engine

import (
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start harpooon engine",
	Long:  `Start harpoon engine`,
	Run: func(cmd *cobra.Command, args []string) {
		harpoonConfig.InitConfig(true)
		harpoonConfig.GetTargets(true)
		harpoonConfig.RunTargets()
	},
}

func init() {
	harpoonCmd.AddCommand(startCmd)
	cmdGroup := []*cobra.Command{
		harpoonCmd,
		startCmd,
	}
	harpoonConfig = NewHarpoonConfig()
	for _, cmd := range cmdGroup {
		harpoonConfig.bindFlags(cmd)
	}
}
