package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containers/podman/v4/pkg/bindings"
	"github.com/go-co-op/gocron"
	"github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/redhat-et/harpoon/pkg/engine/api"
	"github.com/redhat-et/harpoon/pkg/engine/utils"

	"k8s.io/klog/v2"
)

const (
	harpoonService = "harpoon"
	defaultVolume  = "harpoon-volume"
	harpoonImage   = "quay.io/harpoon/harpoon:latest"
	// TODO change back
	systemdImage = "quay.io/sallyom/harpoon:systemd"

	configMethod       = "config"
	rawMethod          = "raw"
	systemdMethod      = "systemd"
	kubeMethod         = "kube"
	fileTransferMethod = "filetransfer"
	ansibleMethod      = "ansible"
	deleteFile         = "delete"
	systemdPathRoot    = "/etc/systemd/system"
)

var (
	DefaultConfigPath     = filepath.Join("/opt", "mount", "config.yaml")
	DefaultConfigNew      = filepath.Join("/opt", "mount", "config-new.yaml")
	DefaultConfigBackup   = filepath.Join("/opt", "mount", "config-backup.yaml")
	DefaultHostConfigPath = filepath.Join(os.Getenv("HOME"), ".harpoon", "config.yaml")
)

type HarpoonConfig struct {
	Targets        []*api.Target
	PAT            string
	Conn           context.Context
	Volume         string
	Scheduler      *gocron.Scheduler
	RestartHarpoon bool
}

func NewHarpoonConfig() *HarpoonConfig {
	return &HarpoonConfig{
		Targets: []*api.Target{
			{
				MethodSchedules: make(map[string]string),
			},
		},
	}
}

type FileMountOptions struct {
	// Conn holds the podman client
	Conn   context.Context
	Path   string
	Method string
	Target *api.Target
}

// harpoonCmd represents the base command when called without any subcommands
var harpoonCmd = &cobra.Command{
	Version: "0.0.0",
	Use:     "harpoon",
	Short:   "a tool to schedule gitOps workflows",
	Long:    "Harpoon is a tool to schedule gitOps workflows based on a given configuration file",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main().
func Execute() {
	cobra.CheckErr(harpoonCmd.Execute())
}

func (o *HarpoonConfig) bindFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&o.Volume, "volume", defaultVolume, "podman volume to hold harpoon data. If volume doesn't exist, harpoon will create it.")
}

var (
	harpoonVolume string
	harpoonConfig *HarpoonConfig
)

// restart fetches new targets from an updated config
// with hardReset. With hard reset existing targets
// will be wiped and replaced. If hardReset == false (default), only targets
// and methods with changes will be reset, new targets will be
// added and stale targets disappear.
func (hc *HarpoonConfig) Restart() {
	hc.Scheduler.RemoveByTags(kubeMethod, ansibleMethod, fileTransferMethod, systemdMethod, rawMethod)
	hc.InitConfig(false)
	hc.GetTargets(false)
	hc.RunTargets()
}

// initconfig reads in config file and env variables if set, and initializes HarpoonConfig.
func (hc *HarpoonConfig) InitConfig(initial bool) {
	v := viper.New()
	var config = NewHarpoonConfig()
	// may or may not be from initial startup, this runs with each processConfig, also
	// on initial runs, if config file exists locally, run that first.
	// If configURL is not set in file, the env var will be picked up with first scheduled processing
	if initial && !isLocalConfig(config, v) {
		// env var is set from the configTarget.Url in processConfig, if this is not the initial run.
		envURL := os.Getenv("HARPOON_CONFIG_URL")
		_ = hc.CheckForConfigUpdates(envURL, false)
	}
	// if not initial run, only way to get here is if already determined need for reload
	defaultConfigPath := DefaultConfigPath
	flagConfigDir := filepath.Dir(defaultConfigPath)
	flagConfigName := filepath.Base(defaultConfigPath)
	v.AddConfigPath(flagConfigDir)
	v.SetConfigName(flagConfigName)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err == nil {
		klog.Infof("Using config file: %s", v.ConfigFileUsed())
		if err := v.Unmarshal(&config); err != nil {
			cobra.CheckErr(err)
		}
	} else {
		log.Fatalf("could not locate harpoon config file, place at %s and restart", defaultConfigPath)
	}

	if len(config.Targets) == 0 {
		cobra.CheckErr("no harpoon targets found, exiting")
	}
	if config.Volume == "" {
		config.Volume = defaultVolume
	}

	harpoonVolume = config.Volume
	ctx := context.Background()
	if hc.Conn == nil {
		// TODO: socket directory same for all platforms?
		// sock_dir := os.Getenv("XDG_RUNTIME_DIR")
		// socket := "unix:" + sock_dir + "/podman/podman.sock"
		conn, err := bindings.NewConnection(ctx, "unix://run/podman/podman.sock")
		if err != nil || conn == nil {
			log.Fatalf("error establishing connection to podman.sock: %v", err)
		}
		hc.Conn = conn
	}

	if err := FetchImage(hc.Conn, harpoonImage, false); err != nil {
		cobra.CheckErr(err)
	}
	beforeTargets := len(hc.Targets)
	hc.Targets = config.Targets
	if beforeTargets > 0 {
		// replace LastCommit - to avoid re-running same jobs, since the scheduler finished all jobs
		// with the last commit before arriving here
		for i, t := range hc.Targets {
			if t.Methods.Raw != nil {
				t.Methods.Raw.LastCommit = config.Targets[i].Methods.Raw.LastCommit
			}
			if t.Methods.Kube != nil {
				t.Methods.Kube.LastCommit = config.Targets[i].Methods.Kube.LastCommit
			}
			if t.Methods.Ansible != nil {
				t.Methods.Ansible.LastCommit = config.Targets[i].Methods.Ansible.LastCommit
			}
			if t.Methods.FileTransfer != nil {
				t.Methods.FileTransfer.LastCommit = config.Targets[i].Methods.FileTransfer.LastCommit
			}
			if t.Methods.Systemd != nil {
				t.Methods.Systemd.LastCommit = config.Targets[i].Methods.Systemd.LastCommit
			}
		}
	}

	// look for a ConfigFileTarget, only find the first
	// TODO: add logic to merge multiple configs
	for _, t := range hc.Targets {
		if t.Methods.ConfigTarget == nil {
			continue
		}
		// reset URL if necessary
		// ConfigUrl set in config file overrides env variable
		// If the same, this is no change, if diff then the new config has updated the configUrl
		if t.Methods.ConfigTarget.ConfigUrl != "" {
			os.Setenv("HARPOON_CONFIG_URL", t.Methods.ConfigTarget.ConfigUrl)
		}
		break
	}
}

// getTargets returns map of repoName to map of method:Schedule
func (hc *HarpoonConfig) GetTargets(initial bool) {
	for _, target := range hc.Targets {
		target.Mu.Lock()
		defer target.Mu.Unlock()
		schedMethods := make(map[string]string)
		if target.Methods.ConfigTarget != nil {
			schedMethods[configMethod] = target.Methods.ConfigTarget.Schedule
		}
		if target.Methods.Raw != nil {
			schedMethods[rawMethod] = target.Methods.Raw.Schedule
		}
		if target.Methods.Kube != nil {
			schedMethods[kubeMethod] = target.Methods.Kube.Schedule
		}
		if target.Methods.Systemd != nil {
			schedMethods[systemdMethod] = target.Methods.Systemd.Schedule
		}
		if target.Methods.FileTransfer != nil {
			schedMethods[fileTransferMethod] = target.Methods.FileTransfer.Schedule
		}
		if target.Methods.Ansible != nil {
			schedMethods[ansibleMethod] = target.Methods.Ansible.Schedule
		}
		target.MethodSchedules = schedMethods
		hc.update(target)
	}
}

// This assumes each Target has no more than 1 each of Raw, Systemd, FileTransfer
func (hc *HarpoonConfig) RunTargets() {
	allTargets := make(map[string]map[string]string)
	for _, target := range hc.Targets {
		if target.Url != "" {
			if err := hc.getClone(target); err != nil {
				klog.Warningf("Target: %s, clone error: %v, will retry next scheduled run", target.Name, err)
			}
		}
		allTargets[target.Name] = target.MethodSchedules
	}

	hc.Scheduler = gocron.NewScheduler(time.UTC)
	s := hc.Scheduler
	for repoName, schedMethods := range allTargets {
		var target api.Target
		for _, t := range hc.Targets {
			if repoName == t.Name {
				target = *t
			}
		}

		for method, schedule := range schedMethods {
			switch method {
			case configMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.RemoveByTag(configMethod)
				s.Cron(schedule).Tag(configMethod).Do(hc.processConfig, ctx, &target, schedule)
			case kubeMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.Cron(schedule).Tag(kubeMethod).Do(hc.processKube, ctx, &target, schedule)
			case rawMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.Cron(schedule).Tag(rawMethod).Do(hc.processRaw, ctx, &target, schedule)
			case systemdMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.Cron(schedule).Tag(systemdMethod).Do(hc.processSystemd, ctx, &target, schedule)
			case fileTransferMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.Cron(schedule).Tag(fileTransferMethod).Do(hc.processFileTransfer, ctx, &target, schedule)
			case ansibleMethod:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				klog.Infof("Processing Target: %s Method: %s", target.Name, method)
				s.Cron(schedule).Tag(ansibleMethod).Do(hc.processAnsible, ctx, &target, schedule)
			default:
				klog.Warningf("Target: %s Method: %s, unknown method type, ignoring", target.Name, method)
			}
		}
	}
	s.StartAsync()
	select {}
}

func (hc *HarpoonConfig) processConfig(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()
	hc.RestartHarpoon = false

	// configUrl in config file will override the environment variable
	config := target.Methods.ConfigTarget
	envURL := os.Getenv("HARPOON_CONFIG_URL")
	// config.Url from target overrides env variable
	if config.ConfigUrl != "" {
		envURL = config.ConfigUrl
	}
	os.Setenv("HARPOON_CONFIG_URL", envURL)
	// If ConfigUrl is not populated, warn and leave
	if envURL == "" {
		klog.Warningf("Harpoon ConfigFileTarget found, but neither $HARPOON_CONFIG_URL on system nor ConfigTarget.ConfigUrl are set, exiting without updating the config.")
	}
	// CheckForConfigUpdates downloads & places config file in defaultConfigPath
	// if the downloaded config file differs from what's currently on the system.
	hc.RestartHarpoon = hc.CheckForConfigUpdates(envURL, true)
	if !hc.RestartHarpoon {
		return
	}
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: configMethod,
		Target: target,
		Path:   DefaultConfigPath,
	}
	if err := hc.EngineMethod(ctx, mo, nil); err != nil {
		klog.Warningf("Unexpected error processing config, will retry next run: %v", err)
		return
	}

	hc.update(target)
	if hc.RestartHarpoon {
		harpoonConfig.Restart()
	}
}

func (hc *HarpoonConfig) processRaw(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()

	raw := target.Methods.Raw
	tag := []string{".json", ".yaml", ".yml"}
	var targetFile string
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: rawMethod,
		Target: target,
	}

	if raw.LastCommit == nil {
		// if this initial fetch of a commit fails, leave, will try again w/ next scheduled run
		hc.ResetTarget(target, rawMethod, nil)
		if raw.LastCommit == nil {
			return
		}
		fileName, subDirTree, err := hc.GetPathOrTree(target, raw.TargetPath, rawMethod)
		if err != nil {
			hc.ResetTarget(target, rawMethod, err)
			return
		}
		targetFile, err = hc.ApplyInitial(ctx, mo, fileName, raw.TargetPath, &tag, subDirTree)
		if err != nil {
			hc.ResetTarget(target, rawMethod, err)
			return
		}
		mo.Path = targetFile
	}

	if err := hc.GetChangesAndRunEngine(ctx, mo); err != nil {
		hc.ResetTarget(target, rawMethod, err)
		return
	}
	hc.update(target)
}

func (hc *HarpoonConfig) processAnsible(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()

	ans := target.Methods.Ansible
	tag := []string{"yaml", "yml"}
	var targetFile = ""
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: ansibleMethod,
		Target: target,
	}
	if ans.LastCommit == nil {
		// if this initial fetch of a commit fails, leave, will try again w/ next scheduled run
		hc.ResetTarget(target, ansibleMethod, nil)
		if ans.LastCommit == nil {
			return
		}
		fileName, subDirTree, err := hc.GetPathOrTree(target, ans.TargetPath, ansibleMethod)
		if err != nil {
			hc.ResetTarget(target, ansibleMethod, err)
			return
		}
		targetFile, err = hc.ApplyInitial(ctx, mo, fileName, ans.TargetPath, &tag, subDirTree)
		if err != nil {
			hc.ResetTarget(target, ansibleMethod, err)
			return
		}
		mo.Path = targetFile
	}

	if err := hc.GetChangesAndRunEngine(ctx, mo); err != nil {
		hc.ResetTarget(target, ansibleMethod, err)
		return
	}
	hc.update(target)
}

func (hc *HarpoonConfig) processSystemd(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()

	sd := target.Methods.Systemd
	var targetFile string
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: systemdMethod,
		Target: target,
	}
	tag := []string{".service"}
	if sd.LastCommit == nil {
		// if this initial fetch of a commit fails, leave, will try again w/ next scheduled run
		hc.ResetTarget(target, systemdMethod, nil)
		if sd.LastCommit == nil {
			return
		}
		fileName, subDirTree, err := hc.GetPathOrTree(target, sd.TargetPath, systemdMethod)
		if err != nil {
			hc.ResetTarget(target, systemdMethod, err)
			return
		}
		targetFile, err = hc.ApplyInitial(ctx, mo, fileName, sd.TargetPath, &tag, subDirTree)
		if err != nil {
			hc.ResetTarget(target, systemdMethod, err)
			return
		}
		mo.Path = targetFile
	}

	if err := hc.GetChangesAndRunEngine(ctx, mo); err != nil {
		hc.ResetTarget(target, systemdMethod, err)
		return
	}
	hc.update(target)
}

func (hc *HarpoonConfig) processFileTransfer(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()

	ft := target.Methods.FileTransfer
	var targetFile string
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: fileTransferMethod,
		Target: target,
	}
	if ft.LastCommit == nil {
		// if this initial fetch of a commit fails, leave, will try again w/ next scheduled run
		hc.ResetTarget(target, fileTransferMethod, nil)
		if ft.LastCommit == nil {
			return
		}
		fileName, subDirTree, err := hc.GetPathOrTree(target, ft.TargetPath, fileTransferMethod)
		if err != nil {
			hc.ResetTarget(target, fileTransferMethod, err)
			return
		}
		targetFile, err = hc.ApplyInitial(ctx, mo, fileName, ft.TargetPath, nil, subDirTree)
		if err != nil {
			hc.ResetTarget(target, fileTransferMethod, err)
			return
		}
		mo.Path = targetFile
	}

	if err := hc.GetChangesAndRunEngine(ctx, mo); err != nil {
		hc.ResetTarget(target, fileTransferMethod, err)
		return
	}
	hc.update(target)
}

func (hc *HarpoonConfig) processKube(ctx context.Context, target *api.Target, schedule string) {
	target.Mu.Lock()
	defer target.Mu.Unlock()

	kube := target.Methods.Kube
	tag := []string{"yaml", "yml"}
	var targetFile string
	mo := &FileMountOptions{
		Conn:   hc.Conn,
		Method: kubeMethod,
		Target: target,
	}
	if kube.LastCommit == nil {
		// if this initial fetch of a commit fails, leave, will try again w/ next scheduled run
		hc.ResetTarget(target, kubeMethod, nil)
		if kube.LastCommit == nil {
			return
		}
		fileName, subDirTree, err := hc.GetPathOrTree(target, kube.TargetPath, kubeMethod)
		if err != nil {
			hc.ResetTarget(target, kubeMethod, err)
			return
		}
		targetFile, err = hc.ApplyInitial(ctx, mo, fileName, kube.TargetPath, &tag, subDirTree)
		if err != nil {
			hc.ResetTarget(target, kubeMethod, err)
			return
		}
		mo.Path = targetFile
	}

	if err := hc.GetChangesAndRunEngine(ctx, mo); err != nil {
		hc.ResetTarget(target, kubeMethod, err)
		return
	}
	hc.update(target)
}

func (hc *HarpoonConfig) ApplyInitial(ctx context.Context, mo *FileMountOptions, fileName, tp string, tag *[]string, subDirTree *object.Tree) (string, error) {
	directory := filepath.Base(mo.Target.Url)
	if fileName != "" {
		found := false
		if checkTag(tag, fileName) {
			found = true
			mo.Path = filepath.Join(directory, fileName)
			if err := hc.EngineMethod(ctx, mo, nil); err != nil {
				return fileName, utils.WrapErr(err, "error running engine with method %s, for file %s",
					mo.Method, fileName)
			}
		}
		if !found {
			err := fmt.Errorf("%s target file must be of type %v", mo.Method, tag)
			return fileName, utils.WrapErr(err, "error running engine with method %s, for file %s",
				mo.Method, fileName)
		}

	} else {
		// ... get the files iterator and print the file
		err := subDirTree.Files().ForEach(func(f *object.File) error {
			if checkTag(tag, f.Name) {
				mo.Path = filepath.Join(directory, tp, f.Name)
				if err := hc.EngineMethod(ctx, mo, nil); err != nil {
					return utils.WrapErr(err, "error running engine with method %s, for file %s",
						mo.Method, mo.Path)
				}
			}
			return nil
		})
		if err != nil {
			return fileName, err
		}
	}
	return fileName, nil
}

func (hc *HarpoonConfig) GetChangesAndRunEngine(ctx context.Context, mo *FileMountOptions) error {
	var lastCommit *object.Commit
	var targetPath string
	switch mo.Method {
	case rawMethod:
		raw := mo.Target.Methods.Raw
		lastCommit = raw.LastCommit
		targetPath = raw.TargetPath
	case kubeMethod:
		kube := mo.Target.Methods.Kube
		lastCommit = kube.LastCommit
		targetPath = kube.TargetPath
	case ansibleMethod:
		ans := mo.Target.Methods.Ansible
		lastCommit = ans.LastCommit
		targetPath = ans.TargetPath
	case fileTransferMethod:
		ft := mo.Target.Methods.FileTransfer
		lastCommit = ft.LastCommit
		targetPath = ft.TargetPath
	case systemdMethod:
		sd := mo.Target.Methods.Systemd
		lastCommit = sd.LastCommit
		targetPath = sd.TargetPath
	default:
		return fmt.Errorf("unknown method: %s", mo.Method)
	}

	tp := targetPath
	if mo.Path != "" {
		tp = mo.Path
	}
	var (
		newCommit *object.Commit
		err       error
	)
	changesThisMethod := make(map[*object.Change]string)
	changesThisMethod, newCommit, err = hc.findDiff(mo, tp, lastCommit)
	if err != nil {
		return utils.WrapErr(err, "error method: %s commit: %s", mo.Method, lastCommit.Hash.String())
	}
	hc.SetLastCommit(mo.Target, mo.Method, newCommit)
	hc.update(mo.Target)

	if len(changesThisMethod) == 0 {
		//if mo.Method == systemdMethod && mo.Target.Methods.Systemd.RestartAlways {
		//	mo.Path = filepath.Join(filepath.Base(mo.Target.Url), tp)
		//	if err := hc.EngineMethod(ctx, mo, nil); err != nil {
		//		return utils.WrapErr(err, "error method: %s path: %s, commit: %s", mo.Method, mo.Path, newCommit.Hash.String())
		//	}
		//} else {
			klog.Infof("Target: %s, Method: %s: Path: %s, No changes.....Requeuing", mo.Target.Name, mo.Method, tp)
			return nil
		}
	}

	for change, path := range changesThisMethod {
		mo.Path = path
		if err := hc.EngineMethod(ctx, mo, change); err != nil {
			return utils.WrapErr(err, "error method: %s path: %s, commit: %s", mo.Method, mo.Path, newCommit.Hash.String())
		}
	}
	return nil
}

func (hc *HarpoonConfig) update(target *api.Target) {
	for _, t := range hc.Targets {
		if target.Name == t.Name {
			t = target
		}
	}
}

func (hc *HarpoonConfig) findDiff(mo *FileMountOptions, targetPath string, commit *object.Commit) (map[*object.Change]string, *object.Commit, error) {
	directory := filepath.Base(mo.Target.Url)
	// map of change to path
	thisMethodChanges := make(map[*object.Change]string)
	gitRepo, err := git.PlainOpen(directory)
	if err != nil {
		return thisMethodChanges, nil, fmt.Errorf("error while opening the repository: %v", err)
	}
	w, err := gitRepo.Worktree()
	if err != nil {
		return thisMethodChanges, nil, fmt.Errorf("error while opening the worktree: %v", err)
	}
	var beforeFetchTree *object.Tree
	// ... retrieve the tree from this method's last fetched commit
	beforeFetchTree, _, err = getTree(gitRepo, commit)
	if err != nil {
		return thisMethodChanges, nil, fmt.Errorf("error checking out last known commit, has branch been force-pushed, commit no longer exists?: %v", err)
	}

	// Fetch the latest changes from the origin remote and merge into the current branch
	ref := fmt.Sprintf("refs/heads/%s", mo.Target.Branch)
	refName := plumbing.ReferenceName(ref)
	refSpec := gitconfig.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/heads/%s", mo.Target.Branch, mo.Target.Branch))
	if err = gitRepo.Fetch(&git.FetchOptions{
		RefSpecs: []gitconfig.RefSpec{refSpec, "HEAD:refs/heads/HEAD"},
		Force:    true,
	}); err != nil && err != git.NoErrAlreadyUpToDate {
		return nil, commit, err
	}

	// force checkout to latest fetched branch
	if err := w.Checkout(&git.CheckoutOptions{
		Branch: refName,
		Force:  true,
	}); err != nil {
		return thisMethodChanges, nil, fmt.Errorf("error checking out latest branch %s: %v", ref, err)
	}

	afterFetchTree, newestCommit, err := getTree(gitRepo, nil)
	if err != nil {
		return thisMethodChanges, nil, err
	}

	changes, err := afterFetchTree.Diff(beforeFetchTree)
	if err != nil {
		return thisMethodChanges, nil, fmt.Errorf("%s: error while generating diff: %s", directory, err)
	}
	// the change logic is backwards "From" is actually "To"
	for _, change := range changes {
		if strings.Contains(change.From.Name, targetPath) {
			path := directory + "/" + change.From.Name
			thisMethodChanges[change] = path
		} else if strings.Contains(change.To.Name, targetPath) {
			thisMethodChanges[change] = deleteFile
		}
	}
	return thisMethodChanges, newestCommit, nil
}

// Each engineMethod call now owns the prev and dest variables instead of being shared in mo
func (hc *HarpoonConfig) EngineMethod(ctx context.Context, mo *FileMountOptions, change *object.Change) error {
	switch mo.Method {
	case rawMethod:
		if change == nil {
			return rawPodman(ctx, mo, nil)
		}
		prev, err := getChangeString(change)
		if err != nil {
			return err
		}
		return rawPodman(ctx, mo, prev)
	case configMethod:
		// Only here if config has been updated by CheckForConfigUpdates.
		// If so, update files on disk with fileTransferPodman.
		// Updated config file is at /opt/mount/config.yaml in harpoon pod
		// cp updated config /opt/mount/config.yaml in pod to $HOME/.harpoon/config.yaml on host
		dest := DefaultHostConfigPath
		mo.Path = DefaultConfigPath
		if err := fileTransferPodman(ctx, mo, nil, dest); err != nil {
			return err
		}
		return nil
	case systemdMethod:
		var prev *string = nil
		if change != nil {
			if change.To.Name != "" {
				prev = &change.To.Name
			}
		}
		nonRootHomeDir := os.Getenv("HOME")
		if nonRootHomeDir == "" {
			return fmt.Errorf("Could not determine $HOME for host, must set $HOME on host machine for non-root systemd method")
		}
		var dest string
		if mo.Target.Methods.Systemd.Root {
			dest = systemdPathRoot
		} else {
			dest = filepath.Join(nonRootHomeDir, ".config", "systemd", "user")
		}
		klog.Infof("Deploying systemd file(s) %s", mo.Path)
		if err := fileTransferPodman(ctx, mo, prev, dest); err != nil {
			return utils.WrapErr(err, "Error deploying systemd file(s) Target: %s, Path: %s", mo.Target.Name, mo.Target.Methods.Systemd.TargetPath)
		}
		klog.Infof("Transferring systemd unit file %s", filepath.Base(mo.Path))
		return systemdPodman(ctx, mo, dest)
	case fileTransferMethod:
		var prev *string = nil
		if change != nil {
			if change.To.Name != "" {
				prev = &change.To.Name
			}
		}
		dest := mo.Target.Methods.FileTransfer.DestinationDirectory
		return fileTransferPodman(ctx, mo, prev, dest)
	case kubeMethod:
		if change == nil {
			return kubePodman(ctx, mo, nil)
		}
		prev, err := getChangeString(change)
		if err != nil {
			return err
		}
		return kubePodman(ctx, mo, prev)
	case ansibleMethod:
		return ansiblePodman(ctx, mo)
	default:
		return fmt.Errorf("unsupported method: %s", mo.Method)
	}
	return nil
}

func (hc *HarpoonConfig) getClone(target *api.Target) error {
	directory := filepath.Base(target.Url)
	absPath, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	var exists bool
	if _, err := os.Stat(directory); err == nil {
		exists = true
		// if directory/.git does not exist, fail quickly
		if _, err := os.Stat(directory + "/.git"); err != nil {
			return fmt.Errorf("%s exists but is not a git repository", directory)
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if !exists {
		klog.Infof("git clone %s %s --recursive", target.Url, target.Branch)
		var user string
		if hc.PAT != "" {
			user = "harpoon"
		}
		_, err = git.PlainClone(absPath, false, &git.CloneOptions{
			Auth: &githttp.BasicAuth{
				Username: user, // the value of this field should not matter when using a PAT
				Password: hc.PAT,
			},
			URL:           target.Url,
			ReferenceName: plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", target.Branch)),
			SingleBranch:  true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (hc *HarpoonConfig) GetPathOrTree(target *api.Target, subDir, method string) (string, *object.Tree, error) {
	directory := filepath.Base(target.Url)
	gitRepo, err := git.PlainOpen(directory)
	if err != nil {
		return "", nil, err
	}
	tree, _, err := getTree(gitRepo, nil)
	if err != nil {
		return "", nil, err
	}

	subDirTree, err := tree.Tree(subDir)
	if err != nil {
		if err == object.ErrDirectoryNotFound {
			// check if exact filepath
			file, err := tree.File(subDir)
			if err == nil {
				return file.Name, nil, nil
			}
		}
	}
	return "", subDirTree, err
}

// arrive at ResetTarget 1 of 2 ways:
//      1) initial run of target - if clone or commit fetch fails, don't set LastCommit, try again next run
//      2) processing error during run - will attempt to fetch the remote commit
func (hc *HarpoonConfig) ResetTarget(target *api.Target, method string, err error) {
	if err != nil {
		klog.Warningf("Target: %s Method: %s encountered error: %v, resetting...", target.Name, method, err)
	}
	commit, err := hc.getGit(target)
	if err != nil {
		klog.Warningf("Target: %s error fetching commit, will retry next scheduled run: %v", target.Name, err)
	}
	if commit == nil {
		klog.Warningf("Target: %s, fetched empty commit, will retry next scheduled run", target.Name)
	}
	hc.SetLastCommit(target, method, commit)
	hc.update(target)
}

func (hc *HarpoonConfig) getGit(target *api.Target) (*object.Commit, error) {
	if err := hc.getClone(target); err != nil {
		return nil, err
	}
	directory := filepath.Base(target.Url)
	gitRepo, err := git.PlainOpen(directory)
	if err != nil {
		return nil, err
	}

	_, commit, err := getTree(gitRepo, nil)
	if err != nil {
		return nil, err
	}
	return commit, nil
}

func (hc *HarpoonConfig) SetLastCommit(target *api.Target, method string, commit *object.Commit) {
	switch method {
	case kubeMethod:
		target.Methods.Kube.LastCommit = commit
	case rawMethod:
		target.Methods.Raw.LastCommit = commit
	case systemdMethod:
		target.Methods.Systemd.LastCommit = commit
	case fileTransferMethod:
		target.Methods.FileTransfer.LastCommit = commit
	case ansibleMethod:
		target.Methods.Ansible.LastCommit = commit
	}
}

// CheckForConfigUpdates, downloads, & places config file
// in defaultConfigPath in harpoon container (/opt/mount/config.yaml).
// This runs with the initial startup as well as with scheduled ConfigTarget runs,
// if $HARPOON_CONFIG_URL is set.
func (hc *HarpoonConfig) CheckForConfigUpdates(envURL string, existsAlready bool) bool {
	if envURL != "" {
		if err := downloadUpdateConfigFile(envURL); err != nil {
			klog.Infof("Could not download config: %v", err)
			return true
		}
	}
	reset := false
	var err error
	reset, err = compareFiles(DefaultConfigNew, DefaultConfigPath)
	if err != nil {
		klog.Warningf("Unable to update config: %v", err)
		return false
	}
	if reset && existsAlready {
		// add a copy
		if err := CopyFile(DefaultConfigPath, DefaultConfigBackup); err != nil {
			klog.Warningf("Error copying %s to %s", DefaultConfigPath, DefaultConfigBackup)
			return false
		}
	}
	if reset {
		// cp new to default path
		if err := CopyFile(DefaultConfigNew, DefaultConfigPath); err != nil {
			klog.Warningf("Error copying %s to %s", DefaultConfigNew, DefaultConfigPath)
			return false
		}
	}
	return reset
}
