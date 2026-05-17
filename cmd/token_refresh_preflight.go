package cmd

import (
	"context"
	"time"

	"github.com/rclone/rclone/fs"
	configfile "github.com/rclone/rclone/fs/config"
	"github.com/spf13/cobra"
)

var tokenRefreshPreflightTypes = map[string]struct{}{
	"123_open":        {},
	"1guangyapan":     {},
	"aliyunpds":       {},
	"halalcloud_open": {},
}

var tokenRefreshPreflightSkipCommands = map[string]struct{}{
	"authorize":       {},
	"completion":      {},
	"config":          {},
	"gendocs":         {},
	"genautocomplete": {},
	"help":            {},
	"listremotes":     {},
	"obscure":         {},
	"selfupdate":      {},
	"version":         {},
}

const tokenRefreshPreflightTimeout = 20 * time.Second

func runTokenRefreshPreflight(cmd *cobra.Command) {
	if !shouldRunTokenRefreshPreflight(cmd) {
		return
	}
	remotes := configfile.GetRemotes()
	for _, remote := range remotes {
		if !isTokenRefreshPreflightRemote(remote) {
			continue
		}
		go refreshRemoteTokenInBackground(remote)
	}
}

func refreshRemoteTokenInBackground(remote configfile.Remote) {
	refreshCtx, cancel := context.WithTimeout(context.Background(), tokenRefreshPreflightTimeout)
	defer cancel()
	_, err := fs.NewFs(refreshCtx, remote.Name+":")
	if err != nil && err != fs.ErrorIsFile {
		fs.Logf(remote.Name, "Background token refresh failed: %v", err)
		return
	}
	fs.Debugf(remote.Name, "Background token refresh completed")
}

func shouldRunTokenRefreshPreflight(cmd *cobra.Command) bool {
	if cmd == nil || cmd.Parent() == nil {
		return false
	}
	topLevel := cmd
	for topLevel.Parent() != nil && topLevel.Parent().Parent() != nil {
		topLevel = topLevel.Parent()
	}
	_, skip := tokenRefreshPreflightSkipCommands[topLevel.Name()]
	return !skip
}

func isTokenRefreshPreflightRemote(remote configfile.Remote) bool {
	if _, ok := tokenRefreshPreflightTypes[remote.Type]; !ok {
		return false
	}
	if remote.Source == "environment" {
		return true
	}
	switch remote.Type {
	case "123_open":
		return hasRemoteConfigValue(remote.Name, "access_token") || hasRemoteConfigValue(remote.Name, "refresh_token")
	case "1guangyapan":
		return hasRemoteConfigValue(remote.Name, "access_token") || hasRemoteConfigValue(remote.Name, "refresh_token") || hasRemoteConfigValue(remote.Name, "phone_number")
	case "aliyunpds", "halalcloud_open":
		return hasRemoteConfigValue(remote.Name, "refresh_token") || hasRemoteConfigValue(remote.Name, "access_token")
	default:
		return true
	}
}

func hasRemoteConfigValue(remoteName, key string) bool {
	value, found := configfile.FileGetValue(remoteName, key)
	return found && value != ""
}
