package _115

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/rclone/rclone/fs"
)

// ------------------------------------------------------------
// Modifications and Helper Functions
// ------------------------------------------------------------

// parseRootID parses RootID (CID or Share Code) from a path string like remote:{ID}/path
// Returns rootID, receiveCode (if share), error
func parseRootID(s string) (rootID, receiveCode string, err error) {
	// Regex to find {ID} or {share_link} at the beginning
	re := regexp.MustCompile(`^\{([^}]+)\}`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		// No ID found at the start, assume standard path
		return "", "", nil // Return nil error, indicating no special root ID found
	}
	potentialID := m[1]

	// Check if it looks like a CID (19 digits)
	reCID := regexp.MustCompile(`^\d{19}$`)
	if reCID.MatchString(potentialID) {
		return potentialID, "", nil // It's a CID
	}

	// Check if it looks like a share link
	// Use the existing parseShareLink logic
	sCode, rCode, shareErr := parseShareLink(potentialID)
	if shareErr == nil && len(sCode) == 11 { // Check length for basic validation
		return sCode, rCode, nil // It's a share code
	}

	// If it doesn't match known patterns, return an error
	return "", "", fmt.Errorf("invalid format in {}: %q", potentialID)
}

// getID finds the ID of a file or directory at the given path relative to the Fs root.
func (f *Fs) getID(ctx context.Context, relativePath string) (id string, err error) {
	// Handle the case where the Fs itself points to a single file
	if f.fileObj != nil {
		obj := *f.fileObj
		if relativePath == "" || relativePath == obj.Remote() || relativePath == strings.TrimPrefix(obj.Remote(), "isFile:") {
			if ider, ok := obj.(fs.IDer); ok {
				return ider.ID(), nil
			}
			return "", fmt.Errorf("object does not implement IDer interface")
		}
		return "", fmt.Errorf("path %q does not match the single file remote %q", relativePath, obj.Remote())
	}

	// Trim leading/trailing slashes
	cleanPath := strings.Trim(relativePath, "/")

	// If path is empty, return the root ID of the Fs
	if cleanPath == "" {
		rootID, err := f.dirCache.RootID(ctx, false) // Don't create root if it doesn't exist
		if err != nil {
			return "", fmt.Errorf("failed to get root ID: %w", err)
		}
		return rootID, nil
	}

	// Try finding it as a directory first using the cache
	id, err = f.dirCache.FindDir(ctx, cleanPath, false) // create = false
	if err == nil {
		return id, nil // Found as directory
	}
	if !errors.Is(err, fs.ErrorDirNotFound) {
		return "", fmt.Errorf("error finding directory %q: %w", cleanPath, err) // Other error during dir search
	}

	// Directory not found, try finding it as a file object
	fs.Debugf(f, "Path %q not found as directory, trying as file.", cleanPath)
	o, err := f.NewObject(ctx, cleanPath)
	if err == nil {
		// Found as file, return its ID
		objWithID, ok := o.(fs.IDer)
		if !ok || objWithID.ID() == "" {
			// This shouldn't happen if NewObject succeeds
			return "", fmt.Errorf("found object %q but it has no ID", cleanPath)
		}
		return objWithID.ID(), nil
	}

	// If NewObject also fails (e.g., ErrorObjectNotFound), return that error
	return "", fmt.Errorf("path %q not found as file or directory: %w", cleanPath, err)
}

// parseShareLink extracts share code and receive code from a URL string.
// Handles both standard 115.com/s/... links and potentially just the code itself.
func parseShareLink(linkOrCode string) (shareCode, receiveCode string, err error) {
	// Check if it's a full URL
	if strings.Contains(linkOrCode, "115.com/s/") {
		u, parseErr := url.Parse(linkOrCode)
		if parseErr != nil {
			return "", "", fmt.Errorf("invalid share link format: %w", parseErr)
		}
		// Extract code from path: /s/{shareCode}
		pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(pathParts) < 2 || pathParts[0] != "s" || len(pathParts[1]) != 11 {
			return "", "", fmt.Errorf("could not extract valid share code from path: %s", u.Path)
		}
		shareCode = pathParts[1]
		// Extract password from query
		receiveCode = u.Query().Get("password") // Handles missing password gracefully
		return shareCode, receiveCode, nil
	}

	// Assume it might be just the share code (11 characters)
	if len(linkOrCode) == 11 {
		// Basic validation: check if it looks like a typical share code format (alphanumeric)
		// This is a weak check.
		reCode := regexp.MustCompile(`^[a-zA-Z0-9]{11}$`)
		if reCode.MatchString(linkOrCode) {
			return linkOrCode, "", nil // Assume no password if only code is provided
		}
	}

	return "", "", fmt.Errorf("invalid share link or code format: %q", linkOrCode)
}
