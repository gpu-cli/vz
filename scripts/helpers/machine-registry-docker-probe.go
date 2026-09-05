// Test-only guest probe for the Machine runtime registry lane. This is not a
// Docker CLI replacement or part of the shipped guest binary allowlist.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const guestDockerSocket = "/run/vz-docker/docker.sock"
const volumeRoot = "/var/lib/docker/engine/volumes"
const markerFilename = ".vz-registry-e2e-marker"
const markerLabel = "dev.vz.registry-e2e.owner"
const maxResponseBytes = 1024 * 1024
const maxMarkerBytes = 64

var safeName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

type volume struct {
	Name       string            `json:"Name"`
	Driver     string            `json:"Driver"`
	Mountpoint string            `json:"Mountpoint"`
	Labels     map[string]string `json:"Labels"`
}

type evidence struct {
	Operation    string `json:"operation"`
	Name         string `json:"name"`
	Mountpoint   string `json:"mountpoint"`
	Marker       string `json:"marker"`
	MarkerSHA256 string `json:"marker_sha256"`
	APIOwner     string `json:"api_owner"`
}

func requestVolume(ctx context.Context, client *http.Client, method, path string, body []byte, status int) (volume, error) {
	var result volume
	request, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return result, err
	}
	if len(data) > maxResponseBytes {
		return result, errors.New("Docker volume response exceeds limit")
	}
	if response.StatusCode != status {
		return result, fmt.Errorf("Docker volume API returned HTTP %d; expected %d", response.StatusCode, status)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("invalid Docker volume response: %w", err)
	}
	return result, nil
}

func verifyVolume(value volume, name, marker, root string) error {
	if value.Name != name || value.Driver != "local" || len(value.Labels) != 1 || value.Labels[markerLabel] != marker {
		return errors.New("Docker volume name, local driver, or exact owner label mismatch")
	}
	if value.Mountpoint != filepath.Join(root, name, "_data") {
		return errors.New("Docker volume mountpoint is not its exact expected private data path")
	}
	return nil
}

// openExactDirectory anchors every absolute path component to a retained
// directory descriptor. os.Root confines each lookup, while Lstat plus the
// post-open inode comparison rejects symlink components and replacement races.
func openExactDirectory(path string) (*os.Root, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("directory path must be absolute and clean")
	}
	current, err := os.OpenRoot(string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	components := strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator))
	for _, component := range components {
		if component == "" {
			continue
		}
		before, err := current.Lstat(component)
		if err != nil {
			_ = current.Close()
			return nil, err
		}
		if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			_ = current.Close()
			return nil, errors.New("directory path contains a non-directory or symbolic link")
		}
		next, err := current.OpenRoot(component)
		if err != nil {
			_ = current.Close()
			return nil, err
		}
		after, err := next.Stat(".")
		if err != nil || !os.SameFile(before, after) {
			_ = next.Close()
			_ = current.Close()
			if err != nil {
				return nil, err
			}
			return nil, errors.New("directory path changed during exact open")
		}
		if err := current.Close(); err != nil {
			_ = next.Close()
			return nil, err
		}
		current = next
	}
	return current, nil
}

func createExactMarker(root *os.Root, marker string) error {
	file, err := root.OpenFile(
		markerFilename,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW,
		0600,
	)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(marker)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func readExactMarker(root *os.Root, marker string, expectedUID int) ([]byte, error) {
	file, err := root.OpenFile(markerFilename, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, statErr
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode() != 0600 || stat.Nlink != 1 || int(stat.Uid) != expectedUID {
		_ = file.Close()
		return nil, errors.New("volume marker must be an exact private single-link regular file owned by the effective user")
	}
	if info.Size() < 0 || info.Size() > maxMarkerBytes || info.Size() != int64(len(marker)) {
		_ = file.Close()
		return nil, errors.New("volume marker has unexpected bounded size")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxMarkerBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(data) > maxMarkerBytes || string(data) != marker {
		return nil, errors.New("private Docker volume marker mismatch")
	}
	return data, nil
}

// root is an injected fixture path in host unit tests. The executable always
// calls this with the fixed guest volumeRoot; no CLI override permits escape.
func probe(ctx context.Context, client *http.Client, operation, name, marker, root string) (evidence, error) {
	var result evidence
	if operation != "create" && operation != "verify" {
		return result, errors.New("operation must be create or verify")
	}
	if !safeName.MatchString(name) || !safeName.MatchString(marker) {
		return result, errors.New("volume and marker must be bounded safe fixture names")
	}
	if operation == "create" {
		body, err := json.Marshal(map[string]any{
			"Name": name, "Driver": "local", "Labels": map[string]string{markerLabel: marker},
		})
		if err != nil {
			return result, err
		}
		created, err := requestVolume(ctx, client, http.MethodPost, "/v1.52/volumes/create", body, http.StatusCreated)
		if err != nil {
			return result, err
		}
		if err := verifyVolume(created, name, marker, root); err != nil {
			return result, err
		}
	}
	current, err := requestVolume(ctx, client, http.MethodGet, "/v1.52/volumes/"+url.PathEscape(name), nil, http.StatusOK)
	if err != nil {
		return result, err
	}
	if err := verifyVolume(current, name, marker, root); err != nil {
		return result, err
	}
	mountpoint, err := openExactDirectory(current.Mountpoint)
	if err != nil {
		return result, fmt.Errorf("open exact Docker volume mountpoint: %w", err)
	}
	defer mountpoint.Close()
	if operation == "create" {
		if err := createExactMarker(mountpoint, marker); err != nil {
			return result, err
		}
	}
	data, err := readExactMarker(mountpoint, marker, os.Geteuid())
	if err != nil {
		return result, err
	}
	return evidence{
		Operation: operation, Name: name, Mountpoint: current.Mountpoint,
		Marker: marker, MarkerSHA256: fmt.Sprintf("%x", sha256.Sum256(data)),
		APIOwner: current.Labels[markerLabel],
	}, nil
}

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

func dockerClient(socket string, dial dialContextFunc) (*http.Client, *http.Transport) {
	if dial == nil {
		dialer := &net.Dialer{Timeout: 5 * time.Second}
		dial = dialer.DialContext
	}
	transport := &http.Transport{
		Proxy: nil, DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dial(ctx, "unix", socket)
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("Docker probe does not follow redirects")
		},
	}
	return client, transport
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("expected create or verify")
	}
	flags := flag.NewFlagSet("machine-registry-docker-probe", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	socket := flags.String("socket", guestDockerSocket, "exact private guest Docker socket")
	name := flags.String("volume", "", "fixture volume")
	marker := flags.String("marker", "", "fixture owner marker")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *socket != guestDockerSocket {
		return errors.New("unexpected arguments or non-private Docker socket")
	}
	client, transport := dockerClient(*socket, nil)
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := probe(ctx, client, args[0], *name, *marker, volumeRoot)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
