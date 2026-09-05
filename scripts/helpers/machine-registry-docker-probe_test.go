package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func fixture(t *testing.T) (string, volume) {
	t.Helper()
	root := canonicalTempDir(t)
	value := volume{
		Name: "vz-registry-shared", Driver: "local",
		Mountpoint: filepath.Join(root, "vz-registry-shared", "_data"),
		Labels:     map[string]string{markerLabel: "developer-a"},
	}
	if err := os.MkdirAll(value.Mountpoint, 0700); err != nil {
		t.Fatal(err)
	}
	return root, value
}

func clientForVolume(t *testing.T, value volume, calls *[]string) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		*calls = append(*calls, request.Method+" "+request.URL.Path)
		status := http.StatusOK
		if request.Method == http.MethodPost {
			if request.URL.Path != "/v1.52/volumes/create" {
				t.Errorf("unexpected create path: %s", request.URL.Path)
			}
			var payload struct {
				Name   string
				Driver string
				Labels map[string]string
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.Name != value.Name || payload.Driver != "local" || payload.Labels[markerLabel] != "developer-a" {
				t.Fatalf("unexpected create payload: %+v", payload)
			}
			status = http.StatusCreated
		} else if request.Method != http.MethodGet || request.URL.Path != "/v1.52/volumes/"+value.Name {
			t.Fatalf("unexpected API request: %s %s", request.Method, request.URL.Path)
		}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return response(status, string(data)), nil
	})}
}

func TestCreateThenVerifyUsesRealVolumeContractAndNeverOverwrites(t *testing.T) {
	root, value := fixture(t)
	var calls []string
	client := clientForVolume(t, value, &calls)
	created, err := probe(context.Background(), client, "create", value.Name, "developer-a", root)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || created.Marker != "developer-a" || created.APIOwner != "developer-a" || len(created.MarkerSHA256) != 64 {
		t.Fatalf("incomplete create evidence: %+v; calls=%v", created, calls)
	}
	verified, err := probe(context.Background(), client, "verify", value.Name, "developer-a", root)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || verified.MarkerSHA256 != created.MarkerSHA256 {
		t.Fatal("verify changed content or performed a write API call")
	}
	if _, err := probe(context.Background(), client, "create", value.Name, "developer-a", root); err == nil {
		t.Fatal("create unexpectedly overwrote existing marker")
	}
	if data, err := os.ReadFile(filepath.Join(value.Mountpoint, markerFilename)); err != nil || string(data) != "developer-a" {
		t.Fatalf("original marker changed: %q %v", data, err)
	}
}

func TestForeignVolumeMetadataFailsBeforeMarkerWrite(t *testing.T) {
	for _, field := range []string{"owner", "extra-label", "mountpoint", "driver", "name"} {
		t.Run(field, func(t *testing.T) {
			root, value := fixture(t)
			originalMountpoint := value.Mountpoint
			switch field {
			case "owner":
				value.Labels[markerLabel] = "developer-b"
			case "extra-label":
				value.Labels["foreign"] = "present"
			case "mountpoint":
				value.Mountpoint = root
			case "driver":
				value.Driver = "foreign-plugin"
			case "name":
				value.Name = "foreign-volume"
			}
			client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				data, _ := json.Marshal(value)
				return response(http.StatusCreated, string(data)), nil
			})}
			if _, err := probe(context.Background(), client, "create", "vz-registry-shared", "developer-a", root); err == nil {
				t.Fatal("foreign metadata accepted")
			}
			if _, err := os.Stat(filepath.Join(originalMountpoint, markerFilename)); !os.IsNotExist(err) {
				t.Fatalf("marker created before metadata validation: %v", err)
			}
		})
	}
}

func TestMalformedOversizedAndErrorResponsesFailClosed(t *testing.T) {
	for name, result := range map[string]*http.Response{
		"error":     response(http.StatusInternalServerError, `{}`),
		"malformed": response(http.StatusOK, `{`),
		"oversized": response(http.StatusOK, strings.Repeat(" ", maxResponseBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) { return result, nil })}
			if _, err := requestVolume(context.Background(), client, http.MethodGet, "/v1.52/volumes/test", nil, http.StatusOK); err == nil {
				t.Fatal("invalid API response accepted")
			}
		})
	}
}

func TestInvalidNamesAndSocketNeverReachTransport(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		t.Fatal("invalid fixture reached transport")
		return nil, nil
	})}
	for _, name := range []string{"../escape", "", "slash/name", strings.Repeat("a", 65)} {
		if _, err := probe(context.Background(), client, "create", name, "developer-a", canonicalTempDir(t)); err == nil {
			t.Fatalf("unsafe name accepted: %q", name)
		}
	}
	var output bytes.Buffer
	if err := run([]string{"verify", "--socket", "/var/run/docker.sock", "--volume", "test", "--marker", "developer-a"}, &output); err == nil {
		t.Fatal("non-private socket accepted")
	}
	if output.Len() != 0 {
		t.Fatal("failed invocation emitted success evidence")
	}
}

func TestVerifyRejectsSymlinkMarker(t *testing.T) {
	root, value := fixture(t)
	foreign := filepath.Join(root, "foreign-marker")
	if err := os.WriteFile(foreign, []byte("developer-a"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(value.Mountpoint, markerFilename)); err != nil {
		t.Fatal(err)
	}
	var calls []string
	if _, err := probe(context.Background(), clientForVolume(t, value, &calls), "verify", value.Name, "developer-a", root); err == nil {
		t.Fatal("symlink marker accepted")
	}
}

func TestExactMountpointRejectsSymlinkedDataDirectory(t *testing.T) {
	root, value := fixture(t)
	if err := os.Remove(value.Mountpoint); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(root, "foreign-data")
	if err := os.Mkdir(foreign, 0700); err != nil {
		t.Fatal(err)
	}
	foreignMarker := filepath.Join(foreign, markerFilename)
	if err := os.WriteFile(foreignMarker, []byte("developer-a"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, value.Mountpoint); err != nil {
		t.Fatal(err)
	}
	var calls []string
	if _, err := probe(context.Background(), clientForVolume(t, value, &calls), "verify", value.Name, "developer-a", root); err == nil {
		t.Fatal("symlinked Docker volume data directory accepted")
	}
	data, err := os.ReadFile(foreignMarker)
	if err != nil || string(data) != "developer-a" {
		t.Fatalf("foreign marker changed: %q %v", data, err)
	}
}

func TestExactMarkerRejectsHardlinkModeAndOwnerMismatch(t *testing.T) {
	for _, test := range []string{"hardlink", "mode", "owner"} {
		t.Run(test, func(t *testing.T) {
			_, value := fixture(t)
			marker := filepath.Join(value.Mountpoint, markerFilename)
			if err := os.WriteFile(marker, []byte("developer-a"), 0600); err != nil {
				t.Fatal(err)
			}
			expectedUID := os.Geteuid()
			switch test {
			case "hardlink":
				if err := os.Link(marker, filepath.Join(value.Mountpoint, "marker-alias")); err != nil {
					t.Fatal(err)
				}
			case "mode":
				if err := os.Chmod(marker, 0640); err != nil {
					t.Fatal(err)
				}
			case "owner":
				expectedUID++
			}
			mountpoint, err := openExactDirectory(value.Mountpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer mountpoint.Close()
			if _, err := readExactMarker(mountpoint, "developer-a", expectedUID); err == nil {
				t.Fatalf("%s mismatch accepted", test)
			}
		})
	}
}

func TestDockerClientUsesOnlyUnixSocketAndRejectsRedirect(t *testing.T) {
	var dialNetwork string
	var dialAddress string
	var dialCalls int
	serverResult := make(chan error, 1)
	dial := func(_ context.Context, network, address string) (net.Conn, error) {
		dialCalls++
		dialNetwork = network
		dialAddress = address
		clientConnection, serverConnection := net.Pipe()
		go func() {
			defer serverConnection.Close()
			request, err := http.ReadRequest(bufio.NewReader(serverConnection))
			if err == nil {
				_ = request.Body.Close()
				_, err = fmt.Fprint(serverConnection, "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:2375/fallback\r\nContent-Length: 0\r\n\r\n")
			}
			serverResult <- err
		}()
		return clientConnection, nil
	}
	client, transport := dockerClient(guestDockerSocket, dial)
	defer transport.CloseIdleConnections()
	response, err := client.Get("http://docker/probe")
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("Docker client followed redirect")
	}
	if serverErr := <-serverResult; serverErr != nil {
		t.Fatal(serverErr)
	}
	if dialCalls != 1 || dialNetwork != "unix" || dialAddress != guestDockerSocket {
		t.Fatalf("unexpected dial behavior: calls=%d network=%q address=%q", dialCalls, dialNetwork, dialAddress)
	}
	if transport.Proxy != nil || !transport.DisableKeepAlives {
		t.Fatal("Docker transport enabled proxying or connection reuse")
	}
}
