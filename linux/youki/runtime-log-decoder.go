// Runtime-log compatibility probe for actual pinned youki failure artifacts.
// It never executes a runtime. The pinned build supplies artifact provenance;
// synthetic unit tests do not substitute for replaying its actual output.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	containerdCommit = "aad11006b869517fcd3009450b6f82da282e1a9b"
	decoderSourceSHA = "95c47a9c3e3520fb4110a1793ce12c9ddcf1506fe7c9787ae040527a94932567"
	youkiCommit      = "94ba653efbb180ce04650f6ae01a8e6bc8f96d92"
	invalidIDError   = "container id can't be used to represent a file name (such as . or ..)"
	expectedMessage  = "error in executing command: " + invalidIDError
)

// decoderLastError reproduces the decoding loop from containerd v2.3.3
// cmd/containerd-shim-runc-v2/process/utils.go, getLastRuntimeError.
// File opening is the only omitted upstream behavior. In particular, do not
// lowercase Level or alias message/timestamp: those would hide the regression.
// The upstream source is Apache-2.0, Copyright The containerd Authors.
func decoderLastError(input io.Reader) (string, error) {
	var (
		err    error
		errMsg string
		log    struct {
			Level string
			Msg   string
			Time  time.Time
		}
	)
	dec := json.NewDecoder(input)
	for err = nil; err == nil; {
		if err = dec.Decode(&log); err != nil && err != io.EOF {
			return "", err
		}
		if log.Level == "error" {
			errMsg = strings.TrimSpace(log.Msg)
		}
	}
	return errMsg, nil
}

// strictRecord is intentionally stronger than upstream's permissive decoder:
// this deterministic invalid-create fixture must emit one complete record,
// with no field inheritance, duplicate keys, ANSI suffix or stale file tail.
func strictRecord(raw []byte) error {
	if len(raw) == 0 || len(raw) > 65536 || !utf8.Valid(raw) || bytes.ContainsRune(raw, '\x1b') || !bytes.HasSuffix(raw, []byte("\n")) || bytes.Count(raw, []byte("\n")) != 1 {
		return errors.New("missing, oversized, non-UTF8 or non-newline JSON log")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("runtime log must contain an object")
	}
	fields := map[string]string{}
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || (key != "level" && key != "msg" && key != "time" && key != "target" && key != "fields") {
			return errors.New("unexpected runtime log field")
		}
		if _, duplicate := fields[key]; duplicate {
			return errors.New("duplicate runtime log field")
		}
		var value string
		if key == "fields" {
			if token, err = dec.Token(); err != nil || token != json.Delim('{') {
				return errors.New("runtime event fields must be an object")
			}
			if token, err = dec.Token(); err != nil || token != "message" {
				return errors.New("runtime event must retain exactly its message")
			}
			if err = dec.Decode(&value); err != nil || value != expectedMessage || dec.More() {
				return errors.New("unexpected or duplicate runtime event field")
			}
			if token, err = dec.Token(); err != nil || token != json.Delim('}') {
				return errors.New("incomplete runtime event fields")
			}
			fields[key] = value
			continue
		}
		if err := dec.Decode(&value); err != nil {
			return err
		}
		fields[key] = value
	}
	if _, err = dec.Token(); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("extra runtime log record or trailing bytes")
	}
	if len(fields) != 5 || fields["level"] != "error" || fields["msg"] != expectedMessage || fields["target"] != "youki" || fields["fields"] != expectedMessage {
		return errors.New("wrong deterministic failure level/message")
	}
	stamp, err := time.Parse(time.RFC3339Nano, fields["time"])
	if err != nil || stamp.IsZero() {
		return errors.New("runtime time is not nonzero RFC3339")
	}
	return nil
}

func digest(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func readRegular(path string, limit int64) ([]byte, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != abs {
		return nil, errors.New("noncanonical or redirected artifact")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() || st.Nlink != 1 || before.Size() > limit {
		return nil, errors.New("bounded single-link regular artifact required")
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || int64(len(raw)) != before.Size() || !os.SameFile(before, after) ||
		after.Size() != before.Size() || after.ModTime() != before.ModTime() {
		return nil, errors.New("artifact changed while reading")
	}
	return raw, nil
}

func verify(candidate string) (map[string]any, error) {
	abs, err := filepath.Abs(candidate)
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{}
	for _, name := range []string{"youki", "inputs.env", "runtime-log.json", "runtime-log-stdout.txt", "runtime-log-stderr.txt", "runtime-log-exit-status.txt"} {
		limit := int64(65536)
		if name == "youki" {
			limit = 128 * 1024 * 1024
		}
		raw, err := readRegular(filepath.Join(abs, name), limit)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		files[name] = raw
	}
	if len(files["youki"]) < 64 || !bytes.Equal(files["youki"][:7], []byte{'\x7f', 'E', 'L', 'F', 2, 1, 1}) ||
		files["youki"][18] != 183 || files["youki"][19] != 0 {
		return nil, errors.New("actual built ELF64 youki required")
	}
	pins := map[string]string{}
	for _, line := range strings.Split(string(files["inputs.env"]), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		_, duplicate := pins[key]
		if !ok || key == "" || duplicate {
			return nil, errors.New("malformed/duplicate source pin")
		}
		pins[key] = value
	}
	if pins["YOUKI_VERSION"] != "0.7.0" || pins["YOUKI_COMMIT"] != youkiCommit {
		return nil, errors.New("unexpected youki source version/commit")
	}
	if string(files["runtime-log-exit-status.txt"]) != "1\n" || len(files["runtime-log-stdout.txt"]) != 0 ||
		string(files["runtime-log-stderr.txt"]) != "Error: "+invalidIDError+"\n" {
		return nil, errors.New("actual invalid-create exit/stdout/stderr mismatch")
	}
	if err := strictRecord(files["runtime-log.json"]); err != nil {
		return nil, err
	}
	message, err := decoderLastError(bytes.NewReader(files["runtime-log.json"]))
	if err != nil || message != expectedMessage {
		return nil, errors.New("pinned containerd decoder did not extract the exact runtime error")
	}
	hashes := map[string]string{}
	for name, raw := range files {
		hashes[name] = digest(raw)
	}
	return map[string]any{"schema_version": 1, "outcome": "actual_youki_invalid_create_log_decoded",
		"scope":              "host_decoder_replay_of_build_time_runtime_failure_not_VM_or_Docker_parity",
		"containerd_version": "2.3.3", "containerd_commit": containerdCommit,
		"decoder_source_sha256": decoderSourceSHA, "youki_commit": youkiCommit,
		"candidate": abs, "input_sha256": hashes, "extracted_error": message}, nil
}

func main() {
	candidate := flag.String("candidate", "", "retained directory containing actual built youki/log/stdout/stderr/exit-status/inputs.env")
	flag.Parse()
	if *candidate == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: runtime-log-decoder --candidate /absolute/retained-candidate")
		os.Exit(2)
	}
	proof, err := verify(*candidate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime log compatibility rejected:", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(proof); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
