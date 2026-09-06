package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// All records generated in unit tests are synthetic, not production evidence.
func validRecord() []byte {
	raw, _ := json.Marshal(map[string]any{"level": "error", "msg": expectedMessage, "time": "2026-09-06T04:00:00.123456789Z", "target": "youki", "fields": map[string]string{"message": expectedMessage}})
	return append(raw, '\n')
}

func TestExactContainerdDecoderContract(t *testing.T) {
	for _, test := range []struct {
		name, raw, want string
		fail            bool
	}{
		{"error", string(validRecord()), expectedMessage, false},
		{"upstream-youki-tracing-shape", `{"timestamp":"2026-09-06T04:00:00Z","level":"ERROR","message":"underlying failure"}`, "", false},
		{"uppercase-error-not-selected", `{"time":"2026-09-06T04:00:00Z","level":"ERROR","msg":"underlying failure"}`, "", false},
		{"message-not-msg", `{"level":"error","message":"underlying failure"}`, "", false},
		{"last-error-wins", `{"level":"error","msg":" first "}` + "\n" + `{"level":"warn","msg":"not selected"}` + "\n" + `{"level":"error","msg":" last "}`, "last", false},
		{"invalid-time", `{"level":"error","msg":"failure","time":"not-rfc3339"}`, "", true},
		{"text", "2026-09-06T04:00:00Z ERROR runtime: failed", "", true},
		{"trailing-text", string(validRecord()) + "stale text", "", true},
		{"malformed", `{"level":"error"`, "", true},
		{"empty", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := decoderLastError(strings.NewReader(test.raw))
			if (err != nil) != test.fail || got != test.want {
				t.Fatalf("got %q,%v; want %q,fail=%v", got, err, test.want, test.fail)
			}
		})
	}
}

func TestStrictDeterministicRuntimeRecord(t *testing.T) {
	if err := strictRecord(validRecord()); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"uppercase":                bytes.ReplaceAll(validRecord(), []byte(`"error"`), []byte(`"ERROR"`)),
		"message-alias":            bytes.ReplaceAll(validRecord(), []byte(`"msg"`), []byte(`"message"`)),
		"timestamp-alias":          bytes.ReplaceAll(validRecord(), []byte(`"time"`), []byte(`"timestamp"`)),
		"wrong-error":              bytes.ReplaceAll(validRecord(), []byte(expectedMessage), []byte("unrelated failure")),
		"two-errors":               append(append([]byte{}, validRecord()...), validRecord()...),
		"stale-tail":               append(append([]byte{}, validRecord()...), []byte("old log tail")...),
		"ansi":                     append(append([]byte{}, validRecord()...), []byte("\x1b[0m")...),
		"unknown-field":            bytes.Replace(validRecord(), []byte("{"), []byte(`{"unexpected":true,`), 1),
		"wrong-target":             bytes.Replace(validRecord(), []byte(`"target":"youki"`), []byte(`"target":"other"`), 1),
		"forged-fields":            bytes.Replace(validRecord(), []byte(`"fields":{"message":`), []byte(`"fields":{"level":"error","message":`), 1),
		"duplicate-nested-message": bytes.Replace(validRecord(), []byte(`"fields":{"message":`), []byte(`"fields":{"message":"forged","message":`), 1),
		"nonobject-fields":         bytes.Replace(validRecord(), []byte(`"fields":{`), []byte(`"fields":[{`), 1),
		"extra-newline":            append([]byte("\n"), validRecord()...),
		"duplicate-level":          bytes.Replace(validRecord(), []byte("{"), []byte(`{"level":"warn",`), 1),
		"missing-msg":              []byte("{\"level\":\"error\",\"time\":\"2026-09-06T04:00:00Z\"}\n"),
		"empty":                    nil,
		"missing-newline":          bytes.TrimSuffix(validRecord(), []byte("\n")),
		"invalid-utf8":             []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}', '\n'},
		"wrong-type":               bytes.ReplaceAll(validRecord(), []byte(`"error"`), []byte(`123`)),
		"zero-time":                bytes.ReplaceAll(validRecord(), []byte("2026-09-06T04:00:00.123456789Z"), []byte("0001-01-01T00:00:00Z")),
		"oversized":                append(bytes.Repeat([]byte(" "), 65536), validRecord()...),
	} {
		t.Run(name, func(t *testing.T) {
			if strictRecord(raw) == nil {
				t.Fatal("invalid runtime record accepted")
			}
		})
	}
}

func TestBoundedArtifactInput(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "record")
	if err := os.WriteFile(path, validRecord(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(path, 65536); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(path, 1); err == nil {
		t.Fatal("oversized input accepted")
	}
	link := filepath.Join(root, "symlink")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(link, 65536); err == nil {
		t.Fatal("symlink accepted")
	}
	hard := filepath.Join(root, "hardlink")
	if err := os.Link(path, hard); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(path, 65536); err == nil {
		t.Fatal("multiply linked input accepted")
	}
	if _, err := verify(root); err == nil {
		t.Fatal("missing actual artifact bundle accepted")
	}
}
