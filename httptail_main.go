package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var (
	intervalSec       = flag.Int("interval-sec", 15, "Number of seconds between checks")
	requestTimeoutSec = flag.Int("request-timeout-sec", 5, "HTTP request timeout in seconds")
	stateFilePath     = flag.String("state-file", "", "Path to store state persistently")
)

type Tailer struct {
	url                      string
	stateFilePath            string
	requestTimeoutSec        int
	lastOffset               int64
	unsupportedRangeRequests bool
	client                   *http.Client
}

func NewTailer(url string, requestTimeoutSec int, stateFilePath string) *Tailer {
	return &Tailer{
		url:               url,
		requestTimeoutSec: requestTimeoutSec,
		stateFilePath:     stateFilePath,
		client:            &http.Client{},
	}
}

func (t *Tailer) fetchNewLines() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.requestTimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", t.url, nil)
	if err != nil {
		return nil, err
	}

	if t.lastOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", t.lastOffset-1))
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		fmt.Fprintf(os.Stderr, "Server returned 206, file was probably truncated. Resetting state.\n")
		t.lastOffset = 0
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	var skipBytes int64 = 0
	if t.lastOffset > 0 {
		if resp.StatusCode == http.StatusPartialContent {
			skipBytes = 1
		} else {
			if !t.unsupportedRangeRequests {
				fmt.Fprintf(os.Stderr, "Server doesn't support range requests.\n")
				t.unsupportedRangeRequests = true
			}
			skipBytes = t.lastOffset
		}
	} else if resp.StatusCode == http.StatusPartialContent {
		return nil, fmt.Errorf("expected 200, got 206")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(body) == 0 {
		fmt.Fprintf(os.Stderr, "Empty response.\n")
		return nil, nil
	}

	nlByte := []byte("\n")
	lines := []string{}

	if len(body) <= int(skipBytes) {
		// fmt.Fprintf(os.Stderr, "No new bytes.\n")
		return nil, nil
	}

	body = body[skipBytes:]
	nlIndex := bytes.Index(body, nlByte)
	for nlIndex != -1 { // got whole line
		lines = append(lines, string(body[0:nlIndex]))
		t.lastOffset += int64(nlIndex + len(nlByte))
		body = body[nlIndex+len(nlByte):]
		nlIndex = bytes.Index(body, nlByte)
	}

	return lines, nil
}

func (t *Tailer) loadState() error {
	if t.stateFilePath == "" {
		t.lastOffset = 0
		return nil
	}
	data, err := os.ReadFile(t.stateFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.lastOffset = 0
			return nil
		}
		return fmt.Errorf("could not read checkpoint file: %v", err)
	}

	var lastOffset int64
	n, err := fmt.Sscanf(string(data), "%d", &lastOffset)
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("invalid checkpoint file")
	}
	if lastOffset < 0 {
		return fmt.Errorf("invalid offset in checkpoint file: %d", lastOffset)
	}
	t.lastOffset = lastOffset
	return nil
}

func (t *Tailer) saveState() error {
	if t.stateFilePath == "" {
		return nil
	}
	data := fmt.Sprintf("%d\n", t.lastOffset)
	return os.WriteFile(t.stateFilePath, []byte(data), 0644)
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] URL\n\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	url := flag.Arg(0)

	tailer := NewTailer(url, *requestTimeoutSec, *stateFilePath)
	err := tailer.loadState()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load state: %v\n", err)
	}

	for {
		lines, err := tailer.fetchNewLines()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching file: %v\n", err)
		} else {
			err := tailer.saveState()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to save state: %v\n", err)
			}
			for _, line := range lines {
				fmt.Println(line)
			}
		}
		time.Sleep(time.Duration(*intervalSec) * time.Second)
	}
}
