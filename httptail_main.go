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
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/lib/atexit"
)

var (
	intervalSec       = flag.Int("interval-sec", 15, "Number of seconds between checks")
	requestTimeoutSec = flag.Int("request-timeout-sec", 5, "HTTP request timeout in seconds")
	stateFilePath     = flag.String("state-file", "", "Path to store state persistently")
)

type Tailer struct {
	url               string
	lastOffset        int64
	lastLines         int
	totalLength       int64
	requestTimeoutSec int
	client            *http.Client
}

func NewTailer(url string, requestTimeoutSec int) *Tailer {
	return &Tailer{
		url:               url,
		requestTimeoutSec: requestTimeoutSec,
		client:            &http.Client{},
	}
}

func getTotalLengthFromContentRange(contentRange string) (int64, error) {
	parts := strings.Split(contentRange, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid Content-Range header: %s", contentRange)
	}
	return strconv.ParseInt(parts[1], 10, 64)
}

func (t *Tailer) fetchNewLines() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.requestTimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", t.url, nil)
	if err != nil {
		return nil, err
	}

	if t.lastOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", t.lastOffset - 1))
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		fmt.Fprintf(os.Stderr, "Server returned 206, file was probably truncated. Resetting state.\n")
		t.lastOffset = 0
		t.lastLines = 0
		t.totalLength = 0
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
			fmt.Fprintf(os.Stderr, "Server doesn't support range requests.\n")
			skipBytes = t.lastOffset
		}
	} else if resp.StatusCode == http.StatusPartialContent {
		return nil, fmt.Errorf("expected 200, got 206")
	}

	var totalLength int64
	if resp.StatusCode == http.StatusPartialContent {
		contentRange := resp.Header.Get("Content-Range")
		var err error
		totalLength, err = getTotalLengthFromContentRange(contentRange)
		if err != nil {
			return nil, err
		}
	} else {
		totalLength = resp.ContentLength
	}

	if totalLength < t.totalLength {
		fmt.Fprintf(os.Stderr, "Total length decreased (old %d, new %d), file was probably truncated. Resetting state.\n", t.totalLength, totalLength)
		t.lastOffset = 0
		t.lastLines = 0
		t.totalLength = 0
		return nil, nil
	}
	t.totalLength = totalLength

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
		t.lastOffset += int64(nlIndex) + 1
		t.lastLines += 1
		body = body[nlIndex+1:]
		nlIndex = bytes.Index(body, nlByte)
	}

	return lines, nil
}

func (t *Tailer) loadState() {
	if *stateFilePath == "" {
		return
	}
	data, err := os.ReadFile(*stateFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		fmt.Fprintf(os.Stderr, "Warning: could not read state file: %v\n", err)
		return
	}
	parts := strings.Split(string(data), "\n")
	if len(parts) >= 3 {
		offset, err1 := strconv.ParseInt(parts[0], 10, 64)
		lines, err2 := strconv.Atoi(parts[1])
		totalLen, err3 := strconv.ParseInt(parts[2], 10, 64)
		if err1 == nil && err2 == nil && err3 == nil {
			t.lastOffset = offset
			t.lastLines = lines
			t.totalLength = totalLen
		}
	}
}

func (t *Tailer) saveState() {
	if *stateFilePath == "" {
		return
	}
	data := fmt.Sprintf("%d\n%d\n%d\n", t.lastOffset, t.lastLines, t.totalLength)
	_ = os.WriteFile(*stateFilePath, []byte(data), 0644)
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] URL\n\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	url := flag.Arg(0)
	tailer := NewTailer(url, *requestTimeoutSec)
	tailer.loadState()

	atexit.Register(func() {
		fmt.Fprintf(os.Stderr, "Caught signal, saving state and exitting.\n")
		tailer.saveState()
	})

	for {
		lines, err := tailer.fetchNewLines()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching file: %v\n", err)
		} else {
			for _, line := range lines {
				fmt.Println(line)
			}
		}
		time.Sleep(time.Duration(*intervalSec) * time.Second)
	}
}
