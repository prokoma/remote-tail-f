package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type HttpTailer struct {
	TailerBase

	url               string
	requestTimeoutSec int
	rangeNotSupported bool
	client            *http.Client
}

func NewHttpTailer(url string, requestTimeoutSec int, stateFilePath string) *HttpTailer {
	return &HttpTailer{
		TailerBase: TailerBase{
			stateFilePath: stateFilePath,
			lastOffset:    0,
		},
		url:               url,
		requestTimeoutSec: requestTimeoutSec,
		rangeNotSupported: false,
		client:            &http.Client{},
	}
}

func (t *HttpTailer) FetchNewLines() ([]string, error) {
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
			if !t.rangeNotSupported {
				fmt.Fprintf(os.Stderr, "Server doesn't support range requests.\n")
				t.rangeNotSupported = true
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
