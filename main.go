package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"
)

var (
	intervalSec       = flag.Int("interval-sec", 15, "Number of seconds between checks")
	requestTimeoutSec = flag.Int("request-timeout-sec", 5, "Request timeout in seconds")
	stateFilePath     = flag.String("state-file", "", "Path to store state persistently")
)

type Tailer interface {
	FetchNewLines() ([]string, error)
	LoadState() error
	SaveState() error
}

type TailerBase struct {
	stateFilePath string
	lastOffset    int64
}

func (t *TailerBase) LoadState() error {
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

func (t *TailerBase) SaveState() error {
	if t.stateFilePath == "" {
		return nil
	}
	data := fmt.Sprintf("%d\n", t.lastOffset)
	return os.WriteFile(t.stateFilePath, []byte(data), 0644)
}

func CreateTailerFromArgs() (Tailer, error) {
	urlParsed, err := url.Parse(flag.Arg(0))
	if err != nil {
		return nil, fmt.Errorf("invalid url: %v", err)
	}

	switch urlParsed.Scheme {
	case "http", "https":
		return NewHttpTailer(urlParsed.String(), *requestTimeoutSec, *stateFilePath), nil
	case "sftp":
		password, _ := urlParsed.User.Password()
		if password == "" {
			password = os.Getenv("SFTP_PASSWORD")
		}
		if password == "" {
			return nil, fmt.Errorf("provide password in URL or through SFTP_PASSWORD environment variable")
		}
		if len(urlParsed.Path) < 1 {
			return nil, fmt.Errorf("missing file path")
		}
		relPath := urlParsed.Path[1:]
		return NewSftpTailer(urlParsed.Host, urlParsed.User.Username(), password, relPath, *requestTimeoutSec, *stateFilePath), nil
	default:
		return nil, fmt.Errorf("invalid protocol: %v", urlParsed.Scheme)
	}
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] URL\n\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	tailer, err := CreateTailerFromArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Tailer: %v\n", err)
		os.Exit(1)
	}
	err = tailer.LoadState()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load state: %v\n", err)
	}

	for {
		lines, err := tailer.FetchNewLines()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching file: %v\n", err)
		} else {
			err := tailer.SaveState()
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
