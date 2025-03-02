package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SftpTailer struct {
	TailerBase

	requestTimeoutSec int
	address           string
	username          string
	password          string
	filePath          string
	client            *sftp.Client
	sshClient         *ssh.Client
}

func NewSftpTailer(address string, username string, password string, filePath string, requestTimeoutSec int, stateFilePath string) *SftpTailer {
	return &SftpTailer{
		TailerBase: TailerBase{
			stateFilePath: stateFilePath,
			lastOffset:    0,
		},
		requestTimeoutSec: requestTimeoutSec,
		address:           address,
		username:          username,
		password:          password,
		filePath:          filePath,
	}
}

func (t *SftpTailer) connect() error {
	config := &ssh.ClientConfig{
		User: t.username,
		Auth: []ssh.AuthMethod{
			ssh.Password(t.password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(t.requestTimeoutSec) * time.Second,
	}

	sshClient, err := ssh.Dial("tcp", t.address, config)
	if err != nil {
		return err
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return err
	}

	t.sshClient = sshClient
	t.client = sftpClient
	return nil
}

func (t *SftpTailer) disconnect() {
	if t.client != nil {
		t.client.Close()
		t.client = nil
	}
	if t.sshClient != nil {
		t.sshClient.Close()
		t.sshClient = nil
	}
}

func (t *SftpTailer) FetchNewLines() ([]string, error) {
	if t.client == nil {
		err := t.connect()
		if err != nil {
			return nil, fmt.Errorf("failed to connect: %v", err)
		}
	}

	file, err := t.client.Open(t.filePath)
	if err != nil {
		t.disconnect()
		return nil, fmt.Errorf("failed to open %s: %v", t.filePath, err)

	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		t.disconnect()
		return nil, fmt.Errorf("failed to stat %s: %v", t.filePath, err)
	}

	if stat.Size() < t.lastOffset {
		fmt.Fprintf(os.Stderr, "File truncated. Resetting state.\n")
		t.lastOffset = 0
	}

	if stat.Size() == t.lastOffset {
		return nil, nil
	}

	_, err = file.Seek(t.lastOffset, io.SeekStart)
	if err != nil {
		t.disconnect()
		return nil, fmt.Errorf("failed to seek %s to %v: %v", t.filePath, t.lastOffset, err)
	}

	body, err := io.ReadAll(file)
	if err != nil {
		t.disconnect()
		return nil, fmt.Errorf("failed to read %s from %v: %v", t.filePath, t.lastOffset, err)
	}

	nlByte := []byte("\n")
	lines := []string{}

	nlIndex := bytes.Index(body, nlByte)
	for nlIndex != -1 {
		lines = append(lines, string(body[0:nlIndex]))
		t.lastOffset += int64(nlIndex + len(nlByte))
		body = body[nlIndex+len(nlByte):]
		nlIndex = bytes.Index(body, nlByte)
	}

	return lines, nil
}
