package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"git.sr.ht/~spc/go-log"
	"github.com/godbus/dbus/v5"
	"github.com/google/uuid"
)

type Worker struct {
	conn     *dbus.Conn
	Features map[string]string
}

func (w *Worker) Dispatch(addr string, id string, metadata map[string]string, data []byte) *dbus.Error {
	go func() {
		workingDirectory := "/var/run/insights-collector-worker"
		if err := os.MkdirAll(workingDirectory, 0755); err != nil {
			replyError(w.conn, "cannot create run directory", err)
			return
		}
		corePath := filepath.Join(workingDirectory, fmt.Sprintf("insights-core-%v.egg", time.Now().Unix()))
		if err := os.WriteFile(corePath, data, 0644); err != nil {
			replyError(w.conn, "cannot write file to path: %v", err)
			return
		}

		cmd := exec.Command("/usr/libexec/platform-python",
			"-m", "insights.collect",
			"--compress")
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"LANG=" + os.Getenv("LANG"),
			"PYTHONPATH=" + corePath,
		}
		cmd.Stderr = os.Stderr

		output, err := cmd.Output()
		if err != nil {
			replyError(w.conn, "cannot run command", err)
			return
		}

		archiveData, err := os.ReadFile(strings.TrimSpace(string(output)))
		if err != nil {
			replyError(w.conn, "cannot read file", err)
			return
		}

		var (
			responseCode     int
			responseMetadata map[string]string
			responseData     []byte
		)

		obj := w.conn.Object("com.redhat.yggdrasil.Dispatcher1", "/com/redhat/yggdrasil/Dispatcher1")
		err = obj.Call("com.redhat.yggdrasil.Dispatcher1.Transmit", 0, "insightscollector", uuid.New().String(), map[string]string{}, archiveData).Store(&responseCode, &responseMetadata, &responseData)
		if err != nil {
			log.Errorf("cannot call com.redhat.yggdrasil.Dispatcher1.Transmit: %v", err)
			return
		}

		log.Infof("responseCode = %v", responseCode)
		log.Infof("responseMetadata = %#v", responseMetadata)
		log.Infof("responseData = %v", responseData)
	}()

	return nil
}

func replyError(conn *dbus.Conn, message string, err error) {
	var (
		responseCode     int
		responseMetadata map[string]string
		responseData     []byte
	)

	log.Errorf("%v: %v", message, err)
	obj := conn.Object("com.redhat.yggdrasil.Dispatcher1", "/com/redhat/yggdrasil/Dispatcher1")
	err = obj.Call("com.redhat.yggdrasil.Dispatcher1.Transmit", 0, "insightscollector", uuid.New().String(), map[string]string{}, err.Error()).Store(&responseCode, &responseMetadata, &responseData)
	if err != nil {
		log.Errorf("cannot call com.redhat.yggdrasil.Dispatcher1.Transmit: %v", err)
		return
	}

	log.Infof("responseCode = %v", responseCode)
	log.Infof("responseMetadata = %#v", responseMetadata)
	log.Infof("responseData = %v", responseData)
}
