package tempo

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"testing"
)

const (
	queue1 = "q1"
	queue2 = "q2"
)

func TestAddTask(t *testing.T) {
	// TODO really complete the tests

	qh := &queueHandler{
		taskQueues: make(map[string]*queue),
	}
	qh.RegisterQueue(queue1, 2, 10)

	task := func() {
		fmt.Println()
	}

	Id, err := qh.AddTask(queue1, task)
	if err != nil {
		t.Errorf("Error adding task: %v", err)
	}

	_, err = qh.AddTask(queue1, task)
	if err != nil {
		t.Errorf("Error adding task: %v", err)
	}

	qinfo, err := qh.ListTask(queue1)
	if err != nil {
		t.Errorf("Error listing tasks: %v", err)

	}
	spew.Dump(qinfo)

	err = qh.StarNextTask(queue1)
	if err != nil {
		t.Errorf("Error setting task as running,  task: %v", err)
	}
	qinfo, err = qh.ListTask(queue1)
	if err != nil {
		t.Errorf("Error listing tasks: %v", err)

	}
	spew.Dump(qinfo)

	err = qh.CompleteTask(queue1, Id)
	if err != nil {
		t.Errorf("Error completing task: %v", err)
	}

	qinfo, err = qh.ListTask(queue1)
	if err != nil {
		t.Errorf("Error listing tasks: %v", err)

	}
	spew.Dump(qinfo)

}
