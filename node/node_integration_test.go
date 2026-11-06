package node_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"Tuna_v2/config"
	"Tuna_v2/node"
)

// The subprocess runs the same race-instrumented test binary as the parent.
func TestNodeProcess(t *testing.T) {
	if os.Getenv("TUNA_NODE_HELPER") != "1" {
		t.Skip("subprocess entry point")
	}

	root := os.Getenv("TUNA_TEST_ROOT")
	id, _ := strconv.Atoi(os.Getenv("TUNA_TEST_ID"))
	instance, err := node.NewNode(
		filepath.Join(root, fmt.Sprintf(".node-key-%d.json", id)),
		"",
		filepath.Join(root, "committee.json"),
		filepath.Join(root, "parameters.json"),
		filepath.Join(root, fmt.Sprintf("db-%d", id)),
		filepath.Join(root, "logs"),
		15,
		id,
	)
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
		err = instance.RunContext(ctx)
		stop()
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(0)
}

type commitRecord struct {
	Index        uint64   `json:"index"`
	Leader       string   `json:"leader"`
	Payloads     []string `json:"payloads"`
	Transactions int      `json:"transactions"`
}

type cluster struct {
	t         *testing.T
	root      string
	n         int
	processes map[int]*exec.Cmd
	files     []*os.File
	starts    int
}

func newCluster(t *testing.T, n int, timeout int) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		root:      t.TempDir(),
		n:         n,
		processes: make(map[int]*exec.Cmd),
	}
	if err := config.GenerateLocalFiles(n, 9000, c.root); err != nil {
		t.Fatal(err)
	}

	// Keep test listeners outside the OS ephemeral client-port range. Closing :0
	// reservations before child startup can otherwise let an outbound dial steal
	// another child's future listening port.
	var listeners []net.Listener

	for attempt := 0; attempt < 100; attempt++ {
		base := 18000 + rand.Intn(10000)
		for i := 0; i < 2*n; i++ {
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				break
			}

			listeners = append(listeners, listener)
		}

		if len(listeners) == 2*n {
			break
		}

		for _, listener := range listeners {
			listener.Close()
		}

		listeners = nil
	}

	if len(listeners) != 2*n {
		t.Fatal("could not reserve test ports")
	}

	data, _ := os.ReadFile(filepath.Join(c.root, "committee.json"))

	var committee map[string]map[string]any

	json.Unmarshal(data, &committee)
	for i := 0; i < n; i++ {
		committee[strconv.Itoa(i)]["addr"] = listeners[i].Addr().String()
		committee[strconv.Itoa(i)]["mempool_addr"] = listeners[i+n].Addr().String()
	}

	data, _ = json.Marshal(committee)
	os.WriteFile(filepath.Join(c.root, "committee.json"), data, 0o600)
	parameters := config.GenDefaultParameters()
	parameters.Pool.Rate = 40
	parameters.Pool.BatchSize = 10
	parameters.Pool.MaxQueueSize = 1000
	parameters.Consensus.LeaderTimeout = timeout
	parameters.Consensus.SyncInterval = 30
	parameters.Mempool.RetryDelay = 30
	parameters.Mempool.RequestPloadDelay = 0
	parameters.Mempool.PayloadDelay = 0
	data, _ = json.Marshal(parameters)
	os.WriteFile(filepath.Join(c.root, "parameters.json"), data, 0o600)
	for _, listener := range listeners {
		listener.Close()
	}

	t.Cleanup(func() {
		c.stopAll()
		for _, file := range c.files {
			file.Close()
		}
	})
	return c
}

func (c *cluster) start(id int) {
	c.t.Helper()
	binary, err := os.Executable()
	if err != nil {
		c.t.Fatal(err)
	}

	output, err := os.Create(filepath.Join(c.root, fmt.Sprintf("process-%d-%d.log", id, c.starts)))
	if err != nil {
		c.t.Fatal(err)
	}

	c.files = append(c.files, output)
	c.starts++
	command := exec.Command(binary, "-test.run=^TestNodeProcess$", "-test.timeout=40s")
	command.Env = append(os.Environ(), "TUNA_NODE_HELPER=1", "TUNA_TEST_ROOT="+c.root, fmt.Sprintf("TUNA_TEST_ID=%d", id))
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		c.t.Fatal(err)
	}

	c.processes[id] = command
}

func (c *cluster) stopAll() {
	for _, command := range c.processes {
		command.Process.Signal(syscall.SIGTERM)
	}

	for id, command := range c.processes {
		done := make(chan error, 1)
		go func(command *exec.Cmd) {
			done <- command.Wait()
		}(command)
		select {
		case err := <-done:
			if err != nil {
				c.t.Errorf("node %d exit: %v; logs at %s", id, err, c.root)
			}
		case <-time.After(5 * time.Second):
			command.Process.Kill()
			<-done
			c.t.Errorf("node %d failed graceful shutdown", id)
		}
	}

	c.processes = make(map[int]*exec.Cmd)
}

func (c *cluster) records(id int) []commitRecord {
	c.t.Helper()
	data, _ := os.ReadFile(filepath.Join(c.root, "logs", fmt.Sprintf("node-info-%d.log", id)))

	var result []commitRecord

	for _, line := range strings.Split(string(data), "\n") {
		_, payload, ok := strings.Cut(line, "Tuna_v2 commit ")
		if !ok {
			continue
		}

		var record commitRecord

		if json.Unmarshal([]byte(payload), &record) == nil {
			result = append(result, record)
		}
	}

	return result
}

func (c *cluster) waitFor(ids []int, minCommits int) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, id := range ids {
			records := c.records(id)
			transactions := 0
			for _, record := range records {
				transactions += record.Transactions
			}

			if len(records) < minCommits || transactions == 0 {
				ready = false
			}
		}

		if ready {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	for _, id := range ids {
		data, _ := os.ReadFile(filepath.Join(c.root, "logs", fmt.Sprintf("node-info-%d.log", id)))
		if len(data) > 4000 {
			data = data[len(data)-4000:]
		}

		c.t.Logf("node %d tail: %s", id, data)
	}

	for _, file := range c.files {
		data, _ := os.ReadFile(file.Name())
		if len(data) > 4000 {
			data = data[len(data)-4000:]
		}

		c.t.Logf("process %s: %s", file.Name(), data)
	}

	c.t.Fatal("nodes did not produce expected commits")
}

func (c *cluster) assertConsistency(ids []int) {
	c.t.Helper()
	reference := c.records(ids[0])
	common := len(reference)
	for _, id := range ids {
		records := c.records(id)
		common = min(common, len(records))
		seen := make(map[string]bool)
		for i, record := range records {
			if record.Index != uint64(i+1) {
				c.t.Fatalf("node %d has non-contiguous output indices: %+v", id, record)
			}

			for _, payload := range record.Payloads {
				if seen[payload] {
					c.t.Fatalf("node %d delivered duplicate payload", id)
				}

				seen[payload] = true
			}
		}

		for i := 0; i < min(len(reference), len(records)); i++ {
			if reference[i].Leader != records[i].Leader ||
				!reflect.DeepEqual(reference[i].Payloads, records[i].Payloads) {
				c.t.Fatalf("node %d disagrees at commit %d", id, i+1)
			}
		}
	}

	if common == 0 {
		c.t.Fatal("no common committed prefix")
	}

	c.t.Logf("%d nodes agree on %d commits", len(ids), common)
}

func TestFourAndSevenNodeTCPConsensus(t *testing.T) {
	if testing.Short() {
		t.Skip("TCP subprocess integration")
	}

	for _, n := range []int{4, 7} {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			c := newCluster(t, n, 1000)

			var ids []int

			for i := 0; i < n; i++ {
				ids = append(ids, i)
				c.start(i)
			}

			c.waitFor(ids, 10)
			c.assertConsistency(ids)
		})
	}
}

func TestLateNodeFetchesBlocksAndPayloads(t *testing.T) {
	if testing.Short() {
		t.Skip("TCP subprocess integration")
	}

	c := newCluster(t, 4, 300)
	for i := 0; i < 3; i++ {
		c.start(i)
	}

	c.waitFor([]int{0, 1, 2}, 8)
	c.start(3)
	c.waitFor([]int{
		0,
		1,
		2,
		3,
	}, 12)
	c.assertConsistency([]int{
		0,
		1,
		2,
		3,
	})
}

func TestPersistentClusterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("TCP subprocess integration")
	}

	c := newCluster(t, 4, 1000)
	ids := []int{
		0,
		1,
		2,
		3,
	}
	for _, id := range ids {
		c.start(id)
	}

	c.waitFor(ids, 10)
	c.stopAll()
	before := 0
	for _, id := range ids {
		before = max(before, len(c.records(id)))
	}

	for _, id := range ids {
		c.start(id)
	}

	c.waitFor(ids, before+8)
	c.assertConsistency(ids)
	pattern := regexp.MustCompile(`Tuna_v2 proposal round=(\d+) author=(\d+) hash=([0-9a-f]+)`)
	for _, id := range ids {
		data, _ := os.ReadFile(filepath.Join(c.root, "logs", fmt.Sprintf("node-info-%d.log", id)))
		seen := make(map[string]string)
		for _, match := range pattern.FindAllStringSubmatch(string(data), -1) {
			if old, exists := seen[match[1]]; exists && old != match[3] {
				t.Fatalf("node %d equivocated after restart at round %s", id, match[1])
			}

			seen[match[1]] = match[3]
		}
	}
}
