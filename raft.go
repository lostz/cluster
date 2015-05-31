package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

type Raft struct {
	r         *raft.Raft
	log       *os.File
	dbStore   *raftboltdb.BoltStore
	trans     *raft.NetworkTransport
	peerStore *raft.JSONPeers
	raftAddr  string
}

type masterFSM struct {
	sync.Mutex

	masters map[string]struct{}
}

func MewMasterFSM() *masterFSM {
	fsm := new(masterFSM)
	fsm.masters = make(map[string]struct{})
	return fsm
}

func (fsm *masterFSM) Apply(l *raft.Log) interface{} {
	var a action
	if err := json.Unmarshal(l.Data, &a); err != nil {
		log.Println("decode raft log err %v", err)
		return err
	}

	fsm.handleAction(&a)

	return nil
}

type masterSnapshot struct {
	masters []string
}

func (snap *masterSnapshot) Persist(sink raft.SnapshotSink) error {
	data, _ := json.Marshal(snap.masters)
	_, err := sink.Write(data)
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (snap *masterSnapshot) Release() {

}

func (fsm *masterFSM) Snapshot() (raft.FSMSnapshot, error) {
	snap := new(masterSnapshot)
	snap.masters = make([]string, 0, len(fsm.masters))

	fsm.Lock()
	for master, _ := range fsm.masters {
		snap.masters = append(snap.masters, master)
	}
	fsm.Unlock()
	return snap, nil
}

func (fsm *masterFSM) AddMasters(addrs []string) {
	fsm.Lock()
	defer fsm.Unlock()

	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		fsm.masters[addr] = struct{}{}
	}

}

func (fsm *masterFSM) DelMasters(addrs []string) {
	fsm.Lock()
	defer fsm.Unlock()

	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}

		delete(fsm.masters, addr)
	}
}

func (fsm *masterFSM) SetMasters(addrs []string) {
	m := make(map[string]struct{}, len(addrs))

	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}

		m[addr] = struct{}{}
	}

	fsm.Lock()
	defer fsm.Unlock()

	fsm.masters = m
}

func (fsm *masterFSM) GetMasters() []string {
	fsm.Lock()
	defer fsm.Unlock()

	m := make([]string, 0, len(fsm.masters))
	for master, _ := range fsm.masters {
		m = append(m, master)
	}

	return m
}

func (fsm *masterFSM) Restore(snap io.ReadCloser) error {
	defer snap.Close()

	d := json.NewDecoder(snap)
	var masters []string

	if err := d.Decode(&masters); err != nil {
		return err
	}

	fsm.Lock()
	for _, master := range masters {
		fsm.masters[master] = struct{}{}
	}
	fsm.Unlock()

	return nil
}

func (fsm *masterFSM) handleAction(a *action) {
	switch a.Cmd {
	case addCmd:
		fsm.AddMasters(a.Masters)
	case delCmd:
		fsm.DelMasters(a.Masters)
	case setCmd:
		fsm.SetMasters(a.Masters)
	}
}

func NewRaft(config *Config, fsm raft.FSM) (*Raft, error) {
	r := new(Raft)
	peers := make([]string, 0, len(config.Nodes))

	r.raftAddr = config.Addr
	a, err := net.ResolveTCPAddr("tcp", r.raftAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid raft addr format %s, must host:port, err:%v", r.raftAddr, err)
	}
	peers = raft.AddUniquePeer(peers, a.String())
	for _, node := range config.Nodes {
		a, err = net.ResolveTCPAddr("tcp", node)
		if err != nil {
			return nil, fmt.Errorf("invalid cluster format %s, must host:port, err:%v", node, err)
		}

		peers = raft.AddUniquePeer(peers, a.String())
	}
	os.MkdirAll(config.DataDir, 0755)

	cfg := raft.DefaultConfig()

	if len(config.LogDir) == 0 {
		r.log = os.Stdout
	} else {
		os.MkdirAll(config.LogDir, 0755)
		logFile := path.Join(config.LogDir, "raft.log")
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		r.log = f
		cfg.LogOutput = r.log
	}

	raftDBPath := path.Join(config.DataDir, "raft_db")
	r.dbStore, err = raftboltdb.NewBoltStore(raftDBPath)
	if err != nil {
		return nil, err
	}
	fileStore, err := raft.NewFileSnapshotStore(config.DataDir, 1, r.log)
	if err != nil {
		return nil, err
	}

	r.trans, err = raft.NewTCPTransport(r.raftAddr, nil, 3, 5*time.Second, r.log)
	if err != nil {
		return nil, err
	}

	r.peerStore = raft.NewJSONPeers(config.DataDir, r.trans)
	r.peerStore.SetPeers(peers)
	if peers, _ := r.peerStore.Peers(); len(peers) <= 1 {
		cfg.EnableSingleNode = true
	}

	r.r, err = raft.NewRaft(cfg, fsm, r.dbStore, r.dbStore, fileStore, r.peerStore, r.trans)

	return r, nil

}

func (self *Raft) LeaderCh() <-chan bool {
	return self.r.LeaderCh()
}
func (self *Raft) IsLeader() bool {
	addr := self.r.Leader()
	if addr == "" {
		return false
	} else {
		return addr == self.raftAddr
	}
}
