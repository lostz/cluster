package cluster

type Config struct {
	Addr    string
	Nodes   []string
	DataDir string `toml:"data_dir"`
	LogDir  string `toml:"log_dir"`
}

const (
	addCmd = "add"
	delCmd = "del"
	setCmd = "set"
)

type action struct {
	Cmd     string   `json:"cmd"`
	Masters []string `json:"masters"`
}
