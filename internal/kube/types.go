package kube

import "time"

type File struct {
	Path    string
	Content []byte
}

// InitStep runs to completion before the agent starts, in our case its downloading user files from storage
type InitStep struct {
	Name  string
	Image string
	Args  []string // appended to the image ENTRYPOINT like docker CMD
	Env   []string // KEY=VALUE visible to this step only
}

type RunSpec struct {
	Name        string // DNS-1123 <=63 chars which consists of Label and Subdomain; becomes the Job and Secret name
	Image       string
	Args        []string // appended to the image ENTRYPOINT, like docker CMD
	Env         []string // KEY=VALUE, agent container only
	Files       []File   // read-only, stored in the Secret
	Scratch     []string // emptyDir mount paths shared by init steps and the agent
	Init        []InitStep
	Timeout     time.Duration
	CPU, Memory string // limits
}

type Result struct {
	ExitCode int32  // the agent's own exit code; -1 if it never produced one
	Reason   string // Completed | Error | OOMKilled | DeadlineExceeded
	Logs     string // tail of the agent's logs
}
