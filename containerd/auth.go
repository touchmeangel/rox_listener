package containerd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var dockerHubAliases = []string{
	"docker.io",
	"registry-1.docker.io",
	"index.docker.io",
	"https://index.docker.io/v1/",
}

func isDockerHubHost(host string) bool {
	for _, alias := range dockerHubAliases {
		if host == alias {
			return true
		}
	}
	return false
}

func candidateAuthKeys(host string) []string {
	if !isDockerHubHost(host) {
		return []string{host}
	}
	keys := []string{host}
	for _, alias := range dockerHubAliases {
		if alias != host {
			keys = append(keys, alias)
		}
	}
	return keys
}

type dockerConfigFile struct {
	Auths map[string]struct {
		Auth string `json:"auth"`
	} `json:"auths"`
	CredsStore  string            `json:"credsStore"`
	CredHelpers map[string]string `json:"credHelpers"`
}

func loadDockerConfig() (*dockerConfigFile, error) {
	dir := os.Getenv("DOCKER_CONFIG")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		dir = filepath.Join(home, ".docker")
	}
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil, err
	}
	var cf dockerConfigFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", filepath.Join(dir, "config.json"), err)
	}
	return &cf, nil
}

type credHelperOutput struct {
	ServerURL string `json:"ServerURL"`
	Username  string `json:"Username"`
	Secret    string `json:"Secret"`
}

func runCredHelper(store, host string) (username, secret string, err error) {
	if store == "" {
		return "", "", fmt.Errorf("no credential store configured")
	}
	cmd := exec.Command("docker-credential-"+store, "get")
	cmd.Stdin = strings.NewReader(host)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", "", fmt.Errorf("docker-credential-%s get %s: %w (%s)", store, host, err, strings.TrimSpace(stderr.String()))
	}
	var res credHelperOutput
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		return "", "", fmt.Errorf("parsing docker-credential-%s output: %w", store, err)
	}
	return res.Username, res.Secret, nil
}

func credentialsFor(host string) (string, string, error) {
	cfg, err := loadDockerConfig()
	if err != nil {
		return "", "", nil
	}

	for _, key := range candidateAuthKeys(host) {
		switch {
		case cfg.CredHelpers[key] != "":
			if u, s, err := runCredHelper(cfg.CredHelpers[key], key); err == nil && (u != "" || s != "") {
				return u, s, nil
			}
		case cfg.CredsStore != "":
			if u, s, err := runCredHelper(cfg.CredsStore, key); err == nil && (u != "" || s != "") {
				return u, s, nil
			}
		default:
			if entry, ok := cfg.Auths[key]; ok && entry.Auth != "" {
				if decoded, err := decodeBasicAuth(entry.Auth); err == nil {
					if u, p, found := strings.Cut(decoded, ":"); found {
						return u, p, nil
					}
				}
			}
		}
	}

	return "", "", nil
}
