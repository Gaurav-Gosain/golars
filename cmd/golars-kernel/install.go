package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/spf13/cobra"
)

// kernelSpec is the schema Jupyter expects at
// <data_dir>/kernels/<name>/kernel.json. argv[0] is replaced with the
// resolved kernel binary path so the registered kernel keeps working
// even if the user moves their PATH entries around.
type kernelSpec struct {
	Argv        []string          `json:"argv"`
	DisplayName string            `json:"display_name"`
	Language    string            `json:"language"`
	Env         map[string]string `json:"env,omitempty"`
}

func newInstallCmd() *cobra.Command {
	var (
		userScope bool
		prefix    string
		name      string
		display   string
	)
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Register the golars kernel with Jupyter",
		Long: `Writes kernel.json + sibling assets into Jupyter's data dir
so JupyterLab / Notebook / VSCode pick "golars" up automatically.

Default scope is per-user (~/.local/share/jupyter/kernels/golars on
Linux, ~/Library/Jupyter/kernels/golars on macOS,
%APPDATA%\jupyter\kernels\golars on Windows). Use --prefix to install
into a venv or conda prefix instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			binPath, err := os.Executable()
			if err != nil {
				return fmt.Errorf("resolve binary: %w", err)
			}
			binPath, _ = filepath.Abs(binPath)

			dir, err := installDir(prefix, userScope, name)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", dir, err)
			}

			spec := kernelSpec{
				Argv:        []string{binPath, "{connection_file}"},
				DisplayName: display,
				Language:    "golars",
			}
			out, err := json.MarshalIndent(spec, "", "  ")
			if err != nil {
				return err
			}
			specPath := filepath.Join(dir, "kernel.json")
			if err := os.WriteFile(specPath, append(out, '\n'), 0o644); err != nil {
				return fmt.Errorf("write %s: %w", specPath, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"installed golars kernel\n  binary: %s\n  spec:   %s\n",
				binPath, specPath,
			)
			return nil
		},
	}
	cmd.Flags().BoolVar(&userScope, "user", true, "install per-user (default)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "install under PREFIX/share/jupyter/kernels/<name> (e.g. a venv root)")
	cmd.Flags().StringVar(&name, "name", "golars", "kernel directory name")
	cmd.Flags().StringVar(&display, "display-name", "golars (.glr)", "display name shown in JupyterLab")
	return cmd
}

func installDir(prefix string, userScope bool, name string) (string, error) {
	if prefix != "" {
		return filepath.Join(prefix, "share", "jupyter", "kernels", name), nil
	}
	if !userScope {
		return "", fmt.Errorf("--user=false requires --prefix")
	}
	return userDataDir(name)
}

// userDataDir returns the per-user Jupyter kernel directory for the
// given kernel name. Mirrors `jupyter --paths` resolution without
// shelling out to python.
func userDataDir(name string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Jupyter", "kernels", name), nil
	case "windows":
		appdata := os.Getenv("APPDATA")
		if appdata == "" {
			appdata = filepath.Join(home, "AppData", "Roaming")
		}
		return filepath.Join(appdata, "jupyter", "kernels", name), nil
	default:
		// Linux + BSD: respect XDG_DATA_HOME.
		if x := os.Getenv("XDG_DATA_HOME"); x != "" {
			return filepath.Join(x, "jupyter", "kernels", name), nil
		}
		return filepath.Join(home, ".local", "share", "jupyter", "kernels", name), nil
	}
}
