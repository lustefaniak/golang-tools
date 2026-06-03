// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/tools/gopls/internal/cache"
	"golang.org/x/tools/gopls/internal/filewatcher"
	internalmcp "golang.org/x/tools/gopls/internal/mcp"
	"golang.org/x/tools/gopls/internal/protocol"
	"golang.org/x/tools/gopls/internal/settings"
)

type headlessMCP struct {
	app *Application

	Address          string `flag:"listen" help:"the address on which to run the mcp server"`
	Logfile          string `flag:"logfile" help:"filename to log to; if unset, logs to stderr"`
	RPCTrace         bool   `flag:"rpc.trace" help:"print MCP rpc traces; cannot be used with -listen"`
	Instructions     bool   `flag:"instructions" help:"if set, print gopls' MCP instructions and exit"`
	FileWatcher      string `flag:"fileWatcher" help:"file watcher mode: 'fsnotify' (default; one OS file descriptor per watched directory), 'poll' (descriptor-free periodic scan, recommended for very large monorepos that would otherwise exhaust the file-descriptor limit), or 'off'"`
	DirectoryFilters string `flag:"directoryFilters" help:"comma-separated list of additional directory filters (e.g. -node_modules,+foo) appended to the directoryFilters setting; excluded trees are not watched, reducing file-descriptor and scan cost on large monorepos"`
}

func (m *headlessMCP) Name() string      { return "mcp" }
func (m *headlessMCP) Parent() string    { return m.app.Name() }
func (m *headlessMCP) Usage() string     { return "[mcp-flags]" }
func (m *headlessMCP) ShortHelp() string { return "start the gopls MCP server in headless mode" }

func (m *headlessMCP) DetailedHelp(f *flag.FlagSet) {
	fmt.Fprint(f.Output(), `
Starts the gopls MCP server in headless mode, without needing an LSP client.
Starts the server over stdio or sse with http, depending on whether the listen flag is provided.

Examples:
  $ gopls mcp -listen=localhost:3000
  $ gopls mcp  //start over stdio
`)
	printFlagDefaults(f)
}

func (m *headlessMCP) Run(ctx context.Context, args ...string) error {
	if m.Instructions {
		fmt.Println(internalmcp.Instructions)
		return nil
	}
	if m.Address != "" && m.RPCTrace {
		// There's currently no way to plumb logging instrumentation into the SSE
		// transport that is created on connections to the HTTP handler, so we must
		// disallow the -rpc.trace flag when using -listen.
		return fmt.Errorf("-listen is incompatible with -rpc.trace")
	}
	if m.Logfile != "" {
		f, err := os.Create(m.Logfile)
		if err != nil {
			return fmt.Errorf("opening logfile: %v", err)
		}
		log.SetOutput(f)
		defer f.Close()
	}

	// Start a new in-process gopls session and create a fake client
	// to connect to it.
	cli, sess, err := m.app.connect(ctx)
	if err != nil {
		return err
	}
	defer cli.terminate(ctx)

	var (
		queueMu  sync.Mutex
		queue    []protocol.FileEvent
		nonempty = make(chan struct{}) // receivable when len(queue) > 0
		stop     = make(chan struct{}) // closed when Run returns
	)
	defer close(stop)

	// This goroutine forwards file change events to the LSP server.
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-nonempty:
				queueMu.Lock()
				q := queue
				queue = nil
				queueMu.Unlock()

				if len(q) > 0 {
					if err := cli.server.DidChangeWatchedFiles(ctx, &protocol.DidChangeWatchedFilesParams{
						Changes: q,
					}); err != nil {
						log.Printf("failed to notify changed files: %v", err)
					}
				}

			}
		}
	}()

	errHandler := func(err error) {
		log.Printf("watch error: %v", err)
	}

	// Build a skip predicate from the directoryFilters setting (plus any
	// additional filters passed via -directoryFilters). This prevents the file
	// watcher from recursively watching irrelevant directory trees (e.g.
	// node_modules), which opens one file descriptor per directory under
	// fsnotify and can exhaust the process's file-descriptor limit on large
	// monorepos.
	filters := append([]string(nil), settings.DefaultOptions(m.app.options).DirectoryFilters...)
	for f := range strings.SplitSeq(m.DirectoryFilters, ",") {
		if f = strings.TrimSpace(f); f != "" {
			filters = append(filters, f)
		}
	}
	pathIncluded := cache.PathIncludeFunc(filters)

	// watchedRoots records the workspace roots reported by the MCP client, so
	// the skip predicate can evaluate the directory filters relative to a root
	// (matching gopls' usual workspace-relative filter semantics).
	var (
		rootsMu      sync.Mutex
		watchedRoots []string
	)
	skipDir := func(absPath string) bool {
		// Evaluate the filters relative to the longest matching watched root.
		rel := absPath
		rootsMu.Lock()
		for _, root := range watchedRoots {
			if r, ok := strings.CutPrefix(absPath, root); ok && len(r) < len(rel) {
				rel = r
			}
		}
		rootsMu.Unlock()
		return !pathIncluded(filepath.ToSlash(rel))
	}

	// Resolve the file watcher mode. fsnotify is the default; poll is offered
	// for very large monorepos where one descriptor per watched directory would
	// exhaust the process file-descriptor limit (e.g. the 256 hard limit macOS
	// launchd imposes on GUI-spawned processes, which Go cannot raise).
	mode := settings.FileWatcherMode(m.FileWatcher)
	if mode == "" {
		mode = settings.FileWatcherFSNotify
	}
	switch mode {
	case settings.FileWatcherOff, settings.FileWatcherFSNotify, settings.FileWatcherPoll:
	default:
		return fmt.Errorf("invalid -fileWatcher mode %q (want off, fsnotify, or poll)", m.FileWatcher)
	}

	var w filewatcher.Watcher
	if mode != settings.FileWatcherOff {
		w, err = filewatcher.New(mode, nil, func(events []protocol.FileEvent) {
			if len(events) == 0 {
				return
			}

			// Since there is no promise [protocol.Server.DidChangeWatchedFiles]
			// will return immediately, we should buffer the captured events and
			// sent them whenever available in a separate go routine.
			queueMu.Lock()
			queue = append(queue, events...)
			queueMu.Unlock()

			select {
			case nonempty <- struct{}{}:
			default:
			}
		}, errHandler, skipDir)
		if err != nil {
			return err
		}
		defer w.Close()
	}

	// TODO(hxjiang): in LSP's use case, the file watcher should watch for LSP
	// initial param workspace root.

	// TODO(hxjiang): refactor the queue pattern into a helper function to avoid
	// repetition.
	var (
		watchStop          = make(chan struct{}) // closed to broadcast "stop" event
		watchQueueNonempty = make(chan struct{}) // each send indicates "nonempty"
		watchQueueMu       sync.Mutex
		watchQueue         []string
	)
	// The watchStop event occurs when this function returns,
	// for any reason (cancellation or completion).
	defer close(watchStop)

	// TODO(hxjiang): memorize the roots and stop watching when the previously
	// watched roots are removed.
	// TODO(hxjiang): implement [filewatcher.Watcher]'s method StopWatchDir.

	// watchRoots is the callback triggered when the MCP client reports workspace
	// roots. We do not call w.WatchDir directly from this callback, but spawn a
	// goroutine for it because WatchDir performs OS-level filesystem operations
	// which can be slow. Blocking this callback would block the MCP server's
	// JSON-RPC message loop and stall the entireconnection.
	watchRoots := func(res *mcp.ListRootsResult, err error) {
		if err != nil {
			errHandler(err)
			return
		}
		watchQueueMu.Lock()
		for _, r := range res.Roots {
			uri, err := protocol.ParseDocumentURI(r.URI)
			if err != nil {
				// Discard invalid URIs.
				// Unlike LSP, MCP does not check URI validity during unmarshaling.
				// Fixes go.dev/issue/74652, crash of May 18 2026.
				log.Printf("mcp.ListRootsResult[*].Roots[*].URI contains invalid URI: %v", err)
				continue
			}
			watchQueue = append(watchQueue, uri.Path())

			// Register the root so skipDir can evaluate filters relative to it.
			rootsMu.Lock()
			watchedRoots = append(watchedRoots, uri.Path())
			rootsMu.Unlock()
		}
		watchQueueMu.Unlock()

		select {
		case watchQueueNonempty <- struct{}{}:
		default:
		}
	}
	go func() {
		for {
			select {
			case <-watchStop:
				return
			case <-watchQueueNonempty:
				watchQueueMu.Lock()
				queue := watchQueue
				watchQueue = nil
				watchQueueMu.Unlock()

				for _, dir := range queue {
					if w == nil {
						continue // -fileWatcher=off
					}
					if err := w.WatchDir(dir); err != nil {
						errHandler(err)
					}
				}
			}
		}
	}()

	if m.Address != "" {
		countHeadlessMCPSSE.Inc()
		return internalmcp.Serve(ctx, m.Address, &staticSessions{sess, cli.server}, false, watchRoots)
	} else {
		countHeadlessMCPStdIO.Inc()
		var rpcLog io.Writer
		if m.RPCTrace {
			rpcLog = log.Writer() // possibly redirected by -logfile above
		}
		log.Printf("Listening for MCP messages on stdin...")
		return internalmcp.StartStdIO(ctx, sess, cli.server, rpcLog, watchRoots)
	}
}

// staticSessions implements the [internalmcp.Sessions] interface for a single gopls
// session.
type staticSessions struct {
	session *cache.Session
	server  protocol.Server
}

func (s *staticSessions) SetSessionExitFunc(func(string)) {}

func (s *staticSessions) FirstSession() (*cache.Session, protocol.Server) {
	return s.session, s.server
}

func (s *staticSessions) Session(id string) (*cache.Session, protocol.Server) {
	if s.session.ID() == id {
		return s.session, s.server
	}
	return nil, nil
}
