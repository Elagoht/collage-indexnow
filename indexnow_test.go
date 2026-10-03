package indexnow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	indexnow "github.com/Elagoht/collage-indexnow"
	"github.com/Elagoht/collage/pkg/collage"
)

const key = "4f1c2e9a-7b3d4c8e"

type submission struct {
	Host        string   `json:"host"`
	Key         string   `json:"key"`
	KeyLocation string   `json:"keyLocation"`
	URLList     []string `json:"urlList"`
}

// endpoint fakes IndexNow: it answers with statuses in turn, the last one for good,
// and records what it was sent.
type endpoint struct {
	*httptest.Server
	mu       sync.Mutex
	statuses []int
	got      []submission
	arrived  chan struct{}
	hold     chan struct{} // when set, every request waits for it to close
}

func newEndpoint(t *testing.T, statuses ...int) *endpoint {
	e := &endpoint{statuses: statuses, arrived: make(chan struct{}, 100)}
	e.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.hold != nil {
			<-e.hold
		}
		var s submission
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json; charset=utf-8" || json.NewDecoder(r.Body).Decode(&s) != nil {
			t.Errorf("malformed submission: %s %q", r.Method, r.Header.Get("Content-Type"))
		}
		e.mu.Lock()
		e.got = append(e.got, s)
		status := http.StatusOK
		if len(e.statuses) > 0 {
			status = e.statuses[0]
			if len(e.statuses) > 1 {
				e.statuses = e.statuses[1:]
			}
		}
		e.mu.Unlock()
		w.WriteHeader(status)
		e.arrived <- struct{}{}
	}))
	t.Cleanup(e.Close)
	return e
}

func (e *endpoint) wait(t *testing.T, n int) []submission {
	t.Helper()
	for range n {
		select {
		case <-e.arrived:
		case <-time.After(5 * time.Second):
			t.Fatalf("waited for %d submissions, got %d", n, len(e.submissions()))
		}
	}
	return e.submissions()
}

func (e *endpoint) submissions() []submission {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]submission(nil), e.got...)
}

func (e *endpoint) none(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-e.arrived:
		t.Errorf("unexpected submission: %+v", e.submissions())
	case <-time.After(within):
	}
}

func site(t *testing.T, dev bool, opts indexnow.Options, config map[string]json.RawMessage) *collage.App {
	t.Helper()
	app, err := collage.New(&collage.Config{
		DevMode:      dev,
		Server:       collage.ServerConfig{Host: "localhost", Port: 3000},
		Template:     collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>page</p>`)}}, Root: "t"},
		Cache:        collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Plugins:      []collage.Plugin{indexnow.New(opts)},
		PluginConfig: config,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		page := collage.NewPage(name).WithContent(collage.NewFragment(name, "p.html").Build()).
			WithPath("en", "/"+name).Static().WithDependency("posts", "tag-"+name).Build()
		if err := app.RegisterPage(page); err != nil {
			t.Fatal(err)
		}
	}
	api := collage.NewPage("api").WithContent(collage.NewFragment("api", "p.html").Build()).
		WithPath("en", "/api/x").Static().WithDependency("posts").Build()
	if err := app.RegisterPage(api); err != nil {
		t.Fatal(err)
	}
	return app
}

// warm caches every page, so an invalidation has something to drop.
func warm(t *testing.T, app *collage.App) {
	t.Helper()
	for _, path := range []string{"/a", "/b", "/c", "/d", "/e", "/api/x"} {
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", path, rec.Code)
		}
	}
}

func options(e *endpoint) indexnow.Options {
	return indexnow.Options{Key: key, BaseURL: "https://example.com/", Endpoint: e.URL, Window: 50 * time.Millisecond, Backoff: 5 * time.Millisecond, Exclude: []string{"/api/"}}
}

func shutdown(t *testing.T, app *collage.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown = %v", err)
	}
}

// Invalidations within one window are one request, with absolute URLs, the key,
// and where the key is.
func TestBatchedSubmission(t *testing.T) {
	e := newEndpoint(t)
	app := site(t, false, options(e), nil)
	warm(t, app)
	ctx := context.Background()
	_ = app.InvalidateTags(ctx, "tag-a")
	_ = app.InvalidateTags(ctx, "tag-b")
	got := e.wait(t, 1)
	e.none(t, 150*time.Millisecond)
	want := submission{Host: "example.com", Key: key, KeyLocation: "https://example.com/" + key + ".txt",
		URLList: []string{"https://example.com/a", "https://example.com/b"}}
	if len(got) != 1 || got[0].Host != want.Host || got[0].Key != want.Key || got[0].KeyLocation != want.KeyLocation ||
		strings.Join(got[0].URLList, " ") != strings.Join(want.URLList, " ") {
		t.Errorf("sent %+v, want %+v", got, want)
	}
	shutdown(t, app)
}

// The key is served where keyLocation says.
func TestKeyFile(t *testing.T) {
	app := site(t, false, options(newEndpoint(t)), nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/"+key+".txt", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != key || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Errorf("key file = %d %q %q", rec.Code, rec.Body.String(), rec.Header().Get("Content-Type"))
	}

	// A static build writes it too.
	out := t.TempDir()
	b, err := collage.NewBuilder(site(t, false, options(newEndpoint(t)), nil), collage.BuildOptions{OutDir: out})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Build(context.Background()); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(out, key+".txt")); err != nil || string(body) != key {
		t.Errorf("built key file = %q, %v", body, err)
	}
}

// A busy endpoint is tried again; a refusal is not.
func TestRetries(t *testing.T) {
	e := newEndpoint(t, http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusAccepted)
	app := site(t, false, options(e), nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-c")
	got := e.wait(t, 3)
	e.none(t, 100*time.Millisecond)
	for _, s := range got {
		if strings.Join(s.URLList, " ") != "https://example.com/c" {
			t.Errorf("retry sent %v", s.URLList)
		}
	}
	shutdown(t, app)

	refused := newEndpoint(t, http.StatusForbidden)
	app = site(t, false, options(refused), nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-c")
	refused.wait(t, 1)
	refused.none(t, 100*time.Millisecond)
	shutdown(t, app)

	// Retries run out.
	down := newEndpoint(t, http.StatusBadGateway)
	opts := options(down)
	opts.Retries = 2
	app = site(t, false, opts, nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-c")
	down.wait(t, 3)
	down.none(t, 100*time.Millisecond)
}

// No request carries more than BatchSize URLs.
func TestBatchSize(t *testing.T) {
	e := newEndpoint(t)
	opts := options(e)
	opts.BatchSize = 2
	app := site(t, false, opts, nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "posts")
	got := e.wait(t, 3)
	var all []string
	for _, s := range got {
		if len(s.URLList) > 2 {
			t.Errorf("a request carries %d URLs", len(s.URLList))
		}
		all = append(all, s.URLList...)
	}
	sort.Strings(all)
	if want := "https://example.com/a https://example.com/b https://example.com/c https://example.com/d https://example.com/e"; strings.Join(all, " ") != want {
		t.Errorf("sent %v, want %s (and nothing excluded)", all, want)
	}
	shutdown(t, app)
}

// Invalidating never waits on the endpoint.
func TestNeverBlocks(t *testing.T) {
	e := newEndpoint(t)
	e.hold = make(chan struct{})
	opts := options(e)
	opts.Window = time.Millisecond
	app := site(t, false, opts, nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-a")
	time.Sleep(50 * time.Millisecond) // the first request is now held by the endpoint
	warm(t, app)
	start := time.Now()
	for range 20 {
		_ = app.InvalidateTags(context.Background(), "posts")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("invalidating took %v while the endpoint hung", d)
	}
	close(e.hold)
	shutdown(t, app)
}

// What is waiting when the application stops is sent before it does.
func TestShutdownFlushes(t *testing.T) {
	e := newEndpoint(t)
	opts := options(e)
	opts.Window = time.Hour
	app := site(t, false, opts, nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-d")
	e.none(t, 50*time.Millisecond)
	shutdown(t, app)
	if got := e.submissions(); len(got) != 1 || strings.Join(got[0].URLList, " ") != "https://example.com/d" {
		t.Errorf("sent on shutdown: %+v", got)
	}
}

// A development server tells nobody, unless asked to.
func TestDevelopment(t *testing.T) {
	e := newEndpoint(t)
	app := site(t, true, options(e), nil)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-a")
	e.none(t, 200*time.Millisecond)
	shutdown(t, app)

	config := map[string]json.RawMessage{indexnow.Name: json.RawMessage(`{"inDevelopment":true,"window":"20ms"}`)}
	opts := options(e)
	opts.Window = time.Hour
	app = site(t, true, opts, config)
	warm(t, app)
	_ = app.InvalidateTags(context.Background(), "tag-a")
	e.wait(t, 1)
	shutdown(t, app)
}

// With no BaseURL of its own, the plugin falls back to the application's
// Config.BaseURL: the host, the key location and the submitted URLs are all on
// that origin.
func TestIndexNow_FallsBackToConfigBaseURL(t *testing.T) {
	e := newEndpoint(t)
	opts := options(e)
	opts.BaseURL = "" // fall back to Config.BaseURL
	app, err := collage.New(&collage.Config{
		Server:   collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>page</p>`)}}, Root: "t"},
		Cache:    collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		BaseURL:  "https://fromconfig.example",
		Plugins:  []collage.Plugin{indexnow.New(opts)}, // no BaseURL of its own
	})
	if err != nil {
		t.Fatal(err)
	}
	page := collage.NewPage("a").WithContent(collage.NewFragment("a", "p.html").Build()).
		WithPath("en", "/a").Static().WithDependency("posts", "tag-a").Build()
	if err := app.RegisterPage(page); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/a", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/a = %d, want 200", rec.Code)
	}
	_ = app.InvalidateTags(context.Background(), "tag-a")
	got := e.wait(t, 1)
	want := submission{Host: "fromconfig.example", Key: key, KeyLocation: "https://fromconfig.example/" + key + ".txt",
		URLList: []string{"https://fromconfig.example/a"}}
	if len(got) != 1 || got[0].Host != want.Host || got[0].KeyLocation != want.KeyLocation ||
		strings.Join(got[0].URLList, " ") != strings.Join(want.URLList, " ") {
		t.Errorf("sent %+v, want %+v (on the app's Config.BaseURL)", got, want)
	}
	shutdown(t, app)
}

// A misconfigured plugin does not start.
func TestRefusals(t *testing.T) {
	e := newEndpoint(t)
	for name, mutate := range map[string]func(*indexnow.Options){
		"no key":        func(o *indexnow.Options) { o.Key = "" },
		"short key":     func(o *indexnow.Options) { o.Key = "abc1234" },
		"long key":      func(o *indexnow.Options) { o.Key = strings.Repeat("a", 129) },
		"key character": func(o *indexnow.Options) { o.Key = "abcd_1234" },
		"no base":       func(o *indexnow.Options) { o.BaseURL = "" },
		"base path":     func(o *indexnow.Options) { o.BaseURL = "https://example.com/blog" },
		"base scheme":   func(o *indexnow.Options) { o.BaseURL = "example.com" },
		"endpoint":      func(o *indexnow.Options) { o.Endpoint = "ftp://x" },
		"window":        func(o *indexnow.Options) { o.WindowString = "soon" },
	} {
		opts := options(e)
		mutate(&opts)
		rec := httptest.NewRecorder()
		site(t, false, opts, nil).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/a", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: %d, want 503", name, rec.Code)
		}
	}
}

// origins resolves two hosts, as elagoht/tenant would.
type origins struct{}

func (origins) Name() string                             { return "test/origins" }
func (origins) Version() string                          { return "0" }
func (origins) Init(context.Context, collage.Host) error { return nil }
func (origins) Shutdown(context.Context) error           { return nil }
func (origins) Origin(_ context.Context, host string) (string, bool) {
	switch host {
	case "a.test":
		return "https://a.example", true
	case "b.test":
		return "https://b.example", true
	}
	return "", false
}

// Without a BaseURL, each dropped entry is absolute against its own host's origin,
// one submission per origin, and a path two hosts of one origin share is sent once.
func TestSubmission_PerOrigin(t *testing.T) {
	e := newEndpoint(t)
	app, err := collage.New(&collage.Config{
		Server:   collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>page</p>`)}}, Root: "t"},
		Cache:    collage.CacheConfig{Enabled: true, Type: "memory", DefaultTTL: time.Hour},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Plugins: []collage.Plugin{origins{}, indexnow.New(indexnow.Options{
			Key: key, Endpoint: e.URL, Window: 50 * time.Millisecond, Backoff: 5 * time.Millisecond,
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	page := collage.NewPage("a").WithContent(collage.NewFragment("a", "p.html").Build()).
		WithPath("en", "/a").Static().WithDependency("posts").Build()
	if err := app.RegisterPage(page); err != nil {
		t.Fatal(err)
	}
	for _, url := range []string{"http://a.test/a", "http://b.test/a", "http://A.test:80/a"} {
		app.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, url, nil))
	}
	_ = app.InvalidateTags(context.Background(), "posts")
	got := e.wait(t, 2)
	e.none(t, 150*time.Millisecond)
	sort.Slice(got, func(i, j int) bool { return got[i].Host < got[j].Host })
	want := []submission{
		{Host: "a.example", Key: key, KeyLocation: "https://a.example/" + key + ".txt", URLList: []string{"https://a.example/a"}},
		{Host: "b.example", Key: key, KeyLocation: "https://b.example/" + key + ".txt", URLList: []string{"https://b.example/a"}},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("sent %+v, want %+v", got, want)
	}
	shutdown(t, app)
}

// With no BaseURL of its own, none in Config and no resolver, the application
// does not start: there is no origin to tell IndexNow.
func TestNoOrigin_StartFails(t *testing.T) {
	app, err := collage.New(&collage.Config{
		Server:   collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>page</p>`)}}, Root: "t"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Plugins:  []collage.Plugin{indexnow.New(indexnow.Options{Key: key})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); !errors.Is(err, indexnow.ErrNoBaseURL) {
		t.Fatalf("Start() = %v, want an error wrapping ErrNoBaseURL", err)
	}
}

// countingOrigins resolves every host to https://<host>, counting the calls; for
// "reenter.test" it invalidates again from inside the resolver, as a resolver
// that refreshes its own records might.
type countingOrigins struct {
	calls  atomic.Int32
	target collage.CacheInvalidateHook
}

func (*countingOrigins) Name() string                             { return "test/counting" }
func (*countingOrigins) Version() string                          { return "0" }
func (*countingOrigins) Init(context.Context, collage.Host) error { return nil }
func (*countingOrigins) Shutdown(context.Context) error           { return nil }
func (o *countingOrigins) Origin(ctx context.Context, host string) (string, bool) {
	o.calls.Add(1)
	if host == "reenter.test" {
		_ = o.target.OnCacheInvalidate(ctx, &collage.CacheInvalidateEvent{
			Entries: []collage.InvalidatedEntry{{Host: "a.test", Path: "/inner"}}})
	}
	return "https://" + host, true
}

func countingSite(t *testing.T, e *endpoint) (*countingOrigins, *indexnow.Plugin) {
	t.Helper()
	o := &countingOrigins{}
	p := indexnow.New(indexnow.Options{Key: key, Endpoint: e.URL, Window: 50 * time.Millisecond, Backoff: 5 * time.Millisecond})
	o.target = p
	app, err := collage.New(&collage.Config{
		Server:   collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{"t/p.html": {Data: []byte(`<p>page</p>`)}}, Root: "t"},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Plugins:  []collage.Plugin{o, p},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdown(t, app) })
	o.calls.Store(0)
	return o, p
}

// Each distinct host is resolved once per invalidation, however many of its
// entries were dropped.
func TestSubmission_ResolvesEachHostOnce(t *testing.T) {
	e := newEndpoint(t)
	o, p := countingSite(t, e)
	err := p.OnCacheInvalidate(context.Background(), &collage.CacheInvalidateEvent{Entries: []collage.InvalidatedEntry{
		{Host: "a.test", Path: "/1"}, {Host: "a.test", Path: "/2"}, {Host: "a.test", Path: "/3"}, {Host: "b.test", Path: "/1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if n := o.calls.Load(); n != 2 {
		t.Errorf("resolver called %d times, want 2 (once per host)", n)
	}
	e.wait(t, 2)
}

// The resolver runs outside the plugin's lock: one that invalidates again from
// inside does not deadlock.
func TestSubmission_ResolverRunsOutsideTheLock(t *testing.T) {
	e := newEndpoint(t)
	_, p := countingSite(t, e)
	done := make(chan error, 1)
	go func() {
		done <- p.OnCacheInvalidate(context.Background(), &collage.CacheInvalidateEvent{
			Entries: []collage.InvalidatedEntry{{Host: "reenter.test", Path: "/outer"}}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnCacheInvalidate deadlocked: the resolver ran under the plugin's lock")
	}
	got := e.wait(t, 2)
	sort.Slice(got, func(i, j int) bool { return got[i].Host < got[j].Host })
	if len(got) != 2 || fmt.Sprint(got[0].URLList) != "[https://a.test/inner]" || fmt.Sprint(got[1].URLList) != "[https://reenter.test/outer]" {
		t.Errorf("sent %+v", got)
	}
}
