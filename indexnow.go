// Package indexnow is a collage plugin that tells search engines which URLs
// changed, through the IndexNow protocol: Bing, Yandex, Seznam, Naver and the
// others that share what one of them is told.
//
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{indexnow.New(indexnow.Options{
//			Key:     "4f1c2e9a7b3d4c8e",
//			BaseURL: "https://example.com",
//		})},
//	})
//
// What changed is what the cache let go of: when a tag is invalidated, collage
// names the paths of the pages and documents it dropped, and those are the URLs
// whose content is about to be different. The plugin collects them for a few
// seconds, so a burst of invalidations is one request, and posts them to the
// endpoint from a goroutine of its own — the code that invalidated never waits on
// a search engine. A busy endpoint (429, 5xx) is tried again, later each time;
// what is still waiting when the application stops is sent before it does.
//
// The protocol proves the site is yours by a key served from the site itself, at
// /<key>.txt; the plugin serves it. A development server tells nobody anything.
package indexnow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Elagoht/collage/pkg/collage"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/indexnow"

// DefaultEndpoint is IndexNow's shared endpoint, which passes what it is told on
// to every participating search engine.
const DefaultEndpoint = "https://api.indexnow.org/indexnow"

// MaxBatch is the most URLs IndexNow takes in one request.
const MaxBatch = 10000

// Options configures the plugin.
type Options struct {
	// Key proves the site is yours: 8 to 128 letters, digits and dashes, served at
	// /<key>.txt. Required. Any value works; keep it the same across deployments.
	Key string `json:"key"`
	// BaseURL is the site's origin, "https://example.com": IndexNow is told
	// absolute URLs, and the application cannot know its own host. Required.
	BaseURL string `json:"baseURL"`
	// Endpoint is where the URLs are posted. Default DefaultEndpoint; a search
	// engine's own, "https://www.bing.com/indexnow", works the same.
	Endpoint string `json:"endpoint"`
	// Window is how long URLs are collected after the first one before they are
	// sent together. Default ten seconds.
	Window time.Duration `json:"-"`
	// WindowString is Window as configuration carries it: "10s", "1m".
	WindowString string `json:"window"`
	// BatchSize caps how many URLs one request carries. Default and at most
	// MaxBatch.
	BatchSize int `json:"batchSize"`
	// Retries is how many more times a request is tried when the endpoint answers
	// 429 or 5xx, or cannot be reached. Default 3; negative tries once.
	Retries int `json:"retries"`
	// Backoff is how long the first retry waits, doubled for each one after it,
	// unless the endpoint says otherwise with Retry-After. Default two seconds.
	Backoff time.Duration `json:"-"`
	// Exclude are path prefixes never sent: "/api/", "/search".
	Exclude []string `json:"exclude"`
	// InDevelopment sends from a development server too, which is otherwise
	// silent: what changes on a laptop is not news to a search engine.
	InDevelopment bool `json:"inDevelopment"`
	// Client sends the requests. Default one with a thirty-second timeout.
	Client *http.Client `json:"-"`
}

var keyPattern = regexp.MustCompile(`^[a-zA-Z0-9-]{8,128}$`)

// Plugin sends the URLs.
type Plugin struct {
	opts    Options
	log     *slog.Logger
	enabled bool
	host    string // the site's host, as IndexNow is told it
	base    string // BaseURL without its trailing slash
	keyPath string

	mu      sync.Mutex
	pending map[string]struct{}

	start   sync.Once
	running bool
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

// The hooks the plugin means to implement; a misspelt method would otherwise be a
// hook that silently never fires.
var (
	_ collage.Plugin              = (*Plugin)(nil)
	_ collage.CacheInvalidateHook = (*Plugin)(nil)
)

// New returns a plugin with opts as its starting point, which the application's
// own configuration is then decoded over.
func New(opts Options) *Plugin { return &Plugin{opts: opts} }

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.1.0" }

// ErrInvalidKey is returned by Init for a missing or malformed key.
var ErrInvalidKey = errors.New("indexnow: Key must be 8 to 128 letters, digits and dashes")

// ErrNoBaseURL is returned by Init without an absolute BaseURL.
var ErrNoBaseURL = errors.New("indexnow: BaseURL is required: IndexNow is told absolute URLs")

// Init reads the configuration, refuses what is wrong with it, and serves the key.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	if err := host.Config(&p.opts); err != nil {
		return err
	}
	o := &p.opts
	if !keyPattern.MatchString(o.Key) {
		return ErrInvalidKey
	}
	base, err := url.Parse(o.BaseURL)
	if o.BaseURL == "" || err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" ||
		(base.Path != "" && base.Path != "/") || base.RawQuery != "" {
		return fmt.Errorf("%w, an origin such as https://example.com; got %q", ErrNoBaseURL, o.BaseURL)
	}
	if o.Endpoint == "" {
		o.Endpoint = DefaultEndpoint
	}
	if u, err := url.Parse(o.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("indexnow: endpoint %q is not an http(s) URL", o.Endpoint)
	}
	if o.WindowString != "" {
		d, err := time.ParseDuration(o.WindowString)
		if err != nil {
			return fmt.Errorf("indexnow: window: %w", err)
		}
		o.Window = d
	}
	if o.Window <= 0 {
		o.Window = 10 * time.Second
	}
	if o.BatchSize <= 0 || o.BatchSize > MaxBatch {
		o.BatchSize = MaxBatch
	}
	if o.Retries == 0 {
		o.Retries = 3
	}
	if o.Backoff <= 0 {
		o.Backoff = 2 * time.Second
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 30 * time.Second}
	}

	p.log = host.Logger()
	p.host = base.Hostname()
	p.base = strings.TrimSuffix(o.BaseURL, "/")
	p.keyPath = "/" + o.Key + ".txt"
	p.enabled = !host.DevMode() || o.InDevelopment
	p.pending = make(map[string]struct{})

	doc := collage.NewDocument(Name, "text/plain; charset=utf-8").
		AtRoot(p.keyPath).
		WithBody([]byte(o.Key)).
		Build()
	if err := host.RegisterDocument(doc); err != nil {
		return fmt.Errorf("indexnow: %w", err)
	}
	return nil
}

// OnCacheInvalidate queues the URLs the invalidation dropped, and returns: the
// sending happens elsewhere, so an invalidation from a request, a webhook or a
// command is never held up by a search engine.
func (p *Plugin) OnCacheInvalidate(_ context.Context, ev *collage.CacheInvalidateEvent) error {
	if !p.enabled || len(ev.Paths) == 0 {
		return nil
	}
	added := false
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	for _, path := range ev.Paths {
		if p.excluded(path) {
			continue
		}
		p.pending[p.base+path] = struct{}{}
		added = true
	}
	if added {
		p.start.Do(p.run)
	}
	p.mu.Unlock()
	if !added {
		return nil
	}
	select {
	case p.wake <- struct{}{}:
	default: // already woken; the batch it opened will carry these too
	}
	return nil
}

func (p *Plugin) excluded(path string) bool {
	if path == p.keyPath || strings.HasPrefix(path, "/_collage/") || !strings.HasPrefix(path, "/") {
		return true
	}
	for _, prefix := range p.opts.Exclude {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// run starts the goroutine that sends: started by the first URL, so a static
// build, a command, or a site that never invalidates anything runs none. It is
// called with mu held, so Shutdown sees it either not started or running.
func (p *Plugin) run() {
	p.wake = make(chan struct{}, 1)
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.running = true
	go func() {
		defer close(p.done)
		for {
			select {
			case <-p.wake:
			case <-p.stop:
				return
			}
			// The window opens with the first URL and is not pushed back by the
			// ones after it: a site invalidating every few seconds would otherwise
			// never send.
			timer := time.NewTimer(p.opts.Window)
			select {
			case <-timer.C:
			case <-p.stop:
				timer.Stop()
				return
			}
			p.flush(p.ctx, p.stop)
		}
	}()
}

// Shutdown stops the sender and sends what is still waiting, for as long as ctx
// allows.
func (p *Plugin) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	running := p.running
	p.running = false
	p.closed = true
	p.mu.Unlock()
	if !running {
		return nil
	}
	close(p.stop)
	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
		<-p.done
	}
	p.cancel()
	return p.flush(ctx, nil)
}

// flush sends every pending URL, in batches. A batch the endpoint refuses is
// logged and dropped; one interrupted — by interrupt closing or ctx ending — goes
// back to wait, so Shutdown's own flush sends it.
func (p *Plugin) flush(ctx context.Context, interrupt <-chan struct{}) error {
	p.mu.Lock()
	urls := make([]string, 0, len(p.pending))
	for u := range p.pending {
		urls = append(urls, u)
	}
	p.pending = make(map[string]struct{})
	p.mu.Unlock()
	sort.Strings(urls)

	var errs []error
	for start := 0; start < len(urls); start += p.opts.BatchSize {
		batch := urls[start:min(start+p.opts.BatchSize, len(urls))]
		err := p.send(ctx, interrupt, batch)
		if errors.Is(err, errInterrupted) {
			p.requeue(urls[start:])
			return err
		}
		if err != nil {
			p.log.Error("indexnow: URLs not submitted", "count", len(batch), "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *Plugin) requeue(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, u := range urls {
		p.pending[u] = struct{}{}
	}
}

var errInterrupted = errors.New("indexnow: interrupted")

type submission struct {
	Host        string   `json:"host"`
	Key         string   `json:"key"`
	KeyLocation string   `json:"keyLocation"`
	URLList     []string `json:"urlList"`
}

// send posts one batch, trying again when the endpoint is busy or unreachable.
func (p *Plugin) send(ctx context.Context, interrupt <-chan struct{}, batch []string) error {
	body, err := json.Marshal(submission{
		Host:        p.host,
		Key:         p.opts.Key,
		KeyLocation: p.base + p.keyPath,
		URLList:     batch,
	})
	if err != nil {
		return err
	}
	wait := p.opts.Backoff
	for attempt := 0; ; attempt++ {
		retryAfter, err := p.post(ctx, body)
		if err == nil {
			p.log.Debug("indexnow: URLs submitted", "count", len(batch))
			return nil
		}
		if ctx.Err() != nil {
			return errInterrupted
		}
		var permanent *permanentError
		if errors.As(err, &permanent) || attempt >= p.opts.Retries {
			return err
		}
		delay := wait
		if retryAfter > 0 {
			delay = retryAfter
		}
		p.log.Warn("indexnow: endpoint busy, trying again", "in", delay, "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-interrupt:
			timer.Stop()
			return errInterrupted
		case <-ctx.Done():
			timer.Stop()
			return errInterrupted
		}
		wait *= 2
	}
}

// permanentError is an answer trying again will not change: a key the endpoint
// could not verify (403), URLs not on the host (422), a malformed request (400).
type permanentError struct{ status int }

func (e *permanentError) Error() string {
	return "indexnow: endpoint refused the submission: " + strconv.Itoa(e.status) + " " + http.StatusText(e.status)
}

// post sends body once, and returns how long the endpoint asked to be left alone
// when it said so.
func (p *Plugin) post(ctx context.Context, body []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.Endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := p.opts.Client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted:
		return 0, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		var after time.Duration
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
			after = time.Duration(s) * time.Second
		}
		return after, fmt.Errorf("indexnow: endpoint answered %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	default:
		return 0, &permanentError{status: resp.StatusCode}
	}
}
