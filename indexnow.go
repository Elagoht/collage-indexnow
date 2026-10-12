// Package indexnow is a collage plugin that tells search engines which URLs
// changed, through the IndexNow protocol: Bing, Yandex, Seznam, Naver and the
// others that share what one of them is told.
//
//	app, err := collage.New(&collage.Config{
//		BaseURL: "https://example.com",
//		Plugins: []collage.Plugin{indexnow.New(indexnow.Options{
//			Key: "4f1c2e9a7b3d4c8e",
//		})},
//	})
//
// URLs are absolute against the origin collage names for the host each was
// cached under: an OriginResolver plugin's, else Config.BaseURL.
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
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Elagoht/collage/pkg/collage"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/indexnow"

// defaultEndpoint is IndexNow's shared endpoint, which passes what it is told on
// to every participating search engine.
const defaultEndpoint = "https://api.indexnow.org/indexnow"

// MaxBatch is the most URLs IndexNow takes in one request.
const MaxBatch = 10000

// Options configures the plugin.
type Options struct {
	// Key proves the site is yours: 8 to 128 letters, digits and dashes, served at
	// /<key>.txt. Required. Any value works; keep it the same across deployments.
	Key string `json:"key"`
	// Endpoint is where the URLs are posted. Default IndexNow's shared endpoint,
	// "https://api.indexnow.org/indexnow", which passes what it is told on to every
	// participating search engine; a search engine's own,
	// "https://www.bing.com/indexnow", works the same.
	Endpoint string `json:"endpoint"`
	// Window is how long URLs are collected after the first one before they are
	// sent together: "10s", "1m". Default ten seconds.
	Window collage.Duration `json:"window"`
	// BatchSize caps how many URLs one request carries. Default and at most
	// MaxBatch; negative is an error.
	BatchSize int `json:"batchSize"`
	// Retries is how many more times a request is tried when the endpoint answers
	// 429 or 5xx, or cannot be reached. Default 3; negative is an error.
	Retries int `json:"retries"`
	// Backoff is how long the first retry waits, doubled for each one after it,
	// unless the endpoint says otherwise with Retry-After: "2s". Default two
	// seconds.
	Backoff collage.Duration `json:"backoff"`
	// Skip are request path prefixes never sent: "/api", "/search". A prefix
	// covers the path itself and everything under it, with or without a trailing
	// "/".
	Skip []string `json:"skip"`
	// InDevelopment sends from a development server too, which is otherwise
	// silent: what changes on a laptop is not news to a search engine.
	InDevelopment bool `json:"inDevelopment"`
	// HTTPClient sends the requests. Default one with a thirty-second timeout.
	HTTPClient *http.Client `json:"-"`
}

var keyPattern = regexp.MustCompile(`^[a-zA-Z0-9-]{8,128}$`)

// Plugin sends the URLs.
type Plugin struct {
	opts    Options
	log     *slog.Logger
	enabled bool
	base    string       // Config.BaseURL without its trailing slash; empty when origins decide
	origins collage.Host // set only when origins decide per host
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
func (p *Plugin) Version() string { return "0.3.0" }

// ErrInvalidKey is returned by Init for a missing or malformed key.
var ErrInvalidKey = errors.New("indexnow: Key must be 8 to 128 letters, digits and dashes")

// ErrNoBaseURL is returned by Init when the application has no origin to make
// URLs absolute against: no Config.BaseURL and no OriginResolver plugin.
var ErrNoBaseURL = errors.New("indexnow: no origin: set Config.BaseURL (BASE_URL) or register an OriginResolver plugin; IndexNow is told absolute URLs")

// Init reads the configuration, refuses what is wrong with it, and serves the key.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	opts, err := collage.PluginConfig(host, p.opts)
	if err != nil {
		return err
	}
	p.opts = opts
	o := &p.opts
	if !keyPattern.MatchString(o.Key) {
		return ErrInvalidKey
	}
	// Each entry is absolute against the origin collage names for its host: a
	// resolver plugin's, else Config.BaseURL.
	switch {
	case host.MultiOrigin():
		p.origins = host
	case host.BaseURL() != "":
		p.base = strings.TrimSuffix(host.BaseURL(), "/")
	default:
		return ErrNoBaseURL
	}
	if o.Endpoint == "" {
		o.Endpoint = defaultEndpoint
	}
	if u, err := url.Parse(o.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("indexnow: endpoint %q is not an http(s) URL", o.Endpoint)
	}
	if o.Window < 0 || o.Backoff < 0 {
		return fmt.Errorf("indexnow: window and backoff must not be negative")
	}
	if o.BatchSize < 0 {
		return fmt.Errorf("indexnow: batchSize %d is negative", o.BatchSize)
	}
	if o.Retries < 0 {
		return fmt.Errorf("indexnow: retries %d is negative", o.Retries)
	}
	if o.Window == 0 {
		o.Window = collage.Duration(10 * time.Second)
	}
	if o.BatchSize == 0 || o.BatchSize > MaxBatch {
		o.BatchSize = MaxBatch
	}
	if o.Retries == 0 {
		o.Retries = 3
	}
	if o.Backoff == 0 {
		o.Backoff = collage.Duration(2 * time.Second)
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	p.log = host.Logger()
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
func (p *Plugin) OnCacheInvalidate(ctx context.Context, ev *collage.CacheInvalidateEvent) error {
	if !p.enabled || len(ev.Entries) == 0 {
		return nil
	}
	// Origins are named before the lock, once per host: OriginFor may be a
	// resolver's I/O, which must hold up neither a flush nor another
	// invalidation, and a resolver that invalidates in turn would deadlock.
	urls := make([]string, 0, len(ev.Entries))
	byHost := make(map[string]string)
	for _, entry := range ev.Entries {
		if p.skipped(entry.Path) {
			continue
		}
		origin := p.base
		if origin == "" && p.origins != nil {
			var named bool
			if origin, named = byHost[entry.Host]; !named {
				origin = p.origins.OriginFor(ctx, entry.Host)
				byHost[entry.Host] = origin
			}
		}
		if origin == "" {
			continue
		}
		urls = append(urls, origin+entry.Path)
	}
	if len(urls) == 0 {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	for _, u := range urls {
		p.pending[u] = struct{}{}
	}
	p.start.Do(p.run)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default: // already woken; the batch it opened will carry these too
	}
	return nil
}

func (p *Plugin) skipped(path string) bool {
	if path == p.keyPath || strings.HasPrefix(path, "/_collage/") || !strings.HasPrefix(path, "/") {
		return true
	}
	for _, prefix := range p.opts.Skip {
		if under(path, prefix) {
			return true
		}
	}
	return false
}

// under reports whether path is prefix or below it. A trailing "/" on prefix
// changes nothing: "/api" and "/api/" both cover "/api" and "/api/x", and neither
// covers "/apix".
func under(path, prefix string) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
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
			timer := time.NewTimer(time.Duration(p.opts.Window))
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

	// IndexNow takes one host per submission: group by origin, in a stable order.
	byOrigin := make(map[string][]string)
	for _, u := range urls {
		byOrigin[originOf(u)] = append(byOrigin[originOf(u)], u)
	}
	origins := slices.Sorted(maps.Keys(byOrigin))

	var errs []error
	for i, origin := range origins {
		list := byOrigin[origin]
		for start := 0; start < len(list); start += p.opts.BatchSize {
			batch := list[start:min(start+p.opts.BatchSize, len(list))]
			err := p.send(ctx, interrupt, origin, batch)
			if errors.Is(err, errInterrupted) {
				p.requeue(list[start:])
				for _, rest := range origins[i+1:] {
					p.requeue(byOrigin[rest])
				}
				return err
			}
			if err != nil {
				p.log.Error("indexnow: URLs not submitted", "origin", origin, "count", len(batch), "error", err)
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// originOf is u's scheme and host: the origin it was queued under.
func originOf(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

// hostname is origin's host, as IndexNow is told it.
func hostname(origin string) string {
	parsed, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
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

// send posts one batch, all of one origin, trying again when the endpoint is busy or unreachable.
func (p *Plugin) send(ctx context.Context, interrupt <-chan struct{}, origin string, batch []string) error {
	body, err := json.Marshal(submission{
		Host:        hostname(origin),
		Key:         p.opts.Key,
		KeyLocation: origin + p.keyPath,
		URLList:     batch,
	})
	if err != nil {
		return err
	}
	wait := time.Duration(p.opts.Backoff)
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
	resp, err := p.opts.HTTPClient.Do(req)
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
