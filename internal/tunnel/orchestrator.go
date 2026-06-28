package tunnel

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	PinggyUser           string
	PinggyTarget         string
	PinggySSHPort        int
	CFAccountID          string
	CFAPIToken           string
	CFWorkerName         string
	CFWorkerTemplatePath string
	Lifetime             time.Duration
	MaxRetries           int
	RetryBaseDelay       time.Duration
}

// =============================================================================
// BACKOFF
// =============================================================================

func backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := base * (1 << attempt)
	if delay > max || attempt > 30 {
		delay = max
	}
	jitter := time.Duration(float64(delay) * (0.75 + 0.5*rand.Float64()))
	return jitter
}

func retryWithBackoff(ctx context.Context, max int, base, maxDelay time.Duration, op func() error) error {
	var err error
	for i := 0; i <= max; i++ {
		if i > 0 {
			delay := backoff(i, base, maxDelay)
			log.Printf("[tunnel] reintento %d/%d tras %v", i, max, delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		if err = op(); err == nil {
			return nil
		}
		log.Printf("[tunnel] intento %d falló: %v", i+1, err)
	}
	return fmt.Errorf("agotados %d reintentos: %w", max, err)
}

// =============================================================================
// PINGGY
// =============================================================================

var urlRegex = regexp.MustCompile(`https?://[a-zA-Z0-9\-]+\.pinggy\.[a-z]+`)

type Tunnel struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	url      string
	urlFound chan string
	mu       sync.Mutex
	done     chan struct{}
}

func startTunnel(ctx context.Context, cfg Config) (*Tunnel, error) {
	t := &Tunnel{
		urlFound: make(chan string, 1),
		done:     make(chan struct{}),
	}

	tunnelCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	sshArgs := []string{
		"-p", fmt.Sprintf("%d", cfg.PinggySSHPort),
		"-R" + cfg.PinggyTarget,
		"-o", "StrictHostKeyChecking=no",
		"-o", "ServerAliveInterval=30",
		"-o", "ServerAliveCountMax=3",
		"-o", "LogLevel=ERROR",
		cfg.PinggyUser,
	}

	if key := os.Getenv("PINGGY_SSH_KEY"); key != "" {
		sshArgs = append([]string{"-i", key}, sshArgs...)
	}

	t.cmd = exec.CommandContext(tunnelCtx, "ssh", sshArgs...)

	stdout, _ := t.cmd.StdoutPipe()
	stderr, _ := t.cmd.StderrPipe()

	if err := t.cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start ssh: %w", err)
	}

	go t.scan(stdout, "stdout")
	go t.scan(stderr, "stderr")

	select {
	case url := <-t.urlFound:
		t.mu.Lock()
		t.url = url
		t.mu.Unlock()
		log.Printf("[tunnel] ✅ Túnel activo: %s", url)
		go t.wait()
		return t, nil
	case <-time.After(45 * time.Second):
		t.Stop()
		return nil, fmt.Errorf("timeout esperando URL de Pinggy")
	case <-tunnelCtx.Done():
		t.Stop()
		return nil, ctx.Err()
	}
}

func (t *Tunnel) scan(r io.Reader, label string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if matches := urlRegex.FindString(line); matches != "" {
			select {
			case t.urlFound <- matches:
			default:
			}
		}
	}
}

func (t *Tunnel) wait() {
	if t.cmd != nil {
		t.cmd.Wait()
	}
	close(t.done)
}

func (t *Tunnel) Stop() {
	t.cancel()
	if t.cmd != nil && t.cmd.Process != nil {
		t.cmd.Process.Signal(syscall.SIGTERM)
	}
	<-t.done
}

func (t *Tunnel) URL() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.url
}

// =============================================================================
// CLOUDFLARE
// =============================================================================

type cfClient struct {
	http    *http.Client
	token   string
	account string
}

func newCFClient(token, account string) *cfClient {
	return &cfClient{
		http:    &http.Client{Timeout: 40 * time.Second},
		token:   token,
		account: account,
	}
}

func (c *cfClient) uploadWorker(ctx context.Context, name, content string) error {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s", c.account, name)

	req, _ := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(content))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/javascript")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Cloudflare error %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("[cloudflare] ✅ Worker '%s' actualizado correctamente", name)
	return nil
}

// =============================================================================
// ORQUESTADOR PRINCIPAL (VERSIÓN MEJORADA)
// =============================================================================

func Run(ctx context.Context, cfg Config) error {
	var templateContent []byte
	var err error

	if cfg.CFWorkerTemplatePath != "" {
		templateContent, err = os.ReadFile(cfg.CFWorkerTemplatePath)
		if err != nil {
			return fmt.Errorf("no se pudo leer el template del worker: %w", err)
		}
		log.Printf("[tunnel] Usando template: %s", cfg.CFWorkerTemplatePath)
	} else {
		// Template mínimo de respaldo
		templateContent = []byte(`const UPSTREAM_ORIGIN = '__PINGGY_URL__';` + "\n" + `addEventListener('fetch', event => { event.respondWith(handleRequest(event.request)); }); async function handleRequest(request) { const url = new URL(request.url); const target = new URL(url.pathname + url.search, UPSTREAM_ORIGIN); return fetch(target, request); }`)
	}

	cf := newCFClient(cfg.CFAPIToken, cfg.CFAccountID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var tunnel *Tunnel
		err := retryWithBackoff(ctx, cfg.MaxRetries, cfg.RetryBaseDelay, 5*time.Minute, func() error {
			var e error
			tunnel, e = startTunnel(ctx, cfg)
			return e
		})
		if err != nil {
			log.Printf("[tunnel] Error iniciando túnel: %v. Reintentando en 60s...", err)
			time.Sleep(60 * time.Second)
			continue
		}

		pinggyURL := tunnel.URL()
		if pinggyURL == "" {
			tunnel.Stop()
			continue
		}

		// Reemplazar placeholder en el worker
		workerCode := strings.ReplaceAll(string(templateContent), "__PINGGY_URL__", pinggyURL)
		workerCode = strings.ReplaceAll(workerCode, "{{PINGGY_URL}}", pinggyURL)

		err = retryWithBackoff(ctx, cfg.MaxRetries, cfg.RetryBaseDelay, 5*time.Minute, func() error {
			return cf.uploadWorker(ctx, cfg.CFWorkerName, workerCode)
		})
		if err != nil {
			log.Printf("[tunnel] Error subiendo worker: %v", err)
			tunnel.Stop()
			continue
		}

		log.Printf("[tunnel] ✅ Worker actualizado con URL: %s", pinggyURL)

		timer := time.NewTimer(cfg.Lifetime)
		select {
		case <-ctx.Done():
			timer.Stop()
			tunnel.Stop()
			return ctx.Err()
		case <-timer.C:
			log.Println("[tunnel] Renovando túnel...")
			tunnel.Stop()
		case <-tunnel.Done():
			timer.Stop()
			log.Println("[tunnel] Túnel caído. Reconectando...")
		}
	}
}
