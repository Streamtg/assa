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

// Config agrupa todo lo necesario para el túnel y Cloudflare.
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
// BACKOFF EXPONENCIAL
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
// PINGGY TUNNEL
// =============================================================================

var urlRegex = regexp.MustCompile(`https?://[a-zA-Z0-9\-]+\.a\.pinggy\.[a-z]+`)

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
		"-o", "UserKnownHostsFile=/dev/null",
		cfg.PinggyUser,
	}

	if key := os.Getenv("PINGGY_SSH_KEY"); key != "" {
		sshArgs = append([]string{"-i", key}, sshArgs...)
	}

	t.cmd = exec.CommandContext(tunnelCtx, "ssh", sshArgs...)
	stdout, err := t.cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := t.cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

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
		log.Printf("[tunnel] activo: %s", url)
		go t.wait()
		return t, nil
	case <-time.After(45 * time.Second):
		t.Stop()
		return nil, fmt.Errorf("timeout esperando URL de pinggy")
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
		if strings.Contains(line, "pinggy") || strings.Contains(line, "http") || urlRegex.MatchString(line) {
			log.Printf("[tunnel:%s] %s", label, line)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[tunnel:%s] scanner error: %v", label, err)
	}
}

func (t *Tunnel) wait() {
	if t.cmd == nil {
		return
	}
	err := t.cmd.Wait()
	close(t.done)
	if err != nil && t.cmd.ProcessState != nil && !t.cmd.ProcessState.Success() {
		log.Printf("[tunnel] ssh terminó: %v", err)
	}
}

func (t *Tunnel) Stop() {
	t.cancel()
	if t.cmd != nil && t.cmd.Process != nil {
		_ = t.cmd.Process.Signal(syscall.SIGTERM)
		go func() {
			<-time.After(5 * time.Second)
			if t.cmd != nil && t.cmd.Process != nil {
				_ = t.cmd.Process.Kill()
			}
		}()
	}
	<-t.done
}

func (t *Tunnel) URL() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.url
}

func (t *Tunnel) Done() <-chan struct{} {
	return t.done
}

// =============================================================================
// CLOUDFLARE API
// =============================================================================

type cfClient struct {
	http    *http.Client
	token   string
	account string
}

func newCFClient(token, account string) *cfClient {
	return &cfClient{
		http:    &http.Client{Timeout: 30 * time.Second},
		token:   token,
		account: account,
	}
}

func (c *cfClient) uploadWorker(ctx context.Context, scriptName, content string) error {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s", c.account, scriptName)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(content))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/javascript")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var envelope struct {
		Success bool `json:"success"`
		Errors  []struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil {
		if !envelope.Success {
			return fmt.Errorf("cf api errors: %+v", envelope.Errors)
		}
	}

	log.Printf("[cloudflare] worker '%s' actualizado (HTTP %d)", scriptName, resp.StatusCode)
	return nil
}

// =============================================================================
// ORQUESTADOR PRINCIPAL
// =============================================================================

func Run(ctx context.Context, cfg Config) error {
	var template []byte
	var err error

	if cfg.CFWorkerTemplatePath != "" {
		template, err = os.ReadFile(cfg.CFWorkerTemplatePath)
		if err != nil {
			return fmt.Errorf("read worker template: %w", err)
		}
	} else {
		template = []byte(`const ORIGIN_URL = '__PINGGY_URL__';
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const target = new URL(url.pathname + url.search, ORIGIN_URL);
    const modifiedRequest = new Request(target, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: request.redirect,
    });
    try {
      return await fetch(modifiedRequest);
    } catch (err) {
      return new Response('Origin error: ' + err.message, {status: 502});
    }
  }
};`)
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
			var err error
			tunnel, err = startTunnel(ctx, cfg)
			return err
		})
		if err != nil {
			log.Printf("[orchestrator] fallo al iniciar túnel: %v. Esperando 1m...", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Minute):
				continue
			}
		}

		url := tunnel.URL()
		if url == "" {
			log.Println("[orchestrator] URL vacía, reiniciando ciclo...")
			tunnel.Stop()
			continue
		}

		code := strings.ReplaceAll(string(template), "__PINGGY_URL__", url)
		code = strings.ReplaceAll(code, "{{PINGGY_URL}}", url)
		code = strings.ReplaceAll(code, "${PINGGY_URL}", url)

		err = retryWithBackoff(ctx, cfg.MaxRetries, cfg.RetryBaseDelay, 5*time.Minute, func() error {
			return cf.uploadWorker(ctx, cfg.CFWorkerName, code)
		})
		if err != nil {
			log.Printf("[orchestrator] fallo Cloudflare: %v. Reiniciando túnel...", err)
			tunnel.Stop()
			continue
		}

		timer := time.NewTimer(cfg.Lifetime)
		select {
		case <-ctx.Done():
			timer.Stop()
			tunnel.Stop()
			return ctx.Err()
		case <-timer.C:
			log.Println("[orchestrator] renovación de túnel (60 min). Reiniciando...")
			tunnel.Stop()
			<-time.After(3 * time.Second)
		case <-tunnel.Done():
			timer.Stop()
			log.Println("[orchestrator] túnel caído inesperadamente. Reconectando...")
		}
	}
}
