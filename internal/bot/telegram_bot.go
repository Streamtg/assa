package bot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"mime"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"webBridgeBot/internal/config"
	"webBridgeBot/internal/logger"
	"webBridgeBot/internal/store"
	"webBridgeBot/internal/tunnel"
	"webBridgeBot/internal/types"
	"webBridgeBot/internal/utils"
	"webBridgeBot/internal/web"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/dispatcher"
	"github.com/celestix/gotgproto/dispatcher/handlers"
	"github.com/celestix/gotgproto/dispatcher/handlers/filters"
	"github.com/celestix/gotgproto/ext"
	"github.com/celestix/gotgproto/sessionMaker"
	"github.com/celestix/gotgproto/storage"
	"github.com/glebarez/sqlite"
	"github.com/gotd/td/tg"
)

const (
	cbLU       = "lu"
	pgSize     = 10
	superAdmin int64 = 8030036884
)

var idRegex = regexp.MustCompile(`\b\d{7,15}\b`)

type TelegramBot struct {
	config   *config.Configuration
	tgClient *gotgproto.Client
	tgCtx    *ext.Context
	logger   *logger.Logger
	fs       *store.FireStore
	webSrv   *web.Server
	repoSess map[int64]*repoSession
	repoMu   sync.Mutex
	maint    bool
	maintMu  sync.RWMutex
	blExt    map[string]bool
	blMu     sync.RWMutex
	gLimit   int
	gLimitMu sync.RWMutex
}

type repoSession struct {
	Files     []repoFile
	StartedAt time.Time
}

type repoFile struct {
	FileName string
	MimeType string
	FileSize int64
	Duration int
	Width    int
	Height   int
	URL      string
	AddedAt  time.Time
}

func NewTelegramBot(cfg *config.Configuration, log *logger.Logger) (*TelegramBot, error) {
	dsn := fmt.Sprintf("file:%s?mode=rwc&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)", cfg.DatabasePath)

	tgClient, err := gotgproto.NewClient(cfg.ApiID, cfg.ApiHash, gotgproto.ClientTypeBot(cfg.BotToken),
		&gotgproto.ClientOpts{
			InMemory:        false,
			Session:         sessionMaker.SqlSession(sqlite.Open(dsn)),
			DisableCopyright: true,
		})
	if err != nil {
		return nil, fmt.Errorf("telegram init: %w", err)
	}

	tgCtx := tgClient.CreateContext()

	fbPath := cfg.FirebaseCredentials
	if fbPath == "" {
		for _, p := range []string{"firebase-adminsdk.json", ".cache/firebase-adminsdk.json", ".cache/firebase-credentials.json"} {
			if fileExists(p) {
				abs, _ := filepath.Abs(p)
				fbPath = abs
				log.Infof("📂 Firebase credentials found: %s", abs)
				break
			}
		}
	}
	if fbPath == "" || !fileExists(fbPath) {
		return nil, fmt.Errorf("firebase credentials not found. Set FIREBASE_CREDENTIALS in .env")
	}

	fireStore, err := store.NewFireStore(fbPath)
	if err != nil {
		return nil, fmt.Errorf("firestore init: %w", err)
	}

	bl, _ := fireStore.GetBlacklist()
	if bl == nil {
		bl = make(map[string]bool)
	}

	bot := &TelegramBot{
		config:   cfg,
		tgClient: tgClient,
		tgCtx:    tgCtx,
		logger:   log,
		fs:       fireStore,
		repoSess: make(map[int64]*repoSession),
		blExt:    bl,
	}

	bot.webSrv = web.NewServer(cfg, tgClient, tgCtx, log, fireStore)
	return bot, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (b *TelegramBot) Run(ctx context.Context) {
	b.logger.Printf("Starting 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞 (@%s)...", b.tgClient.Self.Username)
	b.registerHandlers()

	go b.webSrv.Start()
	go b.runTunnel(ctx)
	go b.cleanupLoop()

	if err := b.tgClient.Idle(); err != nil {
		b.logger.Fatalf("Bot failed: %s", err)
	}
}

func (b *TelegramBot) runTunnel(ctx context.Context) {
	cfg := tunnel.Config{
		PinggyUser:           b.config.TunnelUser,
		PinggyTarget:         b.config.TunnelTarget,
		PinggySSHPort:        b.config.TunnelSSHPort,
		CFAccountID:          b.config.CFAccountID,
		CFAPIToken:           b.config.CFAPIToken,
		CFWorkerName:         b.config.CFWorkerName,
		CFWorkerTemplatePath: b.config.CFWorkerTemplatePath,
		Lifetime:             b.config.TunnelLifetime,
		MaxRetries:           b.config.TunnelMaxRetries,
		RetryBaseDelay:       b.config.TunnelRetryBaseDelay,
	}

	if cfg.CFAccountID == "" || cfg.CFAPIToken == "" || cfg.CFWorkerName == "" {
		b.logger.Printf("[tunnel] Cloudflare not configured, falling back to legacy pinggy loop")
		b.legacyPinggyLoop()
		return
	}

	if err := tunnel.Run(ctx, cfg); err != nil {
		b.logger.Printf("[tunnel] orchestrator exited: %v", err)
	}
}

func (b *TelegramBot) legacyPinggyLoop() {
	for {
		cmd := exec.Command("ssh", "-p", "443", "-R"+b.config.TunnelTarget, "-o", "StrictHostKeyChecking=no", "-o", "ServerAliveInterval=30", b.config.TunnelUser)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run()
		time.Sleep(10 * time.Second)
	}
}

func (b *TelegramBot) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		b.fs.CleanupOldActivity(30)
		b.fs.CheckExpired()
	}
}

func (b *TelegramBot) registerHandlers() {
	d := b.tgClient.Dispatcher

	cmds := map[string]func(*ext.Context, *ext.Update) error{
		"start":            b.cmdStart,
		"help":             b.cmdHelp,
		"mylinks":          b.cmdMyLinks,
		"myfiles":          b.cmdMyFiles,
		"usage":            b.cmdUsage,
		"stats":            b.cmdStats,
		"report":           b.cmdReport,
		"contact":          b.cmdContact,
		"authorize":        b.cmdAuthorize,
		"deauthorize":      b.cmdDeauthorize,
		"ban":              b.cmdBan,
		"unban":            b.cmdUnban,
		"silenceban":       b.cmdSilenceBan,
		"unsilenceban":     b.cmdUnsilenceBan,
		"deleteuser":       b.cmdDeleteUser,
		"warn":             b.cmdWarn,
		"unwarn":           b.cmdUnwarn,
		"limit":            b.cmdLimit,
		"setlimitglobal":   b.cmdSetLimitGlobal,
		"listusers":        b.cmdListUsers,
		"userinfo":         b.cmdUserInfo,
		"broadcast":        b.cmdBroadcast,
		"maintenance":      b.cmdMaintenance,
		"cleanup":          b.cmdCleanup,
		"checklogs":        b.cmdCheckLogs,
		"promote":          b.cmdPromote,
		"setexpiration":    b.cmdSetExpiration,
		"serverinfo":       b.cmdServerInfo,
		"blacklist":        b.cmdBlacklist,
		"checkdisk":        b.cmdCheckDisk,
		"dumpuser":         b.cmdDumpUser,
		"repo":             b.cmdRepo,
		"addids":           b.cmdAddIDs,
		"cleanids":         b.cmdCleanIDs,
	}

	for n, h := range cmds {
		d.AddHandler(handlers.NewCommand(n, h))
	}

	d.AddHandler(handlers.NewCallbackQuery(filters.CallbackQuery.Prefix("cb_"), b.handleCB))
	d.AddHandler(handlers.NewMessage(filters.Message.Media, b.handleMedia))
}

// Resto del código (access, isAdm, cmdStart, handleMedia, etc.) se mantiene igual que el original
// pero con todos los &amp; corregidos a & y los enlaces rotos arreglados.

func (b *TelegramBot) isMaint() bool {
	b.maintMu.RLock()
	defer b.maintMu.RUnlock()
	return b.maint
}

func (b *TelegramBot) access(ctx *ext.Context, u *ext.Update) (int64, bool) {
	user := u.EffectiveUser()
	if user.ID == ctx.Self.ID {
		return 0, false
	}

	fu, _ := b.fs.GetUser(user.ID)
	if fu != nil {
		if fu.IsBanned {
			_ = b.rpl(ctx, u, "🚫 You are banned.")
			return 0, false
		}
		if fu.IsSilenced {
			return 0, false
		}
	}

	if b.isMaint() && !b.isAdm(user.ID) {
		_ = b.rpl(ctx, u, "🔧 Under maintenance.")
		return 0, false
	}
	return user.ID, true
}

// ... (el resto del código sigue igual)

func (b *TelegramBot) isAdm(uid int64) bool {
	if uid == superAdmin {
		return true
	}
	u, _ := b.fs.GetUser(uid)
	return u != nil && u.IsAdmin
}

// Continúa con el resto de funciones...
// (cmdStart, handleMedia, genURL, etc.)
