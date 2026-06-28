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
		&gotgproto.ClientOpts{InMemory: false, Session: sessionMaker.SqlSession(sqlite.Open(dsn)), DisableCopyright: true})
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
		config: cfg, tgClient: tgClient, tgCtx: tgCtx,
		logger: log, fs: fireStore,
		repoSess: make(map[int64]*repoSession), blExt: bl,
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
		"start":         b.cmdStart,
		"help":          b.cmdHelp,
		"mylinks":       b.cmdMyLinks,
		"myfiles":       b.cmdMyFiles,
		"usage":         b.cmdUsage,
		"stats":         b.cmdStats,
		"report":        b.cmdReport,
		"contact":       b.cmdContact,
		"authorize":     b.cmdAuthorize,
		"deauthorize":   b.cmdDeauthorize,
		"ban":           b.cmdBan,
		"unban":         b.cmdUnban,
		"silenceban":    b.cmdSilenceBan,
		"unsilenceban":  b.cmdUnsilenceBan,
		"deleteuser":    b.cmdDeleteUser,
		"warn":          b.cmdWarn,
		"unwarn":        b.cmdUnwarn,
		"limit":         b.cmdLimit,
		"setlimitglobal": b.cmdSetLimitGlobal,
		"listusers":     b.cmdListUsers,
		"userinfo":      b.cmdUserInfo,
		"broadcast":     b.cmdBroadcast,
		"maintenance":   b.cmdMaintenance,
		"cleanup":       b.cmdCleanup,
		"checklogs":     b.cmdCheckLogs,
		"promote":       b.cmdPromote,
		"setexpiration": b.cmdSetExpiration,
		"serverinfo":    b.cmdServerInfo,
		"blacklist":     b.cmdBlacklist,
		"checkdisk":     b.cmdCheckDisk,
		"dumpuser":      b.cmdDumpUser,
		"repo":          b.cmdRepo,
		"addids":        b.cmdAddIDs,
		"cleanids":      b.cmdCleanIDs,
	}
	for n, h := range cmds {
		d.AddHandler(handlers.NewCommand(n, h))
	}
	d.AddHandler(handlers.NewCallbackQuery(filters.CallbackQuery.Prefix("cb_"), b.handleCB))
	d.AddHandler(handlers.NewMessage(filters.Message.Media, b.handleMedia))
}

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

func (b *TelegramBot) isAdm(uid int64) bool {
	if uid == superAdmin {
		return true
	}
	u, _ := b.fs.GetUser(uid)
	return u != nil && u.IsAdmin
}

func (b *TelegramBot) getUser(uid int64) *store.User {
	fu, _ := b.fs.GetUser(uid)
	return fu
}

func (b *TelegramBot) cmdStart(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	chatID := u.EffectiveChat().GetID()
	user := u.EffectiveUser()
	isSA := uid == superAdmin
	fu := b.getUser(uid)
	if fu == nil {
		n := time.Now().UTC().Format("2006-01-02 15:04:05")
		fu = &store.User{
			UserID: uid, ChatID: chatID,
			FirstName: user.FirstName, LastName: user.LastName, Username: user.Username,
			IsAuthorized: true, IsAdmin: isSA, CreatedAt: n, UpdatedAt: n,
		}
		if err := b.fs.SaveUser(fu); err != nil {
			b.logger.Printf("❌ SaveUser %d: %v", uid, err)
		}
		b.fs.LogActivity(uid, "register", "New user")
		if !isSA {
			go b.notifyAdm(fmt.Sprintf("👤 New user: %s %s (@%s) ID: %d", user.FirstName, user.LastName, user.Username, uid))
		}
	} else {
		up := map[string]interface{}{
			"updated_at": time.Now().UTC().Format("2006-01-02 15:04:05"),
			"chat_id":    chatID, "first_name": user.FirstName,
			"last_name": user.LastName, "username": user.Username,
		}
		if isSA && !fu.IsAdmin {
			up["is_admin"] = true
			up["is_authorized"] = true
		}
		if !fu.IsAuthorized {
			up["is_authorized"] = true
		}
		_ = b.fs.UpdateUser(uid, up)
	}
	_ = b.fs.TouchUser(uid)
	name := user.FirstName
	if user.Username != "" {
		name = "@" + user.Username
	}
	role := ""
	if isSA {
		role = " 👑"
	}
	return b.rpl(ctx, u, fmt.Sprintf(
		"🌊 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞\n\n"+
			"Welcome, %s%s\n\n"+
			"Send or forward any file and I will\ngenerate a streaming link for you.\n\n"+
			"📁 /mylinks  Your links\n"+
			"📂 /myfiles  Your files (re-sent)\n"+
			"📊 /usage  Your stats\n"+
			"🆘 /report  Support\n"+
			"❓ /help  Full guide", name, role))
}

func (b *TelegramBot) cmdHelp(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	msg := "🌊 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞 Guide\n\n" +
		"📌 Commands\n" +
		"/start  Welcome\n/help  This guide\n" +
		"/mylinks  Your generated links\n" +
		"/myfiles  Your files (re-sent from log)\n" +
		"/usage  Your stats\n" +
		"/report  Support\n/contact  Admin\n"
	if b.isAdm(uid) {
		msg += "\n👑 Admin\n" +
			"/authorize /deauthorize <id>\n/ban /unban /silenceban /unsilenceban <id>\n" +
			"/deleteuser <id>\n/warn /unwarn <id>\n/limit <id> <MB>\n/setlimitglobal <MB>\n" +
			"/listusers /userinfo <id>\n/broadcast <message>\n" +
			"/addids <IDs>  Add verified users\n" +
			"/cleanids  Remove invalid IDs from DB\n" +
			"/maintenance <on|off>\n/cleanup /checklogs <id>\n" +
			"/promote <id> /setexpiration <id> <days>\n" +
			"/serverinfo /checkdisk /blacklist <ext>\n" +
			"/dumpuser <id> /stats\n\n" +
			"📂 /repo start|end|cancel|status|sort\n"
	}
	return b.rpl(ctx, u, msg)
}

func (b *TelegramBot) handleMedia(ctx *ext.Context, u *ext.Update) error {
	chatID := u.EffectiveChat().GetID()
	msg := u.EffectiveMessage.Message
	_, isFwd := msg.GetFwdFrom()
	if !b.isUserChat(ctx, chatID) {
		return dispatcher.EndGroups
	}
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	fu := b.getUser(uid)
	if fu == nil {
		return b.rpl(ctx, u, "Please use /start first.")
	}
	if !fu.IsAuthorized {
		return b.rpl(ctx, u, "Not authorized yet.")
	}
	_ = b.fs.TouchUser(uid)
	file, err := utils.FileFromMedia(msg.Media)
	if err != nil {
		return b.rpl(ctx, u, fmt.Sprintf("Unsupported media: %v", err))
	}
	if file.FileName == "" || file.FileName == "file" {
		caption := msg.Message
		if caption != "" {
			clean := strings.TrimSpace(caption)
			if idx := strings.IndexAny(clean, "\n\r"); idx > 0 {
				clean = clean[:idx]
			}
			if len(clean) > 100 {
				clean = clean[:100]
			}
			if clean != "" {
				if !hasExt(clean) {
					clean += extFromMime(file.MimeType)
				}
				file.FileName = clean
			}
		}
		if file.FileName == "" || file.FileName == "file" {
			file.FileName = fmt.Sprintf("file_%d%s", time.Now().Unix(), extFromMime(file.MimeType))
		}
	}
	fext := getFileExt(file.FileName)
	b.blMu.RLock()
	blocked := b.blExt[strings.ToLower(fext)]
	b.blMu.RUnlock()
	if blocked {
		return b.rpl(ctx, u, fmt.Sprintf("🚫 %s files not allowed.", fext))
	}
	lim := b.getLimit(uid)
	if lim > 0 && file.FileSize > int64(lim)*1024*1024 {
		return b.rpl(ctx, u, fmt.Sprintf("🚫 Limit: %d MB. Your file: %s.", lim, hBytes(file.FileSize)))
	}
	origID := msg.ID
	logID := 0
	if b.config.LogChannelID != "" && b.config.LogChannelID != "0" {
		if id, e := b.fwdToLog(ctx, chatID, origID); e == nil {
			logID = id
		}
	}
	var chURL int64
	var msgURL int
	if logID > 0 {
		chURL, _ = b.logChID()
		msgURL = logID
	} else {
		chURL = chatID
		msgURL = origID
	}
	// Note: chURL and msgURL are kept for storage but no longer used in URL generation
	_ = chURL
	_ = msgURL

	fileURL := b.genURL(file)
	pid := genPubID(uid, origID, file.ID)
	h := utils.GetShortHash(utils.PackFile(file.FileName, file.FileSize, file.MimeType, file.ID), b.config.HashLength)
	mf := &store.MediaFile{
		PublicID: pid, UserID: uid, ChatID: chatID, OriginalMessageID: origID,
		LogChannelID: b.config.LogChannelID, LogMessageID: logID, FileID: file.ID,
		FileName: file.FileName, MimeType: file.MimeType, FileSize: file.FileSize,
		Duration: file.Duration, Width: file.Width, Height: file.Height,
		Title: file.Title, Performer: file.Performer, Hash: h, PublicURL: fileURL,
		IsForwarded: isFwd, CreatedAt: time.Now().UTC().Format("2006-01-02 15:04:05"),
	}
	if err := b.fs.SaveMedia(mf); err != nil {
		b.logger.Printf("SaveMedia error: %v", err)
	}
	if fu != nil {
		_ = b.fs.UpdateUser(uid, map[string]interface{}{
			"total_files": fu.TotalFiles + 1,
			"total_bytes": fu.TotalBytes + file.FileSize,
		})
	}
	b.fs.LogActivity(uid, "upload", file.FileName)
	if logID > 0 {
		go b.sendAudit(mf)
	}
	b.repoMu.Lock()
	if sess, ok := b.repoSess[uid]; ok {
		sess.Files = append(sess.Files, repoFile{
			FileName: file.FileName, MimeType: file.MimeType, FileSize: file.FileSize,
			Duration: file.Duration, Width: file.Width, Height: file.Height,
			URL: fileURL, AddedAt: time.Now(),
		})
		cnt := len(sess.Files)
		b.repoMu.Unlock()
		_ = b.rpl(ctx, u, fmt.Sprintf("📂 Added to repo (%d files)", cnt))
	} else {
		b.repoMu.Unlock()
	}
	return b.sendCard(ctx, u, fileURL, file)
}

func (b *TelegramBot) sendCard(ctx *ext.Context, u *ext.Update, fileURL string, file *types.DocumentFile) error {
	fn := strings.TrimSpace(file.FileName)
	if fn == "" {
		fn = "file"
	}
	fe := strings.ToUpper(strings.TrimPrefix(getFileExt(fn), "."))
	if fe == "" {
		fe = "N/A"
	}
	mt := smartMime(fn, file.MimeType)
	var sb strings.Builder
	sb.WriteString("File processed successfully! 📁\n\n")
	sb.WriteString(fmt.Sprintf("📄 Name: %s\n🏷 Type: %s\n⚖️ Size: %s\n", fn, fe, hBytes(file.FileSize)))
	if isTimed(file, mt) && file.Duration > 0 {
		sb.WriteString(fmt.Sprintf("⏱ Duration: %s\n", fmtDur(file.Duration)))
	}
	if file.Width > 0 && file.Height > 0 {
		sb.WriteString(fmt.Sprintf("📐 %dx%d\n", file.Width, file.Height))
	}
	sb.WriteString(fmt.Sprintf("\n🔗 %s\n\nProcessed via @Hostwave_bot", fileURL))
	_, err := ctx.Reply(u, ext.ReplyTextString(sb.String()), &ext.ReplyOpts{})
	return err
}

func (b *TelegramBot) cmdMyLinks(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	page := 1
	if len(args) > 1 {
		if p, e := strconv.Atoi(args[1]); e == nil && p > 0 {
			page = p
		}
	}
	off := (page - 1) * pgSize
	items, total, err := b.fs.GetUserMedia(uid, off, pgSize)
	if err != nil {
		b.logger.Printf("GetUserMedia error: %v", err)
		return b.rpl(ctx, u, "❌ Error retrieving your links.")
	}
	if total == 0 {
		return b.rpl(ctx, u, "📁 No files yet.\nSend any file to generate a streaming link.")
	}
	pg := page
	tp := int((total + int64(pgSize) - 1) / int64(pgSize))
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🌊 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞  Your Links\n\n📊 Total: %d files\n\n", total))
	for _, m := range items {
		fn := m.FileName
		if len(fn) > 50 {
			fn = fn[:47] + "..."
		}
		fe := strings.ToUpper(strings.TrimPrefix(getFileExt(m.FileName), "."))
		if fe == "" {
			fe = "FILE"
		}
		date := m.CreatedAt
		if len(date) > 10 {
			date = date[:10]
		}
		sb.WriteString(fmt.Sprintf("📄 %s\n", fn))
		sb.WriteString(fmt.Sprintf("   🏷 %s  ⚖️ %s  📅 %s\n", fe, hBytes(m.FileSize), date))
		if m.Duration > 0 {
			sb.WriteString(fmt.Sprintf("   ⏱ %s", fmtDur(m.Duration)))
			if m.Width > 0 && m.Height > 0 {
				sb.WriteString(fmt.Sprintf("  📐 %dx%d", m.Width, m.Height))
			}
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("   🔗 %s\n\n", m.PublicURL))
	}
	sb.WriteString(fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━\nPage %d/%d", pg, tp))
	if tp > 1 {
		sb.WriteString(fmt.Sprintf("  |  /mylinks %d for next page", pg+1))
	}
	return b.rpl(ctx, u, sb.String())
}

func (b *TelegramBot) cmdMyFiles(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	if b.config.LogChannelID == "" || b.config.LogChannelID == "0" {
		return b.rpl(ctx, u, "❌ Log channel not configured.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	page := 1
	if len(args) > 1 {
		if p, e := strconv.Atoi(args[1]); e == nil && p > 0 {
			page = p
		}
	}
	off := (page - 1) * pgSize
	items, total, err := b.fs.GetUserMedia(uid, off, pgSize)
	if err != nil {
		b.logger.Printf("GetUserMedia error: %v", err)
		return b.rpl(ctx, u, "❌ Error retrieving your files.")
	}
	if total == 0 {
		return b.rpl(ctx, u, "📂 No files found in log channel.\nSend files to generate links.")
	}
	pg := page
	tp := int((total + int64(pgSize) - 1) / int64(pgSize))
	_ = b.rpl(ctx, u, fmt.Sprintf("📂 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞  Your Files (Page %d/%d)\n\n📊 Total: %d files\n\nReceiving files now...", pg, tp, total))
	for _, m := range items {
		if m.LogMessageID <= 0 || m.LogChannelID == "" {
			fn := m.FileName
			if len(fn) > 50 {
				fn = fn[:47] + "..."
			}
			date := m.CreatedAt
			if len(date) > 10 {
				date = date[:10]
			}
			_ = b.rpl(ctx, u, fmt.Sprintf("📅 %s\n📄 %s\n🔗 %s\n", date, fn, m.PublicURL))
			time.Sleep(300 * time.Millisecond)
			continue
		}
		err := b.forwardFileFromLog(ctx, u, &m)
		if err != nil {
			b.logger.Printf("ForwardFileFromLog error: %v", err)
			fn := m.FileName
			if len(fn) > 50 {
				fn = fn[:47] + "..."
			}
			date := m.CreatedAt
			if len(date) > 10 {
				date = date[:10]
			}
			_ = b.rpl(ctx, u, fmt.Sprintf("📅 %s\n📄 %s\n🔗 %s\n⚠️ File unavailable for resend", date, fn, m.PublicURL))
		}
		time.Sleep(500 * time.Millisecond)
	}
	if tp > 1 {
		_ = b.rpl(ctx, u, fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━\nPage %d/%d\n/myfiles %d for next page", pg, tp, pg+1))
	}
	return nil
}

func (b *TelegramBot) forwardFileFromLog(ctx *ext.Context, u *ext.Update, m *store.MediaFile) error {
	logPeer, err := b.logChPeer()
	if err != nil {
		return fmt.Errorf("logChPeer error: %w", err)
	}
	userPeer := ctx.PeerStorage.GetInputPeerById(u.EffectiveChat().GetID())
	if userPeer == nil {
		return fmt.Errorf("user peer not found")
	}
	_, err = ctx.Raw.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer: logPeer,
		ToPeer:   userPeer,
		ID:       []int{m.LogMessageID},
		RandomID: []int64{rand.Int63()},
		Silent:   false,
	})
	if err != nil {
		return fmt.Errorf("forward error: %w", err)
	}
	date := m.CreatedAt
	if len(date) > 10 {
		date = date[:10]
	}
	fn := m.FileName
	if len(fn) > 50 {
		fn = fn[:47] + "..."
	}
	_ = b.rpl(ctx, u, fmt.Sprintf("📅 %s | 📄 %s\n🔗 %s\n", date, fn, m.PublicURL))
	return nil
}

func (b *TelegramBot) cmdUsage(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	fu := b.getUser(uid)
	cnt, _ := b.fs.CountMediaByUser(uid)
	today, _ := b.fs.CountMediaTodayByUser(uid)
	lim := b.getLimit(uid)
	ls := "Unlimited"
	if lim > 0 {
		ls = fmt.Sprintf("%d MB", lim)
	}
	tb := int64(0)
	if fu != nil {
		tb = fu.TotalBytes
	}
	return b.rpl(ctx, u, fmt.Sprintf("📊 Your Usage\n\n📁 Files: %d\n💾 Size: %s\n📅 Today: %d\n⚖️ Limit: %s", cnt, hBytes(tb), today, ls))
}

func (b *TelegramBot) cmdStats(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	users, _ := b.fs.CountUsers()
	auth, _ := b.fs.CountAuthorized()
	media, _ := b.fs.CountMedia()
	size, _ := b.fs.TotalMediaSize()
	today, _ := b.fs.CountMediaToday()
	return b.rpl(ctx, u, fmt.Sprintf("📊 𝐇𝐨𝐬𝐭 𝐖𝐚𝐯𝐞 Stats\n\n👥 Users: %d\n✅ Authorized: %d\n📁 Files: %d\n💾 Total: %s\n📅 Today: %d", users, auth, media, hBytes(size), today))
}

func (b *TelegramBot) cmdReport(ctx *ext.Context, u *ext.Update) error {
	uid, ok := b.access(ctx, u)
	if !ok {
		return nil
	}
	raw := u.EffectiveMessage.Text
	idx := strings.Index(raw, " ")
	if idx == -1 || idx >= len(raw)-1 {
		return b.rpl(ctx, u, "Usage: /report <message>")
	}
	_ = b.fs.SaveReport(uid, raw[idx+1:])
	go b.notifyAdm(fmt.Sprintf("📩 Report from %d:\n%s", uid, raw[idx+1:]))
	return b.rpl(ctx, u, "✅ Report submitted.")
}

func (b *TelegramBot) cmdContact(ctx *ext.Context, u *ext.Update) error {
	return b.rpl(ctx, u, "📩 Use /report <message> to contact admin.")
}

func (b *TelegramBot) cmdAddIDs(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	raw := u.EffectiveMessage.Text
	idx := strings.Index(raw, " ")
	if idx == -1 || idx >= len(raw)-1 {
		return b.rpl(ctx, u, "Usage: /addids <ID or paste text with IDs>")
	}
	text := raw[idx+1:]
	matches := idRegex.FindAllString(text, -1)
	if len(matches) == 0 {
		return b.rpl(ctx, u, "No IDs found. IDs must be 7-15 digit numbers.")
	}
	seen := make(map[int64]bool)
	var ids []int64
	for _, m := range matches {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil || id <= 0 {
			continue
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return b.rpl(ctx, u, "No valid IDs found.")
	}
	_ = b.rpl(ctx, u, fmt.Sprintf("🔍 Verifying %d IDs against Telegram...", len(ids)))
	added := 0
	existed := 0
	invalid := 0
	n := time.Now().UTC().Format("2006-01-02 15:04:05")
	for _, id := range ids {
		existing := b.getUser(id)
		if existing != nil {
			if !existing.IsAuthorized {
				_ = b.fs.UpdateUser(id, map[string]interface{}{"is_authorized": true})
			}
			existed++
			continue
		}
		inputUser := &tg.InputUser{UserID: id, AccessHash: 0}
		userResult, err := b.tgClient.API().UsersGetUsers(b.tgCtx, []tg.InputUserClass{inputUser})
		if err != nil || len(userResult) == 0 {
			invalid++
			continue
		}
		tgUser, ok := userResult[0].(*tg.User)
		if !ok || tgUser == nil || tgUser.ID == 0 {
			invalid++
			continue
		}
		newUser := &store.User{
			UserID: tgUser.ID, ChatID: tgUser.ID,
			FirstName: tgUser.FirstName, LastName: tgUser.LastName,
			Username: tgUser.Username,
			IsAuthorized: true, IsAdmin: false,
			CreatedAt: n, UpdatedAt: n,
		}
		if err := b.fs.SaveUser(newUser); err != nil {
			b.logger.Printf("AddIDs SaveUser %d: %v", id, err)
			continue
		}
		added++
		time.Sleep(200 * time.Millisecond)
	}
	b.fs.LogActivity(u.EffectiveUser().ID, "addids", fmt.Sprintf("added=%d existed=%d invalid=%d", added, existed, invalid))
	return b.rpl(ctx, u, fmt.Sprintf("✅ Verification Complete\n\n🆔 IDs checked: %d\n➕ Added: %d\n📋 Already existed: %d\n❌ Not valid: %d", len(ids), added, existed, invalid))
}

func (b *TelegramBot) cmdCleanIDs(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	_ = b.rpl(ctx, u, "🔍 Verifying all users in database...")
	allUsers, err := b.fs.ExportAllUsers()
	if err != nil {
		return b.rpl(ctx, u, fmt.Sprintf("❌ Error loading users: %v", err))
	}
	if len(allUsers) == 0 {
		return b.rpl(ctx, u, "No users in database.")
	}
	valid := 0
	removed := 0
	total := len(allUsers)
	for i, usr := range allUsers {
		if usr.UserID == superAdmin || usr.UserID == b.tgClient.Self.ID {
			valid++
			continue
		}
		inputUser := &tg.InputUser{UserID: usr.UserID, AccessHash: 0}
		result, err := b.tgClient.API().UsersGetUsers(b.tgCtx, []tg.InputUserClass{inputUser})
		isValid := false
		if err == nil && len(result) > 0 {
			if tgUser, ok := result[0].(*tg.User); ok && tgUser != nil && tgUser.ID != 0 {
				isValid = true
				if tgUser.FirstName != usr.FirstName || tgUser.LastName != usr.LastName || tgUser.Username != usr.Username {
					_ = b.fs.UpdateUser(usr.UserID, map[string]interface{}{
						"first_name": tgUser.FirstName,
						"last_name":  tgUser.LastName,
						"username":   tgUser.Username,
					})
				}
			}
		}
		if isValid {
			valid++
		} else {
			_ = b.fs.DeleteUser(usr.UserID)
			removed++
		}
		if (i+1)%25 == 0 {
			_ = b.rpl(ctx, u, fmt.Sprintf("⏳ Progress: %d/%d checked...", i+1, total))
		}
		time.Sleep(300 * time.Millisecond)
	}
	b.fs.LogActivity(u.EffectiveUser().ID, "cleanids", fmt.Sprintf("valid=%d removed=%d total=%d", valid, removed, total))
	return b.rpl(ctx, u, fmt.Sprintf("✅ Cleanup Complete\n\n🆔 Checked: %d\n✅ Valid: %d\n🗑️ Removed: %d", total, valid, removed))
}

func (b *TelegramBot) cmdAuthorize(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /authorize <id> [admin]")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	adm := len(args) > 2 && strings.EqualFold(args[2], "admin")
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"is_authorized": true, "is_admin": adm})
	if fu := b.getUser(tid); fu != nil {
		_ = b.sendDM(fu.ChatID, "✅ You have been authorized.")
	}
	return b.rpl(ctx, u, fmt.Sprintf("✅ User %d authorized.", tid))
}

func (b *TelegramBot) cmdDeauthorize(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /deauthorize <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"is_authorized": false})
	return b.rpl(ctx, u, fmt.Sprintf("❌ User %d deauthorized.", tid))
}

func (b *TelegramBot) setF(ctx *ext.Context, u *ext.Update, field string, val interface{}, cmd, dm, lg string) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: "+cmd+" <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	_ = b.fs.UpdateUser(tid, map[string]interface{}{field: val})
	if dm != "" {
		if fu := b.getUser(tid); fu != nil {
			_ = b.sendDM(fu.ChatID, dm)
		}
	}
	b.fs.LogActivity(u.EffectiveUser().ID, lg, fmt.Sprintf("target=%d", tid))
	return b.rpl(ctx, u, fmt.Sprintf("✅ User %d %s.", tid, lg))
}

func (b *TelegramBot) cmdBan(ctx *ext.Context, u *ext.Update) error             { return b.setF(ctx, u, "is_banned", true, "/ban", "🚫 Banned.", "banned") }
func (b *TelegramBot) cmdUnban(ctx *ext.Context, u *ext.Update) error           { return b.setF(ctx, u, "is_banned", false, "/unban", "✅ Unbanned.", "unbanned") }
func (b *TelegramBot) cmdSilenceBan(ctx *ext.Context, u *ext.Update) error      { return b.setF(ctx, u, "is_silenced", true, "/silenceban", "", "silenced") }
func (b *TelegramBot) cmdUnsilenceBan(ctx *ext.Context, u *ext.Update) error    { return b.setF(ctx, u, "is_silenced", false, "/unsilenceban", "", "unsilenced") }

func (b *TelegramBot) cmdDeleteUser(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /deleteuser <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	_ = b.fs.DeleteUser(tid)
	return b.rpl(ctx, u, fmt.Sprintf("🗑️ User %d deleted.", tid))
}

func (b *TelegramBot) cmdWarn(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /warn <id> [reason]")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	fu := b.getUser(tid)
	w := 0
	if fu != nil {
		w = fu.Warnings + 1
	}
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"warnings": w})
	reason := "No reason"
	if len(args) > 2 {
		reason = strings.Join(args[2:], " ")
	}
	if fu != nil {
		_ = b.sendDM(fu.ChatID, fmt.Sprintf("⚠️ Warning: %s (total: %d)", reason, w))
	}
	return b.rpl(ctx, u, fmt.Sprintf("⚠️ User %d warned (%d).", tid, w))
}

func (b *TelegramBot) cmdUnwarn(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /unwarn <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"warnings": 0})
	return b.rpl(ctx, u, fmt.Sprintf("✅ Warnings cleared for %d.", tid))
}

func (b *TelegramBot) cmdLimit(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 3 {
		return b.rpl(ctx, u, "Usage: /limit <id> <MB>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	mb, _ := strconv.Atoi(args[2])
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"file_size_limit_mb": mb})
	return b.rpl(ctx, u, fmt.Sprintf("✅ Limit for %d: %d MB.", tid, mb))
}

func (b *TelegramBot) cmdSetLimitGlobal(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /setlimitglobal <MB>")
	}
	mb, _ := strconv.Atoi(args[1])
	b.gLimitMu.Lock()
	b.gLimit = mb
	b.gLimitMu.Unlock()
	return b.rpl(ctx, u, fmt.Sprintf("✅ Global limit: %d MB.", mb))
}

func (b *TelegramBot) cmdListUsers(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	pg := 1
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) > 1 {
		if p, e := strconv.Atoi(args[1]); e == nil && p > 0 {
			pg = p
		}
	}
	return b.pageUsers(ctx, u, pg)
}

func (b *TelegramBot) pageUsers(ctx *ext.Context, u *ext.Update, pg int) error {
	off := (pg - 1) * pgSize
	users, total, _ := b.fs.ListUsers(off, pgSize)
	if len(users) == 0 {
		return b.rpl(ctx, u, "No users.")
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("👥 Users (%d)\n\n", total))
	for i, usr := range users {
		st := "❌"
		if usr.IsAuthorized {
			st = "✅"
		}
		ad := ""
		if usr.IsAdmin {
			ad = " 👑"
		}
		un := usr.Username
		if un == "" {
			un = "N/A"
		}
		sb.WriteString(fmt.Sprintf("%d. %d @%s %s%s\n", off+i+1, usr.UserID, un, st, ad))
	}
	tp := (total + pgSize - 1) / pgSize
	sb.WriteString(fmt.Sprintf("\nPage %d/%d", pg, tp))
	var btns []tg.KeyboardButtonClass
	if pg > 1 {
		btns = append(btns, &tg.KeyboardButtonCallback{Text: "⬅️ Prev", Data: []byte(fmt.Sprintf("cb_%s,%d", cbLU, pg-1))})
	}
	if pg < tp {
		btns = append(btns, &tg.KeyboardButtonCallback{Text: "Next ➡️", Data: []byte(fmt.Sprintf("cb_%s,%d", cbLU, pg+1))})
	}
	var mk tg.ReplyMarkupClass
	if len(btns) > 0 {
		mk = &tg.ReplyInlineMarkup{Rows: []tg.KeyboardButtonRow{{Buttons: btns}}}
	}
	_, err := ctx.Reply(u, ext.ReplyTextString(sb.String()), &ext.ReplyOpts{Markup: mk})
	return err
}

func (b *TelegramBot) cmdUserInfo(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /userinfo <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	fu := b.getUser(tid)
	if fu == nil {
		return b.rpl(ctx, u, "User not found.")
	}
	cnt, _ := b.fs.CountMediaByUser(tid)
	return b.rpl(ctx, u, fmt.Sprintf("👤 User Info\n\nID: %d\nName: %s %s\nUsername: @%s\nAuth: %v | Admin: %v\nBanned: %v | Silenced: %v\nWarnings: %d | Limit: %d MB\nFiles: %d | Size: %s\nSince: %s",
		fu.UserID, fu.FirstName, fu.LastName, fu.Username, fu.IsAuthorized, fu.IsAdmin, fu.IsBanned, fu.IsSilenced, fu.Warnings, fu.FileSizeLimitMB, cnt, hBytes(fu.TotalBytes), fu.CreatedAt))
}

func (b *TelegramBot) cmdBroadcast(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	raw := u.EffectiveMessage.Text
	idx := strings.Index(raw, " ")
	if idx == -1 || idx >= len(raw)-1 {
		return b.rpl(ctx, u, "Usage: /broadcast <message>")
	}
	message := raw[idx+1:]
	users, err := b.fs.GetBroadcastUsers(true)
	if err != nil {
		return b.rpl(ctx, u, fmt.Sprintf("❌ Error getting users: %v", err))
	}
	if len(users) == 0 {
		return b.rpl(ctx, u, "No users to broadcast to.")
	}
	go func() {
		sent, fail := 0, 0
		for _, usr := range users {
			if usr.UserID == b.tgClient.Self.ID || usr.UserID == superAdmin {
				continue
			}
			err := b.sendDM(usr.ChatID, message)
			if err != nil {
				b.logger.Printf("Broadcast fail to %d: %v", usr.UserID, err)
				fail++
			} else {
				sent++
			}
			time.Sleep(1500 * time.Millisecond)
		}
		_ = b.sendDM(u.EffectiveChat().GetID(), fmt.Sprintf("📣 Broadcast Done\n\n✅ Sent: %d\n❌ Failed: %d\n📊 Total: %d", sent, fail, len(users)))
	}()
	return b.rpl(ctx, u, fmt.Sprintf("📣 Broadcasting to %d users...", len(users)))
}

func (b *TelegramBot) cmdMaintenance(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /maintenance <on|off>")
	}
	on := strings.ToLower(args[1]) == "on"
	b.maintMu.Lock()
	b.maint = on
	b.maintMu.Unlock()
	if on {
		return b.rpl(ctx, u, "🔧 Maintenance ON.")
	}
	return b.rpl(ctx, u, "✅ Maintenance OFF.")
}

func (b *TelegramBot) cmdCleanup(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	c := b.fs.CleanupOldActivity(30)
	return b.rpl(ctx, u, fmt.Sprintf("🧹 Removed %d old logs.", c))
}

func (b *TelegramBot) cmdCheckLogs(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /checklogs <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	entries, _ := b.fs.GetUserActivity(tid, 15)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 Logs for %d\n\n", tid))
	for _, e := range entries {
		sb.WriteString(fmt.Sprintf("[%s] %s: %s\n", e.CreatedAt[:16], e.Action, e.Detail))
	}
	return b.rpl(ctx, u, sb.String())
}

func (b *TelegramBot) cmdPromote(ctx *ext.Context, u *ext.Update) error {
	if u.EffectiveUser().ID != superAdmin {
		return b.rpl(ctx, u, "🚫 Super admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /promote <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"is_admin": true, "is_authorized": true})
	return b.rpl(ctx, u, fmt.Sprintf("👑 User %d promoted.", tid))
}

func (b *TelegramBot) cmdSetExpiration(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 3 {
		return b.rpl(ctx, u, "Usage: /setexpiration <id> <days>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	days, _ := strconv.Atoi(args[2])
	exp := time.Now().AddDate(0, 0, days).Format("2006-01-02 15:04:05")
	_ = b.fs.UpdateUser(tid, map[string]interface{}{"expires_at": exp})
	return b.rpl(ctx, u, fmt.Sprintf("✅ User %d expires in %d days.", tid, days))
}

func (b *TelegramBot) cmdServerInfo(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return b.rpl(ctx, u, fmt.Sprintf("🖥 Server\n\nOS: %s/%s\nGoroutines: %d\nRAM: %s\nDB: Firebase Firestore", runtime.GOOS, runtime.GOARCH, runtime.NumGoroutine(), hBytes(int64(m.Alloc))))
}

func (b *TelegramBot) cmdBlacklist(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		b.blMu.RLock()
		list := make([]string, 0)
		for e := range b.blExt {
			list = append(list, e)
		}
		b.blMu.RUnlock()
		if len(list) == 0 {
			return b.rpl(ctx, u, "No blacklisted extensions.\nUsage: /blacklist <.ext>")
		}
		return b.rpl(ctx, u, "Blacklisted: "+strings.Join(list, ", "))
	}
	bx := strings.ToLower(args[1])
	if !strings.HasPrefix(bx, ".") {
		bx = "." + bx
	}
	b.blMu.Lock()
	if b.blExt[bx] {
		delete(b.blExt, bx)
	} else {
		b.blExt[bx] = true
	}
	cp := make(map[string]bool)
	for k, v := range b.blExt {
		cp[k] = v
	}
	b.blMu.Unlock()
	_ = b.fs.SetBlacklist(cp)
	if cp[bx] {
		return b.rpl(ctx, u, fmt.Sprintf("🚫 %s blacklisted.", bx))
	}
	return b.rpl(ctx, u, fmt.Sprintf("✅ %s removed.", bx))
}

func (b *TelegramBot) cmdCheckDisk(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	mc, _ := b.fs.CountMedia()
	uc, _ := b.fs.CountUsers()
	return b.rpl(ctx, u, fmt.Sprintf("☁️ Firestore: %d users, %d files", uc, mc))
}

func (b *TelegramBot) cmdDumpUser(ctx *ext.Context, u *ext.Update) error {
	if !b.isAdm(u.EffectiveUser().ID) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "Usage: /dumpuser <id>")
	}
	tid, _ := strconv.ParseInt(args[1], 10, 64)
	items, _ := b.fs.GetAllUserMedia(tid)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📦 User %d files\n\n", tid))
	for _, m := range items {
		d := m.CreatedAt
		if len(d) > 10 {
			d = d[:10]
		}
		sb.WriteString(fmt.Sprintf("📄 %s (%s) %s\n🔗 %s\n\n", m.FileName, hBytes(m.FileSize), d, m.PublicURL))
		if sb.Len() > 3500 {
			_ = b.rpl(ctx, u, sb.String())
			sb.Reset()
			time.Sleep(500 * time.Millisecond)
		}
	}
	if sb.Len() > 0 {
		return b.rpl(ctx, u, sb.String())
	}
	return nil
}

func (b *TelegramBot) cmdRepo(ctx *ext.Context, u *ext.Update) error {
	uid := u.EffectiveUser().ID
	if !b.isAdm(uid) {
		return b.rpl(ctx, u, "🚫 Admin only.")
	}
	args := strings.Fields(u.EffectiveMessage.Text)
	if len(args) < 2 {
		return b.rpl(ctx, u, "📂 Repo\n\n/repo start\n/repo end <Title>\n/repo cancel\n/repo status\n/repo sort <date|size|name>")
	}
	switch strings.ToLower(args[1]) {
	case "start":
		b.repoMu.Lock()
		b.repoSess[uid] = &repoSession{Files: []repoFile{}, StartedAt: time.Now()}
		b.repoMu.Unlock()
		return b.rpl(ctx, u, "📂 Repo started. Send files now.")
	case "cancel":
		b.repoMu.Lock()
		delete(b.repoSess, uid)
		b.repoMu.Unlock()
		return b.rpl(ctx, u, "🗑️ Repo cancelled.")
	case "status":
		b.repoMu.Lock()
		s, ok := b.repoSess[uid]
		b.repoMu.Unlock()
		if !ok || len(s.Files) == 0 {
			return b.rpl(ctx, u, "No active repo.")
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("📂 Repo: %d files\n\n", len(s.Files)))
		for _, f := range s.Files {
			n := f.FileName
			if len(n) > 40 {
				n = n[:37] + "..."
			}
			sb.WriteString(fmt.Sprintf("📄 %s (%s)\n", n, hBytes(f.FileSize)))
		}
		return b.rpl(ctx, u, sb.String())
	case "sort":
		b.repoMu.Lock()
		s, ok := b.repoSess[uid]
		if ok && len(s.Files) > 1 {
			by := "name"
			if len(args) > 2 {
				by = strings.ToLower(args[2])
			}
			sortFiles(s.Files, by)
		}
		b.repoMu.Unlock()
		if !ok {
			return b.rpl(ctx, u, "No active repo.")
		}
		sn := "name"
		if len(args) > 2 {
			sn = args[2]
		}
		return b.rpl(ctx, u, fmt.Sprintf("✅ Sorted by %s.", sn))
	case "end":
		b.repoMu.Lock()
		s, ok := b.repoSess[uid]
		if ok {
			delete(b.repoSess, uid)
		}
		b.repoMu.Unlock()
		if !ok || len(s.Files) == 0 {
			return b.rpl(ctx, u, "No files in repo.")
		}
		title := "Untitled"
		if len(args) > 2 {
			title = strings.Join(args[2:], " ")
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("📂 %s\n\n", title))
		ts, td := int64(0), 0
		for _, f := range s.Files {
			nm := f.FileName
			if idx := strings.LastIndex(nm, "."); idx > 0 {
				nm = nm[:idx]
			}
			sb.WriteString(fmt.Sprintf("▸ %s\n  %s", nm, hBytes(f.FileSize)))
			if f.Duration > 0 {
				sb.WriteString(" | " + fmtDur(f.Duration))
			}
			sb.WriteString(fmt.Sprintf("\n  %s\n\n", f.URL))
			ts += f.FileSize
			td += f.Duration
		}
		sb.WriteString(fmt.Sprintf("📊 %d files | %s", len(s.Files), hBytes(ts)))
		if td > 0 {
			sb.WriteString(" | " + fmtDur(td))
		}
		sb.WriteString("\n\nProcessed via @Hostwave_bot")
		full := sb.String()
		if len(full) > 4000 {
			for _, c := range splitSafe(full, 3900) {
				_ = b.rpl(ctx, u, c)
				time.Sleep(300 * time.Millisecond)
			}
			return nil
		}
		return b.rpl(ctx, u, full)
	default:
		return b.rpl(ctx, u, "Unknown: start, end, cancel, status, sort")
	}
}

func (b *TelegramBot) handleCB(ctx *ext.Context, u *ext.Update) error {
	d := string(u.CallbackQuery.Data)
	parts := strings.Split(strings.TrimPrefix(d, "cb_"), ",")
	if len(parts) < 2 {
		return b.ackCB(ctx, u, "OK")
	}
	chatID := u.CallbackQuery.UserID
	switch parts[0] {
	case cbLU:
		pg, _ := strconv.Atoi(parts[1])
		msg := b.buildUserListMsg(pg)
		_ = b.sendDM(chatID, msg)
	}
	return b.ackCB(ctx, u, "OK")
}

func (b *TelegramBot) buildUserListMsg(pg int) string {
	off := (pg - 1) * pgSize
	users, total, _ := b.fs.ListUsers(off, pgSize)
	if len(users) == 0 {
		return "No users."
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("👥 Users (%d)\n\n", total))
	for i, usr := range users {
		st := "❌"
		if usr.IsAuthorized {
			st = "✅"
		}
		ad := ""
		if usr.IsAdmin {
			ad = " 👑"
		}
		un := usr.Username
		if un == "" {
			un = "N/A"
		}
		sb.WriteString(fmt.Sprintf("%d. %d @%s %s%s\n", off+i+1, usr.UserID, un, st, ad))
	}
	tp := (total + pgSize - 1) / pgSize
	sb.WriteString(fmt.Sprintf("\nPage %d/%d\nUse /listusers %d", pg, tp, pg+1))
	return sb.String()
}

func (b *TelegramBot) ackCB(ctx *ext.Context, u *ext.Update, msg string) error {
	_, err := ctx.AnswerCallback(&tg.MessagesSetBotCallbackAnswerRequest{QueryID: u.CallbackQuery.QueryID, Message: msg})
	return err
}

func (b *TelegramBot) logChID() (int64, error) {
	s := strings.TrimPrefix(strings.TrimPrefix(b.config.LogChannelID, "-100"), "-")
	return strconv.ParseInt(s, 10, 64)
}

func (b *TelegramBot) logChPeer() (tg.InputPeerClass, error) {
	id, err := b.logChID()
	if err != nil {
		return nil, err
	}
	peer := b.tgCtx.PeerStorage.GetInputPeerById(id)
	if peer != nil {
		if p, ok := peer.(*tg.InputPeerChannel); ok && p.AccessHash != 0 {
			return peer, nil
		}
	}
	ch, err := b.tgCtx.Raw.ChannelsGetChannels(b.tgCtx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: id, AccessHash: 0}})
	if err != nil {
		return nil, err
	}
	var chats []tg.ChatClass
	switch v := ch.(type) {
	case *tg.MessagesChats:
		chats = v.GetChats()
	case *tg.MessagesChatsSlice:
		chats = v.GetChats()
	}
	for _, c := range chats {
		if rc, ok := c.(*tg.Channel); ok && rc.ID == id {
			b.tgCtx.PeerStorage.AddPeer(rc.ID, rc.AccessHash, storage.TypeChannel, rc.Username)
			return &tg.InputPeerChannel{
				ChannelID:  rc.ID,
				AccessHash: rc.AccessHash,
			}, nil
		}
	}
	return nil, fmt.Errorf("channel %d not found", id)
}

func (b *TelegramBot) fwdToLog(ctx *ext.Context, from int64, msgID int) (int, error) {
	fp := ctx.PeerStorage.GetInputPeerById(from)
	tp, err := b.logChPeer()
	if err != nil || fp == nil {
		return 0, fmt.Errorf("peer error")
	}
	ups, err := ctx.Raw.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer: fp, ToPeer: tp, ID: []int{msgID}, RandomID: []int64{rand.Int63()},
	})
	if err != nil {
		return 0, err
	}
	return extractChMsgID(ups), nil
}

func (b *TelegramBot) sendAudit(m *store.MediaFile) {
	peer, err := b.logChPeer()
	if err != nil {
		return
	}
	msg := fmt.Sprintf("📁 %s | %s | %s\nUser: %d | URL: %s", m.FileName, m.MimeType, hBytes(m.FileSize), m.UserID, m.PublicURL)
	_, _ = b.tgCtx.Raw.MessagesSendMessage(b.tgCtx, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: msg, ReplyTo: &tg.InputReplyToMessage{ReplyToMsgID: m.LogMessageID}, RandomID: rand.Int63(),
	})
}

func (b *TelegramBot) genURL(file *types.DocumentFile) string {
	h := utils.GetShortHash(utils.PackFile(file.FileName, file.FileSize, file.MimeType, file.ID), b.config.HashLength)
	base := strings.TrimRight(b.config.BaseURL, "/")
	encName := url.QueryEscape(file.FileName)
	return fmt.Sprintf("%s/%s/%s", base, h, encName)
}

func (b *TelegramBot) isUserChat(ctx *ext.Context, cid int64) bool {
	return ctx.PeerStorage.GetPeerById(cid).Type == int(storage.TypeUser)
}

func (b *TelegramBot) rpl(ctx *ext.Context, u *ext.Update, msg string) error {
	_, err := ctx.Reply(u, ext.ReplyTextString(msg), &ext.ReplyOpts{})
	return err
}

func (b *TelegramBot) sendDM(chatID int64, msg string) error {
	peer := &tg.InputPeerUser{
		UserID:     chatID,
		AccessHash: 0,
	}
	_, err := b.tgCtx.Raw.MessagesSendMessage(b.tgCtx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  msg,
		RandomID: rand.Int63(),
	})
	return err
}

func (b *TelegramBot) notifyAdm(msg string) {
	fu := b.getUser(superAdmin)
	if fu != nil {
		_ = b.sendDM(fu.ChatID, msg)
	}
}

func (b *TelegramBot) getLimit(uid int64) int {
	fu := b.getUser(uid)
	if fu != nil && fu.FileSizeLimitMB > 0 {
		return fu.FileSizeLimitMB
	}
	b.gLimitMu.RLock()
	defer b.gLimitMu.RUnlock()
	return b.gLimit
}

func getFileExt(fn string) string {
	i := strings.LastIndex(fn, ".")
	if i == -1 || i == len(fn)-1 {
		return ""
	}
	return strings.ToLower(fn[i:])
}

func hasExt(fn string) bool {
	i := strings.LastIndex(fn, ".")
	return i > 0 && i < len(fn)-1
}

func extFromMime(mt string) string {
	m := mimeMap()
	if e, ok := m[strings.ToLower(mt)]; ok {
		return e
	}
	if strings.HasPrefix(mt, "video/") {
		return ".mp4"
	}
	if strings.HasPrefix(mt, "audio/") {
		return ".mp3"
	}
	if strings.HasPrefix(mt, "image/") {
		return ".jpg"
	}
	if strings.HasPrefix(mt, "application/") {
		return ".bin"
	}
	if strings.HasPrefix(mt, "text/") {
		return ".txt"
	}
	return ".file"
}

func smartMime(fn, cur string) string {
	cur = strings.TrimSpace(strings.ToLower(cur))
	if cur != "" && cur != "application/octet-stream" {
		return cur
	}
	e := getFileExt(fn)
	if e == "" {
		if cur != "" {
			return cur
		}
		return "application/octet-stream"
	}
	if mt, ok := mimeMap()[e]; ok {
		return mt
	}
	if mt := mime.TypeByExtension(e); mt != "" {
		return strings.ToLower(strings.Split(mt, ";")[0])
	}
	if cur != "" {
		return cur
	}
	return "application/octet-stream"
}

func isTimed(f *types.DocumentFile, mt string) bool {
	return f.Duration > 0 && (strings.HasPrefix(strings.ToLower(mt), "video/") || strings.HasPrefix(strings.ToLower(mt), "audio/") || f.IsVoice || f.IsAnimation)
}

func fmtDur(s int) string {
	if s <= 0 {
		return "N/A"
	}
	h, m, sec := s/3600, (s%3600)/60, s%60
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
	}
	return fmt.Sprintf("%02d:%02d", m, sec)
}

func hBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	d, e := int64(1024), 0
	for x := n / 1024; x >= 1024; x /= 1024 {
		d *= 1024
		e++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(d), "KMGTPE"[e])
}

func splitSafe(s string, max int) []string {
	if len(s) <= max {
		return []string{s}
	}
	var ch []string
	for len(s) > max {
		c := max
		if i := strings.LastIndex(s[:max], "\n"); i > 0 {
			c = i
		}
		ch = append(ch, s[:c])
		s = s[c:]
	}
	if strings.TrimSpace(s) != "" {
		ch = append(ch, s)
	}
	return ch
}

func genPubID(parts ...interface{}) string {
	h := sha256.Sum256([]byte(fmt.Sprint(time.Now().UnixNano(), rand.Int63(), parts)))
	return hex.EncodeToString(h[:])[:20]
}

func extractChMsgID(ups tg.UpdatesClass) int {
	if ups == nil {
		return 0
	}
	find := func(u tg.UpdateClass) int {
		switch v := u.(type) {
		case *tg.UpdateNewChannelMessage:
			if m, ok := v.Message.(*tg.Message); ok {
				return m.GetID()
			}
		case *tg.UpdateNewMessage:
			if m, ok := v.Message.(*tg.Message); ok {
				return m.GetID()
			}
		}
		return 0
	}
	switch v := ups.(type) {
	case *tg.Updates:
		for _, u := range v.Updates {
			if id := find(u); id != 0 {
				return id
			}
		}
	case *tg.UpdatesCombined:
		for _, u := range v.Updates {
			if id := find(u); id != 0 {
				return id
			}
		}
	case *tg.UpdateShort:
		return find(v.Update)
	case *tg.UpdateShortMessage:
		return v.ID
	case *tg.UpdateShortChatMessage:
		return v.ID
	case *tg.UpdateShortSentMessage:
		return v.ID
	}
	return 0
}

func sortFiles(f []repoFile, by string) {
	for i := 0; i < len(f); i++ {
		for j := i + 1; j < len(f); j++ {
			swap := false
			switch by {
			case "size":
				swap = f[j].FileSize < f[i].FileSize
			case "date":
				swap = f[j].AddedAt.Before(f[i].AddedAt)
			default:
				swap = strings.ToLower(f[j].FileName) < strings.ToLower(f[i].FileName)
			}
			if swap {
				f[i], f[j] = f[j], f[i]
			}
		}
	}
}

func mimeMap() map[string]string {
	return map[string]string{
		".apk":   "application/vnd.android.package-archive",
		".apks":  "application/vnd.android.package-archive",
		".xapk":  "application/vnd.android.package-archive",
		".aab":   "application/vnd.android.aab",
		".mp4":   "video/mp4",
		".mkv":   "video/x-matroska",
		".mov":   "video/quicktime",
		".avi":   "video/x-msvideo",
		".webm":  "video/webm",
		".flv":   "video/x-flv",
		".wmv":   "video/x-ms-wmv",
		".m4v":   "video/x-m4v",
		".3gp":   "video/3gpp",
		".3g2":   "video/3gpp2",
		".mpeg":  "video/mpeg",
		".mpg":   "video/mpeg",
		".ts":    "video/mp2t",
		".mts":   "video/mp2t",
		".vob":   "video/dvd",
		".ogv":   "video/ogg",
		".mp3":   "audio/mpeg",
		".m4a":   "audio/mp4",
		".wav":   "audio/wav",
		".ogg":   "audio/ogg",
		".flac":  "audio/flac",
		".aac":   "audio/aac",
		".wma":   "audio/x-ms-wma",
		".amr":   "audio/amr",
		".opus":  "audio/opus",
		".jpg":   "image/jpeg",
		".jpeg":  "image/jpeg",
		".png":   "image/png",
		".gif":   "image/gif",
		".webp":  "image/webp",
		".bmp":   "image/bmp",
		".tiff":  "image/tiff",
		".tif":   "image/tiff",
		".ico":   "image/x-icon",
		".svg":   "image/svg+xml",
		".heic":  "image/heic",
		".heif":  "image/heif",
		".pdf":   "application/pdf",
		".txt":   "text/plain",
		".csv":   "text/csv",
		".json":  "application/json",
		".xml":   "application/xml",
		".html":  "text/html",
		".htm":   "text/html",
		".doc":   "application/msword",
		".docx":  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		".xls":   "application/vnd.ms-excel",
		".xlsx":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		".ppt":   "application/vnd.ms-powerpoint",
		".pptx":  "application/vnd.openxmlformats-officedocument.presentationml.presentation",
		".odt":   "application/vnd.oasis.opendocument.text",
		".ods":   "application/vnd.oasis.opendocument.spreadsheet",
		".odp":   "application/vnd.oasis.opendocument.presentation",
		".rtf":   "application/rtf",
		".zip":   "application/zip",
		".rar":   "application/vnd.rar",
		".7z":    "application/x-7z-compressed",
		".tar":   "application/x-tar",
		".gz":    "application/gzip",
		".bz2":   "application/x-bzip2",
		".xz":    "application/x-xz",
		".iso":   "application/x-iso9660-image",
		".dmg":   "application/x-apple-diskimage",
		".exe":   "application/x-msdownload",
		".msi":   "application/x-msdownload",
		".deb":   "application/vnd.debian.binary-package",
		".rpm":   "application/x-rpm",
		".bin":   "application/octet-stream",
		".sh":    "application/x-sh",
		".py":    "text/x-python",
		".js":    "application/javascript",
		".tsx":   "application/typescript",
		".java":  "text/x-java",
		".c":     "text/x-c",
		".cpp":   "text/x-c++src",
		".go":    "text/x-go",
		".rs":    "text/x-rust",
		".php":   "application/x-php",
		".rb":    "text/x-ruby",
		".swift": "text/x-swift",
		".kt":    "text/x-kotlin",
		".sql":   "application/sql",
		".yaml":  "application/x-yaml",
		".yml":   "application/x-yaml",
		".toml":  "application/x-toml",
		".ttf":   "font/ttf",
		".otf":   "font/otf",
		".woff":  "font/woff",
		".woff2": "font/woff2",
		".log":   "text/plain",
		".md":    "text/markdown",
	}
}

var _ = json.Marshal
