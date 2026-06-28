package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"webBridgeBot/internal/config"
	"webBridgeBot/internal/logger"
	"webBridgeBot/internal/store"
	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/ext"
	"github.com/celestix/gotgproto/storage"
	"github.com/gorilla/mux"
	"github.com/gotd/td/tg"
)

type Server struct {
	config         *config.Configuration
	tgClient       *gotgproto.Client
	tgCtx          *ext.Context
	logger         *logger.Logger
	fs             *store.FireStore
	userRepository *UserRepoAdapter
	connTracker    *ConnTracker
	wsManager      *WebSocketManager
	router         *mux.Router
}

func NewServer(cfg *config.Configuration, tgClient *gotgproto.Client, tgCtx *ext.Context, log *logger.Logger, fs *store.FireStore) *Server {
	s := &Server{
		config:         cfg,
		tgClient:       tgClient,
		tgCtx:          tgCtx,
		logger:         log,
		fs:             fs,
		userRepository: NewUserRepoAdapter(fs),
		connTracker:    NewConnTracker(),
		wsManager:      NewWebSocketManager(),
	}
	s.router = mux.NewRouter()
	s.router.HandleFunc("/{hash}/{filename}", s.handleFile).Methods("GET", "HEAD")
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// Additional routes expected by handlers.go
	s.router.HandleFunc("/player/{chatID}", s.handlePlayer).Methods("GET")
	s.router.HandleFunc("/avatar/{chatID}", s.handleAvatar).Methods("GET")
	s.router.HandleFunc("/proxy", s.handleProxy).Methods("GET")
	s.router.HandleFunc("/validate-user/{chatID}", s.handleValidateUser).Methods("GET")
	s.router.HandleFunc("/connection-stats/{chatID}", s.handleConnectionStats).Methods("GET")
	s.router.HandleFunc("/favicon.ico", s.handleFavicon).Methods("GET")
	s.router.HandleFunc("/robots.txt", s.handleRobots).Methods("GET")
	s.router.HandleFunc("/sitemap.xml", s.handleSitemap).Methods("GET")
	s.router.HandleFunc("/.well-known/{path}", s.handleWellKnown).Methods("GET")
	s.router.HandleFunc("/metrics", s.handleMetrics).Methods("GET")
	s.router.HandleFunc("/login", s.handleLogin).Methods("GET")
	s.router.HandleFunc("/ws/{chatID}", s.handleWebSocket)

	return s
}

func (s *Server) Start() {
	addr := ":" + s.config.Port
	s.logger.Infof("Web server starting on %s", addr)
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 300 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Errorf("Web server error: %v", err)
		}
	}()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]
	filename := vars["filename"]

	ctx := r.Context()
	mf, err := s.fs.GetMediaByHash(hash)
	if err != nil {
		s.logger.Printf("Lookup media failed for hash %s: %v", hash, err)
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	if mf == nil {
		s.logger.Printf("Media not found for hash %s", hash)
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	if s.config.DebugMode {
		s.logger.Debugf("File request hash=%s filename=%s stored=%s", hash, filename, mf.FileName)
	}

	chatID := mf.ChatID
	msgID := mf.OriginalMessageID

	if mf.LogChannelID != "" && mf.LogMessageID != 0 {
		cidStr := strings.TrimPrefix(strings.TrimPrefix(mf.LogChannelID, "-100"), "-")
		if cid, err := strconv.ParseInt(cidStr, 10, 64); err == nil && cid != 0 {
			chatID = cid
			msgID = mf.LogMessageID
		}
	}

	msg, err := s.getMessage(ctx, chatID, msgID)
	if err != nil {
		s.logger.Printf("Get message failed for hash %s (chat=%d msg=%d): %v", hash, chatID, msgID, err)
		http.Error(w, "Message unavailable", http.StatusInternalServerError)
		return
	}

	tgMsg, ok := msg.(*tg.Message)
	if !ok {
		s.logger.Printf("Invalid message type for hash %s", hash)
		http.Error(w, "Invalid message", http.StatusInternalServerError)
		return
	}

	var loc tg.InputFileLocationClass
	var size int64
	mimeType := mf.MimeType

	switch media := tgMsg.Media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			http.Error(w, "No document", http.StatusNotFound)
			return
		}
		loc = &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
		}
		size = doc.Size
		if mimeType == "" {
			mimeType = doc.MimeType
		}
	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.(*tg.Photo)
		if !ok {
			http.Error(w, "No photo", http.StatusNotFound)
			return
		}
		var largest tg.PhotoSizeClass
		for _, sz := range photo.Sizes {
			if ps, ok := sz.(*tg.PhotoSize); ok {
				if largest == nil || ps.W > largest.(*tg.PhotoSize).W {
					largest = ps
				}
			}
		}
		if largest == nil {
			http.Error(w, "No photo size", http.StatusNotFound)
			return
		}
		ps := largest.(*tg.PhotoSize)
		loc = &tg.InputPhotoFileLocation{
			ID:            photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			ThumbSize:     ps.Type,
		}
		size = int64(ps.Size)
	default:
		http.Error(w, "Unsupported media", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", mimeType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", mf.FileName))
	if size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	w.Header().Set("Accept-Ranges", "bytes")

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	offset := int64(0)
	limit := int64(512 * 1024)
	for {
		chunk, err := s.tgClient.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: loc,
			Offset:   offset,
			Limit:    int(limit),
		})
		if err != nil {
			s.logger.Printf("UploadGetFile error at offset %d: %v", offset, err)
			return
		}
		switch v := chunk.(type) {
		case *tg.UploadFile:
			if len(v.Bytes) == 0 {
				return
			}
			if _, err := w.Write(v.Bytes); err != nil {
				return
			}
			offset += int64(len(v.Bytes))
			if offset >= size && size > 0 {
				return
			}
		case *tg.UploadFileCDNRedirect:
			http.Error(w, "CDN redirect not supported", http.StatusInternalServerError)
			return
		default:
			return
		}
	}
}

func (s *Server) getMessage(ctx context.Context, chatID int64, msgID int) (tg.MessageClass, error) {
	peer := s.tgCtx.PeerStorage.GetPeerById(chatID)
	if peer == nil {
		return nil, fmt.Errorf("peer not found for %d", chatID)
	}
	inputMsg := []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}}

	if peer.Type == int(storage.TypeChannel) {
		chPeer := s.tgCtx.PeerStorage.GetInputPeerById(chatID)
		if chPeer == nil {
			return nil, fmt.Errorf("channel peer not found")
		}
		inputCh, ok := chPeer.(*tg.InputPeerChannel)
		if !ok {
			return nil, fmt.Errorf("not a channel")
		}
		res, err := s.tgCtx.Raw.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: chatID, AccessHash: inputCh.AccessHash},
			ID:      inputMsg,
		})
		if err != nil {
			return nil, err
		}
		switch v := res.(type) {
		case *tg.MessagesChannelMessages:
			for _, m := range v.Messages {
				if msg, ok := m.(*tg.Message); ok && msg.ID == msgID {
					return msg, nil
				}
			}
		case *tg.MessagesMessages:
			for _, m := range v.Messages {
				if msg, ok := m.(*tg.Message); ok && msg.ID == msgID {
					return msg, nil
				}
			}
		}
		return nil, fmt.Errorf("message not found in channel response")
	}

	res, err := s.tgCtx.Raw.MessagesGetMessages(ctx, inputMsg)
	if err != nil {
		return nil, err
	}
	switch v := res.(type) {
	case *tg.MessagesMessages:
		for _, m := range v.Messages {
			if msg, ok := m.(*tg.Message); ok && msg.ID == msgID {
				return msg, nil
			}
		}
	case *tg.MessagesMessagesSlice:
		for _, m := range v.Messages {
			if msg, ok := m.(*tg.Message); ok && msg.ID == msgID {
				return msg, nil
			}
		}
	}
	return nil, fmt.Errorf("message not found")
}
