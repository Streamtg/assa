package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"syscall"
	"github.com/gorilla/mux"
	"github.com/gotd/td/tg"
)

var tmplPath = "player.html"

func isClientDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	patterns := []string{"broken pipe", "connection reset by peer", "connection reset", "client disconnected", "write: connection reset", "readfrom tcp"}
	for _, p := range patterns {
		if strings.Contains(errMsg, p) {
			return true
		}
	}
	return false
}

func (s *Server) handlePlayer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	chatID, err := parseChatID(vars)
	if err != nil {
		http.Error(w, "Invalid chat ID", http.StatusBadRequest)
		return
	}
	userInfo, err := s.userRepository.GetUserInfo(chatID)
	if err != nil || userInfo == nil || !userInfo.IsAuthorized {
		http.Error(w, "Unauthorized access to player. Please start the bot first.", http.StatusUnauthorized)
		return
	}
	t, err := template.ParseFiles(tmplPath)
	if err != nil {
		s.logger.Printf("Error loading template: %v", err)
		http.Error(w, "Failed to load template", http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, map[string]interface{}{"User": userInfo}); err != nil {
		s.logger.Printf("Error rendering template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
	}
}

func (s *Server) handleAvatar(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	chatID, err := parseChatID(vars)
	if err != nil {
		http.Error(w, "Invalid chat ID", http.StatusBadRequest)
		return
	}
	userInfo, err := s.userRepository.GetUserInfo(chatID)
	if err != nil || userInfo == nil || !userInfo.IsAuthorized {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	ctx := r.Context()
	peer := s.tgCtx.PeerStorage.GetInputPeerById(chatID)
	var inputUser tg.InputUserClass
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		inputUser = &tg.InputUser{UserID: p.UserID, AccessHash: p.AccessHash}
	case *tg.InputPeerSelf:
		inputUser = &tg.InputUserSelf{}
	default:
		http.Error(w, "User peer not found", http.StatusNotFound)
		return
	}

	var photosRes tg.PhotosPhotosClass
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		photosRes, err = s.tgClient.API().PhotosGetUserPhotos(ctx, &tg.PhotosGetUserPhotosRequest{
			UserID: inputUser, Offset: 0, MaxID: 0, Limit: 1,
		})
		if err == nil {
			break
		}
		if floodWait, isFlood := utils.ExtractFloodWait(err); isFlood && attempt < maxRetries-1 {
			s.logger.Printf("Avatar: FLOOD_WAIT for %d, waiting %d seconds (attempt %d/%d)", chatID, floodWait, attempt+1, maxRetries)
			time.Sleep(time.Duration(floodWait) * time.Second)
			continue
		}
		break
	}
	if err != nil {
		s.logger.Printf("Avatar: failed PhotosGetUserPhotos for %d: %v", chatID, err)
		http.NotFound(w, r)
		return
	}

	var photo *tg.Photo
	switch pr := photosRes.(type) {
	case *tg.PhotosPhotos:
		if len(pr.Photos) == 0 {
			http.NotFound(w, r)
			return
		}
		if p, ok := pr.Photos[0].(*tg.Photo); ok {
			photo = p
		}
	case *tg.PhotosPhotosSlice:
		if len(pr.Photos) == 0 {
			http.NotFound(w, r)
			return
		}
		if p, ok := pr.Photos[0].(*tg.Photo); ok {
			photo = p
		}
	default:
		http.NotFound(w, r)
		return
	}
	if photo == nil || photo.AccessHash == 0 {
		http.NotFound(w, r)
		return
	}

	thumbType := "x"
	var sizeBytes int
	for _, sz := range photo.Sizes {
		if ps, ok := sz.(*tg.PhotoSize); ok {
			if ps.Type == "x" {
				thumbType = ps.Type
				sizeBytes = ps.Size
				break
			}
			if sizeBytes == 0 && ps.Size > 0 {
				thumbType = ps.Type
				sizeBytes = ps.Size
			}
		}
	}
	if sizeBytes <= 0 {
		sizeBytes = 256 * 1024
	}
	loc := &tg.InputPhotoFileLocation{
		ID: photo.ID, AccessHash: photo.AccessHash,
		FileReference: photo.FileReference, ThumbSize: thumbType,
	}
	start := int64(0)
	end := int64(sizeBytes - 1)
	if end < 0 {
		end = 0
	}

	// Use the existing Telegram reader if available, otherwise fallback simple
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	if sizeBytes > 0 {
		w.Header().Set("Content-Length", strconv.Itoa(sizeBytes))
	}
	w.WriteHeader(http.StatusOK)
	s.logger.Printf("Avatar: stub response for chatID %d", chatID)
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	externalURL := r.URL.Query().Get("url")
	if externalURL == "" {
		http.Error(w, "Missing 'url' parameter", http.StatusBadRequest)
		return
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(externalURL)
	if err != nil {
		s.logger.Printf("Error fetching external URL %s: %v", externalURL, err)
		http.Error(w, fmt.Sprintf("Failed to fetch resource: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Range, Content-Type")
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) handleValidateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)
	requestingChatID, err := parseChatID(vars)
	if err != nil {
		http.Error(w, "Invalid chat ID", http.StatusBadRequest)
		return
	}
	requestingUser, err := s.userRepository.GetUserInfo(requestingChatID)
	if err != nil || requestingUser == nil || !requestingUser.IsAuthorized {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	userIDStr := r.URL.Query().Get("userId")
	if userIDStr == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Missing userId parameter"})
		return
	}
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid user ID format"})
		return
	}
	userInfo, err := s.userRepository.GetUserInfo(userID)
	if err != nil || userInfo == nil || !userInfo.IsAuthorized {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "User not found or not authorized"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"userID": userInfo.UserID, "chatID": userInfo.ChatID,
		"firstName": userInfo.FirstName, "lastName": userInfo.LastName,
		"username": userInfo.Username,
	})
}

func (s *Server) handleConnectionStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	vars := mux.Vars(r)
	requestingChatID, err := parseChatID(vars)
	if err != nil {
		http.Error(w, "Invalid chat ID", http.StatusBadRequest)
		return
	}
	requestingUser, err := s.userRepository.GetUserInfo(requestingChatID)
	if err != nil || requestingUser == nil || !requestingUser.IsAuthorized {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	stats := s.connTracker.GetStatistics()
	stats["current_active"] = s.connTracker.GetActiveConnections()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleFavicon(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRobots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("User-agent: *\nDisallow: /\n"))
}

func (s *Server) handleSitemap(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func (s *Server) handleWellKnown(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["path"] == "security.txt" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("# Security Policy\n# Contact the bot administrator for security issues.\n"))
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("Unauthorized. Please use the Telegram bot to authenticate.\n"))
}
