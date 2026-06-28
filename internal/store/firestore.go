package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go/v4"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FireStore struct {
	client *firestore.Client
	ctx    context.Context
}

type User struct {
	UserID          int64  `firestore:"user_id"`
	ChatID          int64  `firestore:"chat_id"`
	FirstName       string `firestore:"first_name"`
	LastName        string `firestore:"last_name"`
	Username        string `firestore:"username"`
	IsAuthorized    bool   `firestore:"is_authorized"`
	IsAdmin         bool   `firestore:"is_admin"`
	IsBanned        bool   `firestore:"is_banned"`
	IsSilenced      bool   `firestore:"is_silenced"`
	Warnings        int    `firestore:"warnings"`
	FileSizeLimitMB int    `firestore:"file_size_limit_mb"`
	TotalFiles      int64  `firestore:"total_files"`
	TotalBytes      int64  `firestore:"total_bytes"`
	ExpiresAt       string `firestore:"expires_at,omitempty"`
	LastSeenAt      string `firestore:"last_seen_at,omitempty"`
	CreatedAt       string `firestore:"created_at"`
	UpdatedAt       string `firestore:"updated_at"`
}

type MediaFile struct {
	PublicID          string `firestore:"public_id"`
	UserID            int64  `firestore:"user_id"`
	ChatID            int64  `firestore:"chat_id"`
	OriginalMessageID int    `firestore:"original_message_id"`
	LogChannelID      string `firestore:"log_channel_id"`
	LogMessageID      int    `firestore:"log_message_id"`
	FileID            int64  `firestore:"file_id"`
	FileName          string `firestore:"file_name"`
	MimeType          string `firestore:"mime_type"`
	FileSize          int64  `firestore:"file_size"`
	Duration          int    `firestore:"duration"`
	Width             int    `firestore:"width"`
	Height            int    `firestore:"height"`
	Title             string `firestore:"title"`
	Performer         string `firestore:"performer"`
	Hash              string `firestore:"hash"`
	PublicURL         string `firestore:"public_url"`
	IsForwarded       bool   `firestore:"is_forwarded"`
	CreatedAt         string `firestore:"created_at"`
}

type ActivityEntry struct {
	UserID    int64  `firestore:"user_id"`
	Action    string `firestore:"action"`
	Detail    string `firestore:"detail"`
	CreatedAt string `firestore:"created_at"`
}

type ReportEntry struct {
	UserID     int64  `firestore:"user_id"`
	ReportType string `firestore:"report_type"`
	Message    string `firestore:"message"`
	Status     string `firestore:"status"`
	CreatedAt  string `firestore:"created_at"`
}

func NewFireStore(credentialsPath string) (*FireStore, error) {
	ctx := context.Background()
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile(credentialsPath))
	if err != nil {
		return nil, fmt.Errorf("firebase init: %w", err)
	}
	client, err := app.Firestore(ctx)
	if err != nil {
		return nil, fmt.Errorf("firestore init: %w", err)
	}
	return &FireStore{client: client, ctx: ctx}, nil
}

func (fs *FireStore) Close() {
	if fs.client != nil {
		fs.client.Close()
	}
}

func now() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

func userKey(uid int64) string {
	return fmt.Sprintf("%d", uid)
}

// ─── Users ──────────────────────────────────────────────────────────────────────

func (fs *FireStore) GetUser(uid int64) (*User, error) {
	doc, err := fs.client.Collection("users").Doc(userKey(uid)).Get(fs.ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	var u User
	if err := doc.DataTo(&u); err != nil {
		return nil, err
	}
	return &u, nil
}

func (fs *FireStore) SaveUser(u *User) error {
	_, err := fs.client.Collection("users").Doc(userKey(u.UserID)).Set(fs.ctx, u)
	return err
}

func (fs *FireStore) UpdateUserFields(uid int64, fields map[string]interface{}) error {
	updates := make([]firestore.Update, 0, len(fields))
	for k, v := range fields {
		updates = append(updates, firestore.Update{Path: k, Value: v})
	}
	_, err := fs.client.Collection("users").Doc(userKey(uid)).Update(fs.ctx, updates)
	return err
}

// UpdateUser is an alias for UpdateUserFields (used by telegram_bot.go)
func (fs *FireStore) UpdateUser(uid int64, fields map[string]interface{}) error {
	return fs.UpdateUserFields(uid, fields)
}

// TouchUser updates the last_seen_at and updated_at timestamps
func (fs *FireStore) TouchUser(uid int64) error {
	n := now()
	return fs.UpdateUserFields(uid, map[string]interface{}{
		"last_seen_at": n,
		"updated_at":   n,
	})
}

func (fs *FireStore) DeleteUser(uid int64) error {
	_, err := fs.client.Collection("users").Doc(userKey(uid)).Delete(fs.ctx)
	return err
}

func (fs *FireStore) CountUsers() (int64, error) {
	iter := fs.client.Collection("users").Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (fs *FireStore) CountAuthorizedUsers() (int64, error) {
	iter := fs.client.Collection("users").Where("is_authorized", "==", true).Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// CountAuthorized is an alias for CountAuthorizedUsers (used by telegram_bot.go)
func (fs *FireStore) CountAuthorized() (int64, error) {
	return fs.CountAuthorizedUsers()
}

func (fs *FireStore) ListUsers(offset, limit int) ([]User, int, error) {
	iter := fs.client.Collection("users").OrderBy("created_at", firestore.Asc).Documents(fs.ctx)
	defer iter.Stop()
	var all []User
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		var u User
		if doc.DataTo(&u) == nil {
			all = append(all, u)
		}
	}
	total := len(all)
	if offset >= total {
		return nil, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return all[offset:end], total, nil
}

func (fs *FireStore) GetBroadcastUsers(authorizedOnly bool) ([]User, error) {
	q := fs.client.Collection("users").Where("is_banned", "==", false)
	if authorizedOnly {
		q = q.Where("is_authorized", "==", true)
	}
	iter := q.Documents(fs.ctx)
	defer iter.Stop()
	var users []User
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var u User
		if doc.DataTo(&u) == nil {
			users = append(users, u)
		}
	}
	return users, nil
}

func (fs *FireStore) CheckExpiredUsers() error {
	n := now()
	iter := fs.client.Collection("users").Where("is_authorized", "==", true).Documents(fs.ctx)
	defer iter.Stop()
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		var u User
		if doc.DataTo(&u) != nil {
			continue
		}
		if u.ExpiresAt != "" && u.ExpiresAt < n {
			_ = fs.UpdateUserFields(u.UserID, map[string]interface{}{"is_authorized": false})
		}
	}
	return nil
}

// CheckExpired is an alias for CheckExpiredUsers (used by telegram_bot.go)
func (fs *FireStore) CheckExpired() {
	_ = fs.CheckExpiredUsers()
}

// ─── Media Files ────────────────────────────────────────────────────────────────

func (fs *FireStore) SaveMedia(m *MediaFile) error {
	_, err := fs.client.Collection("media").Doc(m.PublicID).Set(fs.ctx, m)
	return err
}

func (fs *FireStore) CountMedia() (int64, error) {
	iter := fs.client.Collection("media").Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (fs *FireStore) CountMediaByUser(uid int64) (int64, error) {
	iter := fs.client.Collection("media").Where("user_id", "==", uid).Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (fs *FireStore) CountMediaToday() (int64, error) {
	today := time.Now().UTC().Format("2006-01-02")
	iter := fs.client.Collection("media").Where("created_at", ">=", today).Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (fs *FireStore) CountMediaTodayByUser(uid int64) (int64, error) {
	today := time.Now().UTC().Format("2006-01-02")
	iter := fs.client.Collection("media").Where("user_id", "==", uid).Where("created_at", ">=", today).Documents(fs.ctx)
	defer iter.Stop()
	var count int64
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (fs *FireStore) TotalMediaSize() (int64, error) {
	iter := fs.client.Collection("media").Documents(fs.ctx)
	defer iter.Stop()
	var total int64
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			total += m.FileSize
		}
	}
	return total, nil
}

func (fs *FireStore) GetUserMedia(uid int64, offset, limit int) ([]MediaFile, int64, error) {
	iter := fs.client.Collection("media").Where("user_id", "==", uid).OrderBy("created_at", firestore.Desc).Documents(fs.ctx)
	defer iter.Stop()
	var all []MediaFile
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			all = append(all, m)
		}
	}
	total := int64(len(all))
	if offset >= len(all) {
		return []MediaFile{}, total, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], total, nil
}

func (fs *FireStore) GetUserMediaFiltered(uid int64, mimePrefix string, limit int) ([]MediaFile, error) {
	iter := fs.client.Collection("media").Where("user_id", "==", uid).OrderBy("created_at", firestore.Desc).Documents(fs.ctx)
	defer iter.Stop()
	var result []MediaFile
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			if mimePrefix == "" || strings.HasPrefix(m.MimeType, mimePrefix) {
				result = append(result, m)
				if len(result) >= limit {
					break
				}
			}
		}
	}
	return result, nil
}

func (fs *FireStore) SearchMedia(uid int64, query string, limit int) ([]MediaFile, error) {
	iter := fs.client.Collection("media").Where("user_id", "==", uid).OrderBy("created_at", firestore.Desc).Documents(fs.ctx)
	defer iter.Stop()
	qlower := strings.ToLower(query)
	var result []MediaFile
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			if strings.Contains(strings.ToLower(m.FileName), qlower) {
				result = append(result, m)
				if len(result) >= limit {
					break
				}
			}
		}
	}
	return result, nil
}

func (fs *FireStore) GetAllUserMedia(uid int64) ([]MediaFile, error) {
	iter := fs.client.Collection("media").Where("user_id", "==", uid).OrderBy("created_at", firestore.Desc).Documents(fs.ctx)
	defer iter.Stop()
	var all []MediaFile
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			all = append(all, m)
		}
	}
	return all, nil
}

// GetMediaByHash looks up a media file by its 8-char hash (used by web server)
func (fs *FireStore) GetMediaByHash(hash string) (*MediaFile, error) {
	iter := fs.client.Collection("media").Where("hash", "==", hash).Limit(1).Documents(fs.ctx)
	defer iter.Stop()
	doc, err := iter.Next()
	if err != nil {
		return nil, err
	}
	var mf MediaFile
	if err := doc.DataTo(&mf); err != nil {
		return nil, err
	}
	return &mf, nil
}

// ─── Activity Log ───────────────────────────────────────────────────────────────

// LogActivity saves an activity entry. Does not return error (fire-and-forget).
func (fs *FireStore) LogActivity(uid int64, action, detail string) {
	_, _, _ = fs.client.Collection("activity").Add(fs.ctx, ActivityEntry{
		UserID: uid, Action: action, Detail: detail, CreatedAt: now(),
	})
}

func (fs *FireStore) GetUserActivity(uid int64, limit int) ([]ActivityEntry, error) {
	iter := fs.client.Collection("activity").Where("user_id", "==", uid).OrderBy("created_at", firestore.Desc).Limit(limit).Documents(fs.ctx)
	defer iter.Stop()
	var entries []ActivityEntry
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var e ActivityEntry
		if doc.DataTo(&e) == nil {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// CleanupOldActivity deletes old activity logs. Returns count only (used by telegram_bot.go).
func (fs *FireStore) CleanupOldActivity(days int) int64 {
	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02 15:04:05")
	iter := fs.client.Collection("activity").Where("created_at", "<", cutoff).Documents(fs.ctx)
	defer iter.Stop()
	batch := fs.client.Batch()
	var count int64
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			break
		}
		batch.Delete(doc.Ref)
		count++
		if count%400 == 0 {
			_, _ = batch.Commit(fs.ctx)
			batch = fs.client.Batch()
		}
	}
	if count%400 != 0 {
		_, _ = batch.Commit(fs.ctx)
	}
	return count
}

// ─── Reports ────────────────────────────────────────────────────────────────────

// SaveReport with 3 args (used internally)
func (fs *FireStore) saveReportFull(uid int64, reportType, message string) error {
	_, _, err := fs.client.Collection("reports").Add(fs.ctx, ReportEntry{
		UserID: uid, ReportType: reportType, Message: message, Status: "open", CreatedAt: now(),
	})
	return err
}

// SaveReport with 2 args (used by telegram_bot.go) — defaults report_type to "user_report"
func (fs *FireStore) SaveReport(uid int64, message string) error {
	return fs.saveReportFull(uid, "user_report", message)
}

// ─── Settings ───────────────────────────────────────────────────────────────────

func (fs *FireStore) GetBlacklist() (map[string]bool, error) {
	doc, err := fs.client.Collection("settings").Doc("blacklist").Get(fs.ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return make(map[string]bool), nil
		}
		return nil, err
	}
	data := doc.Data()
	result := make(map[string]bool)
	if exts, ok := data["extensions"].([]interface{}); ok {
		for _, e := range exts {
			if s, ok := e.(string); ok {
				result[s] = true
			}
		}
	}
	return result, nil
}

func (fs *FireStore) SetBlacklist(exts map[string]bool) error {
	list := make([]string, 0, len(exts))
	for e := range exts {
		list = append(list, e)
	}
	sort.Strings(list)
	_, err := fs.client.Collection("settings").Doc("blacklist").Set(fs.ctx, map[string]interface{}{
		"extensions": list,
		"updated_at": now(),
	})
	return err
}

// ─── Backup Export ──────────────────────────────────────────────────────────────

func (fs *FireStore) ExportAllUsers() ([]User, error) {
	iter := fs.client.Collection("users").OrderBy("created_at", firestore.Asc).Documents(fs.ctx)
	defer iter.Stop()
	var users []User
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var u User
		if doc.DataTo(&u) == nil {
			users = append(users, u)
		}
	}
	return users, nil
}

func (fs *FireStore) ExportAllMedia() ([]MediaFile, error) {
	iter := fs.client.Collection("media").OrderBy("created_at", firestore.Asc).Documents(fs.ctx)
	defer iter.Stop()
	var media []MediaFile
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var m MediaFile
		if doc.DataTo(&m) == nil {
			media = append(media, m)
		}
	}
	return media, nil
}
