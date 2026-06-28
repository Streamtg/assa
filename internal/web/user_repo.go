package web

import (
	"webBridgeBot/internal/store"
)

type UserInfo struct {
	UserID       int64
	ChatID       int64
	FirstName    string
	LastName     string
	Username     string
	IsAuthorized bool
	IsAdmin      bool
}

type UserRepoAdapter struct {
	fs *store.FireStore
}

func NewUserRepoAdapter(fs *store.FireStore) *UserRepoAdapter {
	return &UserRepoAdapter{fs: fs}
}

func (a *UserRepoAdapter) GetUserInfo(chatID int64) (*UserInfo, error) {
	u, err := a.fs.GetUser(chatID)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, nil
	}
	return &UserInfo{
		UserID:       u.UserID,
		ChatID:       u.ChatID,
		FirstName:    u.FirstName,
		LastName:     u.LastName,
		Username:     u.Username,
		IsAuthorized: u.IsAuthorized,
		IsAdmin:      u.IsAdmin,
	}, nil
}
