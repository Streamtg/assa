package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"webBridgeBot/internal/logger"
	"webBridgeBot/internal/reader"
	"github.com/spf13/viper"
)

const DefaultChunkSize int64 = 256 * 1024

type Configuration struct {
	ApiID               int
	ApiHash             string
	BotToken            string
	BaseURL             string
	Port                string
	HashLength          int
	CacheDirectory      string
	MaxCacheSize        int64
	DatabasePath        string
	DebugMode           bool
	LogLevel            string
	BinaryCache         *reader.BinaryCache
	LogChannelID        string
	RequestTimeout      int
	MaxRetries          int
	RetryBaseDelay      int
	MaxRetryDelay       int
	FirebaseCredentials string

	// ---- nuevos campos para túnel + Cloudflare ----
	TunnelUser           string
	TunnelTarget         string
	TunnelSSHPort        int
	TunnelLifetime       time.Duration
	TunnelMaxRetries     int
	TunnelRetryBaseDelay time.Duration
	CFAccountID          string
	CFAPIToken           string
	CFWorkerName         string
	CFWorkerTemplatePath string
}

func InitializeViper(log *logger.Logger, envFilePath string) {
	viper.AutomaticEnv()
	envFile := findEnvFile(envFilePath, log)
	if envFile != "" {
		viper.SetConfigFile(envFile)
		if err := viper.ReadInConfig(); err != nil {
			log.Infof("Could not read .env file at %s: %v", envFile, err)
			log.Info("Configuration will be loaded from environment variables.")
		} else {
			log.Infof("Loaded config from: %s", envFile)
		}
	} else {
		log.Info(".env not found, using environment variables.")
	}
}

func findEnvFile(customPath string, log *logger.Logger) string {
	if customPath != "" {
		if _, err := os.Stat(customPath); err == nil {
			return customPath
		}
		log.Warningf("Custom .env not found: %s", customPath)
		return ""
	}
	var searchPaths []string
	if execPath, err := os.Executable(); err == nil {
		searchPaths = append(searchPaths, filepath.Join(filepath.Dir(execPath), ".env"))
	}
	if cwd, err := os.Getwd(); err == nil {
		searchPaths = append(searchPaths, filepath.Join(cwd, ".env"))
	}
	for _, path := range searchPaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

func LoadConfig(log *logger.Logger) Configuration {
	var cfg Configuration
	cfg.ApiID = viper.GetInt("api_id")
	cfg.ApiHash = viper.GetString("api_hash")
	cfg.BotToken = viper.GetString("bot_token")
	cfg.BaseURL = viper.GetString("base_url")
	cfg.Port = viper.GetString("port")
	cfg.HashLength = viper.GetInt("hash_length")
	cfg.CacheDirectory = viper.GetString("cache_directory")
	cfg.MaxCacheSize = viper.GetInt64("max_cache_size")
	cfg.DebugMode = viper.GetBool("debug_mode")
	cfg.LogLevel = viper.GetString("log_level")
	cfg.LogChannelID = viper.GetString("log_channel_id")
	cfg.RequestTimeout = viper.GetInt("request_timeout")
	cfg.MaxRetries = viper.GetInt("max_retries")
	cfg.RetryBaseDelay = viper.GetInt("retry_base_delay")
	cfg.MaxRetryDelay = viper.GetInt("max_retry_delay")
	cfg.FirebaseCredentials = resolveFirebaseCredentials(
		viper.GetString("firebase_credentials"),
		cfg.CacheDirectory,
		log,
	)
	// ---- nuevos campos ----
	cfg.TunnelUser = viper.GetString("tunnel_user")
	cfg.TunnelTarget = viper.GetString("tunnel_target")
	cfg.TunnelSSHPort = viper.GetInt("tunnel_ssh_port")
	cfg.TunnelLifetime = viper.GetDuration("tunnel_lifetime")
	cfg.TunnelMaxRetries = viper.GetInt("tunnel_max_retries")
	cfg.TunnelRetryBaseDelay = viper.GetDuration("tunnel_retry_base_delay")
	cfg.CFAccountID = viper.GetString("cf_account_id")
	cfg.CFAPIToken = viper.GetString("cf_api_token")
	cfg.CFWorkerName = viper.GetString("cf_worker_name")
	cfg.CFWorkerTemplatePath = viper.GetString("cf_worker_template_path")

	setDefaultValues(&cfg)
	validateMandatoryFields(cfg, log)
	initializeBinaryCache(&cfg, log)
	if cfg.DebugMode {
		log.Debugf("Config loaded: %+v", cfg)
	}
	return cfg
}

func resolveFirebaseCredentials(raw string, cacheDir string, log *logger.Logger) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := os.Stat(raw); err == nil {
		abs, _ := filepath.Abs(raw)
		log.Infof("Firebase credentials loaded from file: %s", abs)
		return abs
	}
	if cwd, err := os.Getwd(); err == nil {
		full := filepath.Join(cwd, raw)
		if _, err := os.Stat(full); err == nil {
			log.Infof("Firebase credentials loaded from file: %s", full)
			return full
		}
	}
	if execPath, err := os.Executable(); err == nil {
		full := filepath.Join(filepath.Dir(execPath), raw)
		if _, err := os.Stat(full); err == nil {
			log.Infof("Firebase credentials loaded from file: %s", full)
			return full
		}
	}
	for _, name := range []string{"firebase-adminsdk.json", "firebase.json", "serviceAccountKey.json"} {
		if _, err := os.Stat(name); err == nil {
			abs, _ := filepath.Abs(name)
			log.Infof("Firebase credentials auto-detected: %s", abs)
			return abs
		}
	}
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Fatalf("Cannot create cache directory %s: %v", cacheDir, err)
	}
	tempPath := filepath.Join(cacheDir, "firebase-credentials.json")
	if strings.HasPrefix(raw, "{") {
		if err := os.WriteFile(tempPath, []byte(raw), 0600); err != nil {
			log.Fatalf("Failed to write Firebase credentials: %v", err)
		}
		log.Infof("Firebase credentials written from JSON env var to: %s", tempPath)
		return tempPath
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(raw)
	}
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(raw)
	}
	if err == nil && len(decoded) > 10 && strings.Contains(string(decoded), "project_id") {
		if err := os.WriteFile(tempPath, decoded, 0600); err != nil {
			log.Fatalf("Failed to write Firebase credentials: %v", err)
		}
		log.Infof("Firebase credentials decoded from Base64 to: %s", tempPath)
		return tempPath
	}
	if _, err := os.Stat(tempPath); err == nil {
		log.Infof("Firebase credentials using cached version: %s", tempPath)
		return tempPath
	}
	log.Info("Firebase credentials not found locally. Bot will try to fetch from log channel.")
	return ""
}

func validateMandatoryFields(cfg Configuration, log *logger.Logger) {
	if cfg.ApiID == 0 {
		log.Fatal("API_ID is required")
	}
	if cfg.ApiHash == "" {
		log.Fatal("API_HASH is required")
	}
	if cfg.BotToken == "" {
		log.Fatal("BOT_TOKEN is required")
	}
	if cfg.BaseURL == "" {
		log.Fatal("BASE_URL is required")
	}
}

func setDefaultValues(cfg *Configuration) {
	if cfg.HashLength < 6 {
		cfg.HashLength = 8
	}
	if cfg.CacheDirectory == "" {
		cfg.CacheDirectory = ".cache"
	}
	if cfg.MaxCacheSize == 0 {
		cfg.MaxCacheSize = 10 * 1024 * 1024 * 1024
	}
	if cfg.DatabasePath == "" {
		cfg.DatabasePath = fmt.Sprintf("%s/webBridgeBot.db", cfg.CacheDirectory)
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.LogLevel == "" {
		if cfg.DebugMode {
			cfg.LogLevel = "DEBUG"
		} else {
			cfg.LogLevel = "INFO"
		}
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 300
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 10
	}
	if cfg.RetryBaseDelay == 0 {
		cfg.RetryBaseDelay = 1
	}
	if cfg.MaxRetryDelay == 0 {
		cfg.MaxRetryDelay = 60
	}
	if cfg.TunnelUser == "" {
		cfg.TunnelUser = "a.pinggy.io"
	}
	if cfg.TunnelTarget == "" {
		cfg.TunnelTarget = "0:127.0.0.1:8080"
	}
	if cfg.TunnelSSHPort == 0 {
		cfg.TunnelSSHPort = 443
	}
	if cfg.TunnelLifetime == 0 {
		cfg.TunnelLifetime = 60 * time.Minute
	}
	if cfg.TunnelMaxRetries == 0 {
		cfg.TunnelMaxRetries = 10
	}
	if cfg.TunnelRetryBaseDelay == 0 {
		cfg.TunnelRetryBaseDelay = 2 * time.Second
	}
}

func initializeBinaryCache(cfg *Configuration, log *logger.Logger) {
	var err error
	cfg.BinaryCache, err = reader.NewBinaryCache(
		cfg.CacheDirectory,
		cfg.MaxCacheSize,
		DefaultChunkSize,
	)
	if err != nil {
		log.Fatalf("Error initializing BinaryCache: %v", err)
	}
}
