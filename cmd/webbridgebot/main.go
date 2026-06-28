package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"webBridgeBot/internal/bot"
	"webBridgeBot/internal/config"
	"webBridgeBot/internal/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfg config.Configuration
var envFilePath string

func main() {
	log := logger.NewDefault("webBridgeBot: ")

	rootCmd := &cobra.Command{
		Use:   "webBridgeBot",
		Short: "WebBridgeBot",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			config.InitializeViper(log, envFilePath)
			return viper.BindPFlags(cmd.Flags())
		},
		Run: func(cmd *cobra.Command, args []string) {
			cfg = config.LoadConfig(log)
			log.SetLevel(logger.ParseLogLevel(cfg.LogLevel))
			log.Infof("Log level set to: %s", cfg.LogLevel)

			defer func() {
				log.Info("Closing binary cache...")
				if err := cfg.BinaryCache.Close(); err != nil {
					log.Errorf("Error closing binary cache: %v", err)
				}
			}()

			b, err := bot.NewTelegramBot(&cfg, log)
			if err != nil {
				log.Fatalf("Error initializing Telegram bot: %v", err)
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			go func() {
				b.Run(ctx)
				stop()
			}()

			log.Info("Bot is running. Press Ctrl+C to exit.")
			<-ctx.Done()
			log.Info("Shutdown signal received, initiating graceful shutdown...")
		},
	}

	rootCmd.Flags().StringVar(&envFilePath, "env_file", "", "Path to .env file (default: searches in executable directory and current directory)")
	rootCmd.Flags().IntVar(&cfg.ApiID, "api_id", 0, "Telegram API ID (required)")
	rootCmd.Flags().StringVar(&cfg.ApiHash, "api_hash", "", "Telegram API Hash (required)")
	rootCmd.Flags().StringVar(&cfg.BotToken, "bot_token", "", "Telegram Bot Token (required)")
	rootCmd.Flags().StringVar(&cfg.BaseURL, "base_url", "", "Base URL for the web interface (required)")
	rootCmd.Flags().StringVar(&cfg.Port, "port", "8080", "Port for the web server (default 8080)")
	rootCmd.Flags().IntVar(&cfg.HashLength, "hash_length", 8, "Length of the short hash for file URLs (default 8)")
	rootCmd.Flags().StringVar(&cfg.CacheDirectory, "cache_directory", ".cache", "Directory to store cached files and database (default .cache)")
	rootCmd.Flags().Int64Var(&cfg.MaxCacheSize, "max_cache_size", 10*1024*1024*1024, "Maximum cache size in bytes (default 10GB)")
	rootCmd.Flags().BoolVar(&cfg.DebugMode, "debug_mode", false, "Enable debug logging (default false)")
	rootCmd.Flags().StringVar(&cfg.LogLevel, "log_level", "INFO", "Log level: DEBUG, INFO, WARNING, ERROR (default INFO, or DEBUG if debug_mode=true)")
	rootCmd.Flags().StringVar(&cfg.LogChannelID, "log_channel_id", "0", "Optional: Telegram Channel ID or @username to forward all media to (for logging)")
	rootCmd.Flags().StringVar(&cfg.TunnelUser, "tunnel_user", "a.pinggy.io", "Pinggy SSH host (free version)")
	rootCmd.Flags().StringVar(&cfg.TunnelTarget, "tunnel_target", "0:127.0.0.1:8080", "Pinggy remote target")
	rootCmd.Flags().IntVar(&cfg.TunnelSSHPort, "tunnel_ssh_port", 443, "Pinggy SSH port")
	rootCmd.Flags().DurationVar(&cfg.TunnelLifetime, "tunnel_lifetime", 60*time.Minute, "Tunnel renewal interval")
	rootCmd.Flags().IntVar(&cfg.TunnelMaxRetries, "tunnel_max_retries", 10, "Max retries on tunnel failure")
	rootCmd.Flags().DurationVar(&cfg.TunnelRetryBaseDelay, "tunnel_retry_base_delay", 2*time.Second, "Base delay for tunnel backoff")
	rootCmd.Flags().StringVar(&cfg.CFAccountID, "cf_account_id", "", "Cloudflare Account ID")
	rootCmd.Flags().StringVar(&cfg.CFAPIToken, "cf_api_token", "", "Cloudflare API Token")
	rootCmd.Flags().StringVar(&cfg.CFWorkerName, "cf_worker_name", "", "Cloudflare Worker script name")
	rootCmd.Flags().StringVar(&cfg.CFWorkerTemplatePath, "cf_worker_template", "", "Path to Cloudflare Worker template (optional)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
