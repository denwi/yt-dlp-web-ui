package downloaders

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/marcopiovanello/yt-dlp-web-ui/v4/server/internal"
)

const (
	maxReconnectAttempts = 5
	reconnectDelaySecs   = 5
)

var transientErrorPatterns = []string{
	"connection reset by peer",
	"timeout",
	"network is unreachable",
	"no route to host",
	"etimedout",
}

func argsSanitizer(params []string) []string {
	params = slices.DeleteFunc(params, func(e string) bool {
		match, _ := regexp.MatchString(`(\$\{)|(\&\&)`, e)
		return match
	})

	params = slices.DeleteFunc(params, func(e string) bool {
		return e == ""
	})

	return params
}

func buildFilename(o *internal.DownloadOutput) {
	if o.Filename != "" && strings.Contains(o.Filename, ".%(ext)s") {
		o.Filename += ".%(ext)s"
	}

	o.Filename = strings.Replace(
		o.Filename,
		".%(ext)s.%(ext)s",
		".%(ext)s",
		1,
	)
}

func produceLogs(r io.Reader, logs chan<- []byte) {
	go func() {
		scanner := bufio.NewScanner(r)

		for scanner.Scan() {
			logs <- scanner.Bytes()
		}
	}()
}

func consumeLogs(ctx context.Context, logs <-chan []byte, c LogConsumer, d Downloader) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("detaching logs",
				slog.String("url", d.GetUrl()),
				slog.String("id", c.GetName()),
			)
			return
		case entry := <-logs:
			c.ParseLogEntry(entry, d)
		}
	}
}

// isTransientError checks if an error matches transient network error patterns
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	for _, pattern := range transientErrorPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// sleepWithBackoff sleeps with exponential backoff
func sleepWithBackoff(attempt int) time.Duration {
	delay := float64(reconnectDelaySecs) * math.Pow(2, float64(attempt))
	return time.Duration(int(delay))
}

func printYtDlpErrors(stdout io.Reader, shortId, url string) {
	scanner := bufio.NewScanner(stdout)

	for scanner.Scan() {
		slog.Error("yt-dlp process error",
			slog.String("id", shortId),
			slog.String("url", url),
			slog.String("err", scanner.Text()),
		)
	}
}

// retryOnTransientError retries running a function with exponential backoff for transient errors
func retryOnTransientError(fn func() error, id, url string) error {
	for attempt := 0; attempt < maxReconnectAttempts; attempt++ {
		err := fn()

		if err == nil || !isTransientError(err) {
			return err
		}

		slog.Warn("yt-dlp command failed with transient error, retrying",
			slog.String("id", id),
			slog.String("url", url),
			slog.Int("attempt", attempt+1),
			slog.Int("maxAttempts", maxReconnectAttempts),
			slog.String("error", err.Error()),
		)

		sleepWithBackoff(attempt)
	}

	return fn()
}
