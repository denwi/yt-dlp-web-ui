package downloaders

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/marcopiovanello/yt-dlp-web-ui/v4/server/common"
	"github.com/marcopiovanello/yt-dlp-web-ui/v4/server/config"
	"github.com/marcopiovanello/yt-dlp-web-ui/v4/server/internal"
)

const downloadTemplate = `download:
{
	"eta":%(progress.eta)s,
	"percentage":"%(progress._percent_str)s",
	"speed":%(progress.speed)s
}`

// filename not returning the correct extension after postprocess
const postprocessTemplate = `postprocess:
{
	"filepath":"%(info.filepath)s"
}
`

type GenericDownloader struct {
	Params []string

	AutoRemove bool

	progress internal.DownloadProgress
	output   internal.DownloadOutput

	proc *os.Process

	logConsumer LogConsumer

	// embedded
	DownloaderBase
}

func NewGenericDownload(url string, params []string) Downloader {
	g := &GenericDownloader{
		logConsumer: NewJSONLogConsumer(),
	}
	// in base
	g.Id = uuid.NewString()
	g.URL = url
	g.Params = params
	g.Completed = false

	return g
}

func (g *GenericDownloader) Start() error {
	g.SetPending(true)
	g.Params = argsSanitizer(g.Params)

	out := internal.DownloadOutput{
		Path:     config.Instance().Paths.DownloadPath,
		Filename: "%(title).150B [%(id)s].%(ext)s",
	}

	if g.output.Path != "" {
		out.Path = g.output.Path
	}

	if g.output.Filename != "" {
		out.Filename = g.output.Filename
	}

	buildFilename(&out)
	g.output.Filename = out.Filename

	templateReplacer := strings.NewReplacer("\n", "", "\t", "", " ", "")

	baseParams := []string{
		strings.Split(g.URL, "?list")[0],
		"--newline",
		"--no-colors",
		"--no-playlist",
		"--progress-template",
		templateReplacer.Replace(downloadTemplate),
		"--progress-template",
		templateReplacer.Replace(postprocessTemplate),
		"--no-exec",
		"--js-runtimes",
		config.Instance().Paths.JSRuntimePath,
		"--remote-components",
		"ejs:github",
	}

	if !(slices.Contains(g.Params, "-P") || slices.Contains(g.Params, "--paths")) {
		g.Params = append(g.Params, "-o")
		g.Params = append(g.Params, fmt.Sprintf("%s/%s", out.Path, g.output.Filename))
	}

	params := append(baseParams, g.Params...)

	const maxRetries = 5
	var err error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		slog.Info("requesting download",
			slog.String("url", g.URL),
			slog.Int("attempt", attempt+1),
			slog.Int("maxAttempts", maxRetries+1),
			slog.Any("params", params),
		)

		cmd := exec.Command(config.Instance().Paths.DownloaderPath, params...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		stdout, pipeErr := cmd.StdoutPipe()
		if pipeErr != nil {
			err = pipeErr
			break
		}

		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf

		if err = cmd.Start(); err != nil {
			slog.Error("failed to start yt-dlp process", slog.Any("err", err))
			break
		}

		g.proc = cmd.Process
		g.SetPending(false)

		ctx, cancel := context.WithCancel(context.Background())
		logs := make(chan []byte)

		go produceLogs(stdout, logs)
		go consumeLogs(ctx, logs, g.logConsumer, g)

		err = cmd.Wait()

		stdout.Close()
		cancel()
		g.proc = nil

		stderrOutput := stderrBuf.String()

		if stderrOutput != "" {
			printYtDlpErrors(bytes.NewReader(stderrBuf.Bytes()), g.Id, g.URL)
		}

		if err == nil {
			g.progress.Status = internal.StatusCompleted
			break
		}

		transient := isTransientError(err)

		if stderrOutput != "" {
			lowerStderr := strings.ToLower(stderrOutput)
			transient = transient ||
				isTransientError(errors.New(stderrOutput)) ||
				strings.Contains(lowerStderr, "read timed out") ||
				strings.Contains(lowerStderr, "connection timed out") ||
				strings.Contains(lowerStderr, "unexpected_eof_while_reading") ||
				strings.Contains(lowerStderr, "transporterror") ||
				strings.Contains(lowerStderr, "sslerror") ||
				strings.Contains(lowerStderr, "connection reset") ||
				strings.Contains(lowerStderr, "connection aborted") ||
				strings.Contains(lowerStderr, "failed to resolve") ||
				strings.Contains(lowerStderr, "try again") ||
				strings.Contains(lowerStderr, "unable to download video data") ||
				strings.Contains(lowerStderr, "error") ||
				strings.Contains(lowerStderr, "unable to download streaks api info")
		}

		if !transient {
			slog.Error("yt-dlp failed with a permanent error",
				slog.String("id", g.Id),
				slog.String("error", err.Error()),
			)
			break
		}

		if attempt == maxRetries {
			slog.Error("yt-dlp exhausted retry attempts",
				slog.String("id", g.Id),
				slog.String("url", g.URL),
				slog.Int("attempts", maxRetries+1),
				slog.String("error", err.Error()),
			)
			break
		}

		slog.Warn("yt-dlp encountered a transient error, retrying",
			slog.String("id", g.Id),
			slog.Int("attempt", attempt+1),
			slog.Int("maxRetries", maxRetries),
			slog.String("error", err.Error()),
		)

		sleepWithBackoff(attempt)
	}

	if err != nil {
		g.progress.Status = internal.StatusErrored
	}

	g.proc = nil
	g.Complete()

	return err
}

func (g *GenericDownloader) Stop() error {
	defer func() {
		g.progress.Status = internal.StatusCompleted
		g.Complete()
	}()
	// yt-dlp uses multiple child process the parent process
	// has been spawned with setPgid = true. To properly kill
	// all subprocesses a SIGTERM need to be sent to the correct
	// process group
	if g.proc == nil {
		return errors.New("*os.Process not set")
	}

	pgid, err := syscall.Getpgid(g.proc.Pid)
	if err != nil {
		return err
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		return err
	}

	return nil
}

func (g *GenericDownloader) Status() internal.ProcessSnapshot {
	return internal.ProcessSnapshot{
		Id:             g.Id,
		Info:           g.Metadata,
		Progress:       g.progress,
		Output:         g.output,
		Params:         g.Params,
		DownloaderName: "generic",
	}
}

func (g *GenericDownloader) UpdateSavedFilePath(p string) { g.output.SavedFilePath = p }

func (g *GenericDownloader) SetOutput(o internal.DownloadOutput)     { g.output = o }
func (g *GenericDownloader) SetProgress(p internal.DownloadProgress) { g.progress = p }

func (g *GenericDownloader) SetMetadata(fetcher func(url string) (*common.DownloadMetadata, error)) {
	g.FetchMetadata(fetcher)
}

func (g *GenericDownloader) SetPending(p bool) {
	g.Pending = p
}

func (g *GenericDownloader) GetId() string  { return g.Id }
func (g *GenericDownloader) GetUrl() string { return g.URL }

func (g *GenericDownloader) RestoreFromSnapshot(snap *internal.ProcessSnapshot) error {
	if snap == nil {
		return errors.New("cannot restore nil snapshot")
	}

	s := *snap

	g.Id = s.Id
	g.URL = s.Info.URL
	g.Metadata = s.Info
	g.progress = s.Progress
	g.output = s.Output
	g.Params = s.Params

	return nil
}

func (g *GenericDownloader) IsCompleted() bool { return g.Completed }

// newStderrScanner creates a scanner with a larger buffer to handle large stderr output
func newStderrScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 64*1024)
	return scanner
}

// retryWithStdErrCheck runs a function and retries on transient errors detected in stderr or the error itself
func retryWithStdErrCheck(fn func() error, id, url string, stderrLines <-chan string) error {
	for attempt := 0; attempt < maxReconnectAttempts; attempt++ {
		err := fn()

		if err == nil || !isTransientError(err) {
			return err // Permanent error or non-transient, don't retry
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

	return fn() // Last attempt exhausted, try one final time without retry
}
