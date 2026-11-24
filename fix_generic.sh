
#!/usr/bin/env bash
set -euo pipefail

FILE="server/internal/downloaders/generic.go"

if [[ ! -f "$FILE" ]]; then
    echo "Error: $FILE not found. Run this from the project root."
    exit 1
fi

# Backup original
cp "$FILE" "$FILE.bak"

python3 - "$FILE" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
source = path.read_text()

start = source.index("func (g *GenericDownloader) Start() error {")
end = source.index("func (g *GenericDownloader) Stop() error {", start)

replacement = r'''func (g *GenericDownloader) Start() error {
    g.SetPending(true)
    g.Params = argsSanitizer(g.Params)

    out := internal.DownloadOutput{
        Path: config.Instance().Paths.DownloadPath,
        Filename: "%(title).150B [%(id)s].%(ext)s",
    }

    if g.output.Path != "" {
        out.Path = g.output.Path
    }

    if g.output.Filename != "" {
        out.Filename = g.output.Filename
    }

    buildFilename(&g.output)

    templateReplacer := strings.NewReplacer("\n", "", "\t", "", " ", "")

    baseParams := []string{
        strings.Split(g.URL, "?list")[0],
        "--newline",
        "--no-colors",
        "--no-playlist",
        "--progress-template",
        templateReplacer.Replace(downloadTemplate),
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

        if !transient && stderrOutput != "" {
            transient = isTransientError(errors.New(stderrOutput))
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

'''

source = source[:start] + replacement + source[end:]

# Fix imports
if '"bytes"' not in source:
    source = source.replace('import (\n', 'import (\n\t"bytes"\n', 1)

if '"os"' not in source:
    source = source.replace('"os/exec"', '"os"\n\t"os/exec"', 1)

source = source.replace('\t"sync/atomic"\n', '')

path.write_text(source)
PY

# Format Go source
gofmt -w "$FILE"

echo "Successfully patched $FILE"
echo "Original backup: $FILE.bak"

