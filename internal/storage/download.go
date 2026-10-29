package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

const downloadConcurrency = 32

func WorkspacePrefix(userID, workspaceName string) string {
	return "users/" + userID + "/" + workspaceName + "/"
}

func DownloadWorkspace(ctx context.Context, client *s3.Client, bucket, userID, workspaceName, targetDir string) (int, error) {
	if userID == "" || workspaceName == "" || strings.Contains(userID, "/") || strings.Contains(workspaceName, "/") {
		return 0, fmt.Errorf("invalid user id or workspace name")
	}
	root, err := filepath.Abs(targetDir)
	if err != nil {
		return 0, fmt.Errorf("resolving target dir %q: %w", targetDir, err)
	}
	prefix := WorkspacePrefix(userID, workspaceName)

	var downloaded atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(downloadConcurrency)

	listErr := func() error {
		paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(gctx)
			if err != nil {
				return err
			}
			for _, obj := range page.Contents {
				key := aws.ToString(obj.Key)
				if strings.HasSuffix(key, "/") {
					continue
				}
				dest, err := safeJoin(root, strings.TrimPrefix(key, prefix))
				if err != nil {
					return fmt.Errorf("object %q: %w", key, err)
				}
				g.Go(func() error {
					if err := downloadObject(gctx, client, bucket, key, dest); err != nil {
						return err
					}
					downloaded.Add(1)
					return nil
				})
			}
		}
		return nil
	}()

	if err := g.Wait(); err != nil {
		return int(downloaded.Load()), fmt.Errorf("downloading workspace %s: %w", prefix, err)
	}
	if listErr != nil {
		return int(downloaded.Load()), fmt.Errorf("listing workspace %s: %w", prefix, listErr)
	}
	n := int(downloaded.Load())
	if n == 0 {
		return 0, fmt.Errorf("no files found under %s (was the workspace uploaded?)", prefix)
	}
	return n, nil
}

func safeJoin(root, rel string) (string, error) {
	dest := filepath.Join(root, filepath.FromSlash(rel))
	r, err := filepath.Rel(root, dest)
	if err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes target directory")
	}
	return dest, nil
}

func downloadObject(ctx context.Context, client *s3.Client, bucket, key, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", key, err)
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("downloading %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	f, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("creating %s: %w", dest, err)
	}
	if _, err := io.Copy(f, out.Body); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing %s: %w", dest, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dest, err)
	}
	return nil
}
