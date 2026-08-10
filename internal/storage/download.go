package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

func DownloadWorkspace(ctx context.Context, s3Client *s3.Client, bucket, workspacePrefix, targetDir string) error {
	prefix := workspacePrefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(32)

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("listing workspace objects under %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasSuffix(key, "/") {
				continue // folder marker, nothing to download
			}
			relPath := strings.TrimPrefix(key, prefix)
			destPath := filepath.Join(targetDir, filepath.FromSlash(relPath))

			g.Go(func() error {
				return downloadOneObject(ctx, s3Client, bucket, key, destPath)
			})
		}
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("downloading workspace %s: %w", prefix, err)
	}
	return nil
}

func downloadOneObject(ctx context.Context, s3Client *s3.Client, bucket, key, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", key, err)
	}

	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("downloading %s: %w", key, err)
	}
	defer out.Body.Close()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", destPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, out.Body); err != nil {
		return fmt.Errorf("writing %s: %w", destPath, err)
	}
	return nil
}
