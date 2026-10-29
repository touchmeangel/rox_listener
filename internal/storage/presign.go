package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func PresignPut(ctx context.Context, c *s3.Client, bucket, key string, ttl time.Duration) (string, error) {
	req, err := s3.NewPresignClient(c).PresignPutObject(ctx,
		&s3.PutObjectInput{Bucket: &bucket, Key: &key}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func GetObject(ctx context.Context, c *s3.Client, bucket, key string) ([]byte, error) {
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	var missing *types.NoSuchKey // github.com/aws/aws-sdk-go-v2/service/s3/types
	if errors.As(err, &missing) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// def _upload_result(path: str) -> None:
//     url = os.environ.get("ROX_RESULT_PUT_URL")
//     if not url:
//         return  # local runs: keep writing to disk only
//     req = urllib.request.Request(url, data=Path(path).read_bytes(), method="PUT")
//     urllib.request.urlopen(req, timeout=60).read()
