package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/aybabtme/color/brush"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"github.com/dustin/go-humanize"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	filePerms = os.FileMode(os.ModePerm & 0644)
	elog      = log.New(os.Stderr, "", log.Flags())
)

func fatalFlag(format string, args ...interface{}) {
	elog.Printf(brush.Red("[flags] ").String()+brush.LightGray(format).String(), args...)
	flag.PrintDefaults()
	os.Exit(2)
}

func errorf(format string, args ...interface{}) {
	elog.Printf(brush.Yellow("[error] ").String()+brush.LightGray(format).String(), args...)
}

func fatalf(format string, args ...interface{}) {
	elog.Printf(brush.Red("[fatal] ").String()+brush.LightGray(format).String(), args...)
	os.Exit(2)
}

func infof(format string, args ...interface{}) {
	colorFmt := brush.Blue("[info] ").String() + brush.LightGray(format).String()
	log.Printf(colorFmt, args...)
}

func must(u *url.URL, err error) *url.URL {
	if err != nil {
		fatalf("not a URL, %v", err)
	}
	return u
}

func main() {
	awsSecret := flag.String("aws-secret", "", "an AWS secret key")
	awsAccess := flag.String("aws-access", "", "an AWS access key")
	awsRegion := flag.String("aws-region", aws.USEast.Name, "an AWS region string")
	bucketSrc := flag.String("s3-path", "", "a URL of the form `s3://bucketname/path/to/files`")
	tarDst := flag.String("tar-path", "bucket.tar.gz", "a path to save the TAR of what's at `s3-path`")
	flag.Parse()
	region, regionOk := aws.Regions[*awsRegion]

	switch {
	case *awsAccess == "":
		fatalFlag("need an AWS access key.\n")
	case *awsSecret == "":
		fatalFlag("need an AWS secret key.\n")
	case !regionOk:
		fatalFlag("need a valid AWS region, %q is not a valid one.\n", *awsRegion)
	case *bucketSrc == "":
		fatalFlag("need bucket path to read from.\n")
	case *tarDst == "":
		fatalFlag("need filepath to write TAR archive to.\n")
	}

	auth := aws.Auth{
		AccessKey: *awsAccess,
		SecretKey: *awsSecret,
	}

	bktURL := must(url.Parse(*bucketSrc))
	bktRoot := must(bktURL.Parse("/"))

	bktName := bktRoot.Host
	bktPath := bktURL.Path[1:]

	bkt := s3.New(auth, region).Bucket(bktName)

	infof("Listing bucket %q.", bktName)

	contents, err := fetchPath(bkt, "", bktPath, bktPath)
	if err != nil {
		fatalf("couldn't fetch %q: %v.", bktPath, err)
	}

	tarArch := bytes.NewBuffer(nil)
	infof("writing %d objects into tar buffer", len(contents))
	if err := tarify(tarArch, contents); err != nil {
		fatalf("tarifying content, %v.", err)
	}

	infof("gzipping...")
	gzipArch := bytes.NewBuffer(nil)
	gw := gzip.NewWriter(gzipArch)
	if _, err := io.Copy(gw, tarArch); err != nil {
		fatalf("writing tared objects to gzip buffer, %v", err)
	}
	if err := gw.Close(); err != nil {
		fatalf("closing gzip buffer, %v", err)
	}

	if err := ioutil.WriteFile(*tarDst, gzipArch.Bytes(), filePerms); err != nil {
		fatalf("writing tar/gzip buffer to %q, %v", *tarDst, err)
	}
	infof("saved tar/gzip of %q to %q", bktURL.String(), *tarDst)
}

func fetchPath(bkt *s3.Bucket, prfx string, root, bktPath string) ([]S3Content, error) {
	infof("%spath %q", prfx, bktPath)
	list, err := bkt.List(bktPath, "/", "", 10000)
	if err != nil {
		return nil, fmt.Errorf("couldn't list bucket at path %q: %v", bktPath, err)
	}

	infof("%s%d keys", prfx, len(list.Contents))
	var sumKey uint64
	for _, key := range list.Contents {
		infof("%s\t(%s) key %q", prfx, humanize.Bytes(uint64(key.Size)), key.Key)
		sumKey += uint64(key.Size)
	}
	infof("%s\ttotal %s", prfx, humanize.Bytes(sumKey))

	contents, err := fetchAll(bkt, prfx, root, list.Contents)
	if err != nil {
		return nil, fmt.Errorf("fetching content of keys at %q, %v", bktPath, err)
	}

	infof("%s%d folders", prfx, len(list.CommonPrefixes))
	for _, folder := range list.CommonPrefixes {
		infof("\t%q", folder)
	}

	for _, folder := range list.CommonPrefixes {
		newContent, err := fetchPath(bkt, prfx+"\t", root, folder)
		if err != nil {
			return nil, err
		}
		contents = append(contents, newContent...)
	}

	return contents, nil
}

func fetchAll(bkt *s3.Bucket, prfx, base string, keys []s3.Key) ([]S3Content, error) {
	contentC := make(chan S3Content, len(keys))

	doFetch := func(w *sync.WaitGroup, k s3.Key, errc chan<- error) {
		defer w.Done()

		lastMod, err := time.Parse(time.RFC3339Nano, k.LastModified)
		if err != nil {

			errc <- fmt.Errorf("failed to parse time of %q: %v", k.LastModified, err)
			return
		}
		relPath, err := filepath.Rel(base, k.Key)
		if err != nil {
			errc <- fmt.Errorf("failed to find relative path for %q: %v", k.Key, err)
			return
		}

		start := time.Now()
		data, err := bkt.Get(k.Key)
		if err != nil {
			errc <- fmt.Errorf("failed fetch of %q: %v", k.Key, err)
			return
		}

		infof("%s\t(%v) %q from %q ", prfx, time.Since(start), relPath, k.Key)
		contentC <- S3Content{
			Name:    relPath,
			Data:    *bytes.NewBuffer(data),
			LastMod: lastMod,
		}
	}

	errc := make(chan error, len(keys))
	infof("%sfetching ... ", prfx)
	wg := sync.WaitGroup{}
	for _, key := range keys {
		wg.Add(1)
		go doFetch(&wg, key, errc)
	}
	wg.Wait()
	close(contentC)
	close(errc)

	contents := make([]S3Content, 0, len(contentC))
	for content := range contentC {
		contents = append(contents, content)
	}

	var errs []string
	for err := range errc {
		errs = append(errs, err.Error())
	}
	if len(errs) != 0 {
		return nil, fmt.Errorf("%d errors: %s", len(errs), strings.Join(errs, ","))
	}

	return contents, nil

}

func tarify(w io.Writer, objects []S3Content) error {
	tarw := tar.NewWriter(w)
	infof("taring...")
	for _, object := range objects {
		if err := tarw.WriteHeader(object.TarHeader()); err != nil {
			return fmt.Errorf("writing header of %q, %v", object.Name, err)
		}
		if _, err := tarw.Write(object.Data.Bytes()); err != nil {
			return fmt.Errorf("writing content of %q, %v", object.Name, err)
		}
	}
	if err := tarw.Close(); err != nil {
		return fmt.Errorf("closing tar buffer, %v", err)
	}
	return nil
}

type S3Content struct {
	Name    string
	LastMod time.Time
	Data    bytes.Buffer
}

func (s *S3Content) TarHeader() *tar.Header {

	return &tar.Header{
		Name:       s.Name,
		Size:       int64(s.Data.Len()),
		Mode:       int64(filePerms),
		AccessTime: time.Now(),
		ChangeTime: s.LastMod,
		ModTime:    s.LastMod,
		Typeflag:   tar.TypeReg,
		Uid:        os.Getuid(),
		Gid:        os.Getgid(),
	}
}
