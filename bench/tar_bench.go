package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"github.com/aybabtme/benchkit"
	"github.com/aybabtme/color/brush"
	"github.com/dustin/go-humanize"
	"github.com/dustin/randbo"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	filePerms = os.FileMode(os.ModePerm & 0644)
	elog      = log.New(os.Stderr, brush.Red("[fatal] ").String(), 0)
)

func init() {
	log.SetFlags(0)
	log.SetPrefix(brush.Blue("[info] ").String())
}

func fatalFlag(format string, args ...interface{}) {
	elog.Printf(brush.LightGray(format).String(), args...)
	flag.PrintDefaults()
	os.Exit(2)
}

func fatalf(format string, args ...interface{}) {
	elog.Printf(brush.LightGray(format).String(), args...)
	os.Exit(2)
}

func infof(format string, args ...interface{}) { log.Printf(brush.LightGray(format).String(), args...) }

func effectMem(mem *runtime.MemStats) string {
	effectMem := mem.Sys - mem.HeapReleased
	return humanize.Bytes(effectMem)
}

func main() {
	var (
		n       int
		sizeStr string
		discard bool
	)
	flag.IntVar(&n, "n", 0, "number of files to tar")
	flag.StringVar(&sizeStr, "size", "0", "size of files to tar")
	flag.BoolVar(&discard, "discard", false, "tar the file to /dev/null instead of a memory buffer")
	flag.Parse()

	size, err := humanize.ParseBytes(sizeStr)
	if err != nil {
		fatalFlag("flag -size must be a valid byte size: %v", err)
	}

	switch {
	case n <= 0:
		fatalFlag("flag -n must specify at least 1 file to tar")
	case size <= 0:
		fatalFlag("flag -size must specify at least 1 byte per file")
	}

	var dst io.Writer
	if discard {
		dst = ioutil.Discard
	} else {
		dst = bytes.NewBuffer(nil)
	}

	infof("starting benchmark")
	infof("\t- with %d generated tar files", n)
	infof("\t- each file of size %s", humanize.Bytes(size))
	if discard {
		infof("\t- writing to /dev/null")
	} else {
		infof("\t- writing to memory buffer (will grow to %s)", humanize.Bytes(size*uint64(n)))
	}
	start := time.Now()
	results := doBenchmark(n, int(size), dst)
	infof("done benchmark: %v", time.Since(start))

	buf := bytes.NewBuffer(nil)
	infof("plotting...")
	if err := plotSVG(buf, n, sizeStr, results); err != nil {
		fatalf("couldn't plot results to SVG: %v", err)
	}

	filename := fmt.Sprintf("tar_bench_n%d_size%s.svg", n, sizeStr)
	infof("saving plot to %q...", filename)
	if err := ioutil.WriteFile(filename, buf.Bytes(), 0644); err != nil {
		fatalf("couldn't save plot to %q: %v", filename, err)
	}
}

func doBenchmark(n, size int, dst io.Writer) *benchkit.MemResult {
	memkit, results := benchkit.Memory(n)

	if err := benchmarkTar(n, size, dst, memkit); err != nil {
		fatalf("failed to benchmark: %v", err)
	}

	infof("setup\t effectMem=%s", effectMem(results.Setup))
	infof("starting\t effectMem=%s", effectMem(results.Start))

	for id := 0; id < results.N; id++ {
		infof("\t%d\tbefore-effectMem=%s\tafter-effectMem=%s",
			id,
			effectMem(results.BeforeEach[id]),
			effectMem(results.AfterEach[id]),
		)
	}
	infof("teardown\t effectMem=%s", effectMem(results.Teardown))

	return results
}

func benchmarkTar(n, size int, dst io.Writer, bench benchkit.BenchKit) error {
	bench.Setup()
	files := GenTarFiles(n, size)
	bench.Starting()
	err := tarify(dst, files, bench.Each())
	bench.Teardown()
	return err
}

func tarify(w io.Writer, objects []TarFile, each benchkit.BenchEach) error {
	tarw := tar.NewWriter(w)
	for i, object := range objects {
		each.Before(i)
		if err := tarw.WriteHeader(object.TarHeader()); err != nil {
			return fmt.Errorf("writing header of %q, %v", object.Name, err)
		}
		if _, err := tarw.Write(object.Data.Bytes()); err != nil {
			return fmt.Errorf("writing content of %q, %v", object.Name, err)
		}
		each.After(i)
	}
	if err := tarw.Close(); err != nil {
		return fmt.Errorf("closing tar buffer, %v", err)
	}
	return nil
}

var rand = randbo.NewFast()

func GenTarFiles(n, size int) []TarFile {
	files := make([]TarFile, n)
	for i := range files {
		files[i] = GenTarFile(i, size)
	}
	return files
}

func GenTarFile(id, size int) TarFile {
	data := make([]byte, size)
	_, _ = rand.Read(data)
	return TarFile{
		Name:    strconv.Itoa(id),
		LastMod: time.Now(),
		Data:    *bytes.NewBuffer(data),
	}
}

type TarFile struct {
	Name    string
	LastMod time.Time
	Data    bytes.Buffer
}

func (s *TarFile) TarHeader() *tar.Header {
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
