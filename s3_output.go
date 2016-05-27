package s3

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	. "github.com/mozilla-services/heka/pipeline"
)

const INTERVAL_PERIOD time.Duration = 24 * time.Hour
const HOUR_TO_TICK int = 00
const MINUTE_TO_TICK int = 00
const SECOND_TO_TICK int = 00

// S3OutputConfig holds the config for an S3Output plugin.
type S3OutputConfig struct {
	SecretKey        string `toml:"secret_key"`
	AccessKey        string `toml:"access_key"`
	Region           string `toml:"region"`
	Bucket           string `toml:"bucket"`
	Prefix           string `toml:"prefix"`
	TickerInterval   uint   `toml:"ticker_interval"`
	Compression      bool   `toml:"compression"`
	BufferPath       string `toml:"buffer_path"`
	BufferChunkLimit int    `toml:"buffer_chunk_limit"`
}

// S3Output is a Heka S3 output plugin.
type S3Output struct {
	config         *S3OutputConfig
	client         *s3.S3
	bucket         *s3.Bucket
	bufferFilePath string
}

func midnightTickerUpdate() *time.Ticker {
	nextTick := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), time.Now().UTC().Day(), HOUR_TO_TICK, MINUTE_TO_TICK, SECOND_TO_TICK, 0, time.UTC)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(INTERVAL_PERIOD)
	}
	diff := nextTick.Sub(time.Now())
	return time.NewTicker(diff)
}

// ConfigStruct provides the default config for a Heka plugin.
func (so *S3Output) ConfigStruct() interface{} {
	return &S3OutputConfig{Compression: true, BufferChunkLimit: 1000000}
}

// Init is the standard Heka plugin initializer.
func (so *S3Output) Init(config interface{}) (err error) {
	so.config = config.(*S3OutputConfig)
	auth, err := aws.GetAuth(so.config.AccessKey, so.config.SecretKey, "", time.Now())
	if err != nil {
		return
	}
	region, ok := aws.Regions[so.config.Region]
	if !ok {
		err = errors.New("Region of that name not found.")
		return
	}
	so.client = s3.New(auth, region)
	so.bucket = so.client.Bucket(so.config.Bucket)

	prefixList := strings.Split(so.config.Prefix, "/")
	bufferFileName := so.config.Bucket + strings.Join(prefixList, "_")
	so.bufferFilePath = so.config.BufferPath + "/" + bufferFileName
	return
}

// Run is the standard Heka plugin entry point.
func (so *S3Output) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()
	tickerChan := or.Ticker()
	buffer := bytes.NewBuffer(nil)
	midnightTicker := midnightTickerUpdate()

	var (
		pack     *PipelinePack
		outBytes []byte
		ok       = true
	)

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			var err error

			if outBytes, err = or.Encode(pack); err != nil {
				or.LogError(fmt.Errorf("Error encoding message: %s", err))
			} else if outBytes != nil {
				err = so.WriteToBuffer(buffer, outBytes, or)
			}
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to write to buffer: %s", err))
				continue
			}
			pack.Recycle()
		case <-tickerChan:
			or.LogMessage(fmt.Sprintf("Ticker fired, uploading payload."))
			err := so.Upload(buffer, or, false)
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to upload payload: %s", err))
				continue
			}
			or.LogMessage(fmt.Sprintf("Payload uploaded successfully."))
			buffer.Reset()
		case <-midnightTicker.C:
			midnightTicker = midnightTickerUpdate()
			or.LogMessage(fmt.Sprintf("Midnight ticker fired, uploading payload."))
			err := so.Upload(buffer, or, true)
			if err != nil {
				or.LogMessage(fmt.Sprintf("Warning, unable to upload payload: %s", err))
				continue
			}
			or.LogMessage(fmt.Sprintf("Payload uploaded successfully."))
			buffer.Reset()
		}
	}

	or.LogMessage(fmt.Sprintf("Shutting down S3 output runner."))
	return
}

// WriteToBuffer writes bytes to the buffer and writes the buffer to disk if it exceeds the limit.
func (so *S3Output) WriteToBuffer(buffer *bytes.Buffer, outBytes []byte, or OutputRunner) (err error) {
	_, err = buffer.Write(outBytes)
	if err != nil {
		return
	}
	if buffer.Len() > so.config.BufferChunkLimit {
		err = so.SaveToDisk(buffer, or)
	}
	return
}

// SaveToDisk appends the contents of the buffer to disk and resets the buffer.
func (so *S3Output) SaveToDisk(buffer *bytes.Buffer, or OutputRunner) error {
	_, err := os.Stat(so.config.BufferPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(so.config.BufferPath, 0666)
		if err != nil {
			return err
		}
	}

	err = os.Chdir(so.config.BufferPath)
	if err != nil {
		return err
	}

	_, err = os.Stat(so.bufferFilePath)
	if os.IsNotExist(err) {
		or.LogMessage("Creating buffer file: " + so.bufferFilePath)
		w, err := os.Create(so.bufferFilePath)
		w.Close()
		if err != nil {
			return err
		}
	}

	f, err := os.OpenFile(so.bufferFilePath, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(buffer.Bytes())
	if err != nil {
		return err
	}

	buffer.Reset()

	return nil
}

// ReadFromDisk reads and optionally compresses the file from disk and returns a buffer of its contents.
func (so *S3Output) ReadFromDisk(or OutputRunner) (buffer *bytes.Buffer, err error) {
	fi, err := os.Open(so.bufferFilePath)
	if err != nil {
		return
	}
	defer fi.Close()

	if so.config.Compression {
		or.LogMessage("Reading and compressing buffer file.")
		w := gzip.NewWriter(buffer)
		_, err = io.Copy(w, fi)
		w.Close()
	} else {
		or.LogMessage("Reading buffer file.")
		_, err = io.Copy(buffer, fi)
	}

	return buffer, err
}

// Upload flushes any remaining buffer contents to disk and then uploads the file contents to S3.
func (so *S3Output) Upload(buffer *bytes.Buffer, or OutputRunner, isMidnight bool) (err error) {
	_, err = os.Stat(so.bufferFilePath)
	if buffer.Len() == 0 && os.IsNotExist(err) {
		err = errors.New("Nothing to upload.")
		return
	}

	err = so.SaveToDisk(buffer, or)
	if err != nil {
		or.LogMessage("Cannot save to disk")
		return
	}

	buffer, err = so.ReadFromDisk(or)
	if err != nil {
		or.LogMessage("Cannot read from disk")
		return
	}

	var (
		currentTime = time.Now().Local().Format("2006-01-02_150405")
		currentDate = ""
		ext         = ""
		contentType = "text/plain"
	)

	if isMidnight {
		currentDate = time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	} else {
		currentDate = time.Now().UTC().Format("2006-01-02")
	}

	if so.config.Compression {
		ext = ".gz"
		contentType = "multipart/x-gzip"
	}

	path := so.config.Prefix + "/" + currentDate + "/" + currentTime + ext
	err = so.bucket.Put(path, buffer.Bytes(), contentType, s3.Private, s3.Options{})

	or.LogMessage("Upload finished, removing buffer file on disk.")
	if err == nil {
		err = os.Remove(so.bufferFilePath)
	} else {
		or.LogMessage("Error putting to S3 bucket")
	}

	return
}

func init() {
	RegisterPlugin("S3Output", func() interface{} {
		return new(S3Output)
	})
}
