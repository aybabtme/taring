# Taring

Dumb tool to fetch the content of an S3 bucket, tar and gzip it.

Likely you can do the same thing using [aws-cli][1].

## Installation

```
go get github.com/aybabtme/taring
```

## Usage

```
taring -aws-access=$AWS_ACCESS_KEY      \
       -aws-secret=$AWS_SECRET_KEY      \
       -s3-path="s3://mybucket/a/path/" \
       -tar-path="mybucket.tar.gz" 
```

The tool will then recursively fetch all the files in `s3://mybucket/a/path/`, put them
in a tar buffer, gzip that buffer and save to `mybucket.tar.gz`.

If you name the `tar-path` something without `tar.gz` at the end, it will still tar 
and gzip the content.

[1]: https://aws.amazon.com/cli/
