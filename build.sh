#!/bin/sh

command -v go >/dev/null || { echo 'cannot find go command in $PATH'; exit 1; }

ver=$(git describe --tags)
now=$(date -u -Iseconds)

cmd=./cmd/sync
out=./bin/${cmd##*/}
[ -z $out ] && out=./bin/app

os=${1}
[ -z $os ] && os=$(uname -s | tr '[:upper:]' '[:lower:]')

arch=${2}
[ -z $arch ] && arch=$(uname -m | tr '[:upper:]' '[:lower:]')

cgoflag=${3}
[ "$cgoflag" = "" ] && cgoflag=0

echo "building ${cmd} to ${out} with os ${os}, architecture ${arch}, cgo-flag ${cgoflag}, build-time ${now}, and version ${ver}"

CGO_ENABLED=${cgoflag} GOOS=${os} GOARCH=${arch} go build -a -ldflags "-s -w -X main.version=${ver} -X main.buildtime=${now} -extldflags '-static'" -o bin/server ${cmd}