#!/usr/bin/env bash

set -e

if ! [[ "$0" =~ scripts/genProto.sh ]]; then
 echo "must be run from repository root"
 exit 255
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {install|update}"
  exit 1
fi

BUILD_PATH="/tmp/babuza_build_proto"

#function cleanup {
#  # Remove the whole fake GOPATH which can really confuse go mod.
#  rm -rf ${BUILD_PATH}
#}
#
#cleanup
#trap cleanup EXIT

export GOPATH=${BUILD_PATH}
export GOBIN=${BUILD_PATH}/bin
export PATH="${GOBIN}:${PATH}"

GEN_IBABUZA_RPOTO_PATH="./iBabuza/babuzapb"
GEN_PKG_PROTO_PATH="./pkg/babuzaWal/pb ./pkg/cluster/pb"

mkdir -p "${BUILD_PATH}/bin"

GOGO_PROTO_SHA=ba06b47c162d49f2af050fb4c75bcbc86a159d5c #v1.2.1
GOGOPROTO_ROOT="${GOPATH}/src/mod/github.com/gogo/protobuf"

ETCD_SHA=d42e8589e1305d893eeec9e7db746f6f4a76c250 #v3.5.1
ETCD_ROOT="${GOPATH}/src/go.etcd.io/etcd"

if [ "$1" == "install" ]; then
  go get -u google.golang.org/protobuf
  git clone https://github.com/gogo/protobuf.git "${GOGOPROTO_ROOT}"
  pushd "${GOGOPROTO_ROOT}"
    git reset --hard "${GOGO_PROTO_SHA}"
    make install
  popd

  git clone https://github.com/etcd-io/etcd.git ${ETCD_ROOT}
  pushd "${ETCD_ROOT}"
    git reset --hard "${ETCD_SHA}"
  popd
fi

pushd ${GEN_IBABUZA_RPOTO_PATH}
  protoc --gogofast_out=. --gogofast_opt=paths=source_relative -I=".:${GOGOPROTO_ROOT}:${GOPATH}/src" ./*.proto
  sed -i -E 's|"go.etcd.io/etcd/raft/raftpb"|"go.etcd.io/etcd/raft/v3/raftpb"|g' ./*pb.go
popd

for dir in ${GEN_PKG_PROTO_PATH}; do
  pushd "${dir}"
    protoc --gogofast_out=. --gogofast_opt=paths=source_relative -I=".:${GOGOPROTO_ROOT}:${GOPATH}/src:../../../iBabuza" ./*.proto
  popd
done