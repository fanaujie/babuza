#!/usr/bin/env bash

set -e

if ! [[ "$0" =~ tools/genProto.sh ]]; then
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

GEN_IBABUZA_RPOTO_PATH="${PWD}/ibabuza/babuzapb"
GEN_PKG_PROTO_PATH="${PWD}/pkg/wal/babuzawal/pb ${PWD}/pkg/cluster/pb"
GEN_GRPC_PROTO_PATH="${PWD}/pkg/transport/protocol/grpc/pb"

mkdir -p "${BUILD_PATH}/bin"

GOGO_PROTO_SHA=ba06b47c162d49f2af050fb4c75bcbc86a159d5c #v1.2.1
GOGOPROTO_ROOT="${GOPATH}/src/mod/github.com/gogo/protobuf"

ETCD_SHA=a17edfd59754d1aed29c2db33520ab9d401326a5 #v3.5.21
ETCD_ROOT="${GOPATH}/src/go.etcd.io/etcd"

if [ "$1" == "install" ]; then
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
    protoc --gogofast_out=. --gogofast_opt=paths=source_relative -I=".:${GOGOPROTO_ROOT}:${GOPATH}/src:${GEN_IBABUZA_RPOTO_PATH}" ./*.proto
  popd
done

for dir in ${GEN_GRPC_PROTO_PATH}; do
  pushd "${dir}"
    protoc --gogofast_out=plugins=grpc:. --gogofast_opt=paths=source_relative -I=".:${GOGOPROTO_ROOT}:${GOPATH}/src:${GEN_IBABUZA_RPOTO_PATH}" ./*.proto
  popd
done