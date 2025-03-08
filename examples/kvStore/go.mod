module github.com/fanaujie/babuza/examples/kvStore

go 1.23.1

replace (
	github.com/fanaujie/babuza/ibabuza => ../../ibabuza
	github.com/fanaujie/babuza/pkg => ../../pkg
	github.com/fanaujie/babuza/raft => ../../raft
)

require (
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/fanaujie/babuza/ibabuza v0.0.0-00010101000000-000000000000
	github.com/fanaujie/babuza/pkg v0.0.0-00010101000000-000000000000
	github.com/fanaujie/babuza/raft v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v1.1.3
	github.com/stretchr/testify v1.8.4
	go.uber.org/zap v1.26.0

)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenk/backoff v2.2.1+incompatible // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rubyist/circuitbreaker v2.2.1+incompatible // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.12 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.12 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.12 // indirect
	go.etcd.io/etcd/server/v3 v3.5.12 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/exp v0.0.0-20240119083558-1b970713d09a // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
