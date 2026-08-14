module github.com/YouToco/vane

go 1.26.6

// SDK v3.9.9 WS 模式静默丢弃 MessageTypeCard，卡片回调永远到达不了应用层。
// 本地 patch：third_party/oapi-sdk-go/v3/ws/client.go 将 card 消息路由到 eventHandler.Do()。
// SDK 修复后移除此 replace 和 third_party/ 目录。
replace github.com/larksuite/oapi-sdk-go/v3 => ./third_party/oapi-sdk-go/v3

require (
	github.com/PuerkitoBio/goquery v1.12.0
	github.com/a2aproject/a2a-go/v2 v2.3.1
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.10.0
	github.com/larksuite/oapi-sdk-go/v3 v3.9.9
	github.com/mmcdole/gofeed v1.4.0
	github.com/pressly/goose/v3 v3.27.2
	github.com/robfig/cron v1.2.0
	github.com/spf13/viper v1.21.0
	go.temporal.io/api v1.63.0
	go.temporal.io/sdk v1.46.0
	golang.org/x/crypto v0.53.0
)

require (
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.22.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/mmcdole/goxpp/v2 v2.0.0 // indirect
	github.com/nexus-rpc/nexus-proto-annotations v0.1.0 // indirect
	github.com/nexus-rpc/sdk-go v0.6.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260427160629-7cedc36a6bc4 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260427160629-7cedc36a6bc4 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
