module github.com/pschlump/ultima

go 1.27.0

require (
	github.com/flowchartsman/swaggerui v0.0.0-20221017034628-909ed4f3701b
	github.com/go-chi/chi/v5 v5.3.2
	github.com/go-playground/validator/v10 v10.30.4
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/gorilla/websocket v1.5.3
	github.com/oapi-codegen/runtime v1.6.0
	github.com/prometheus/client_golang v1.24.1
	github.com/pschlump/gopher-lua v0.0.3
	github.com/pschlump/htotp v1.1.2
	github.com/pschlump/pluto v0.0.0
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.55.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.15 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/leodido/go-urn v1.5.0 // indirect
	github.com/makiuchi-d/gozxing v0.1.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/tetratelabs/wazero v1.12.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

replace github.com/pschlump/pluto => ../pluto

// replace github.com/pschlump/htotp => ../htotp

replace github.com/pschlump/gopher-lua => ../gopher-lua
