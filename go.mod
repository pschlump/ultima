module github.com/pschlump/ultima

go 1.27.0

require (
	github.com/go-chi/chi/v5 v5.3.2
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/gorilla/websocket v1.5.3
	github.com/pschlump/htotp v0.0.0-00010101000000-000000000000
	github.com/pschlump/pluto v0.0.0
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.55.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/makiuchi-d/gozxing v0.1.1 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

replace github.com/pschlump/pluto => ../pluto

replace github.com/pschlump/htotp => ../htotp
