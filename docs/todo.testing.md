
1.  ~~Need to create a command line test suite for Redis that uses
    `redis-cli` to test every command.  Then, using the same test suite,
    do the same test with a Go client program, using our client library
    and connecting to the gRPC do the same test.  Then do the same for
    the HTTP/WebSocket based interface.~~

    DONE: the CLI command matrix (tests/cli-matrix/, runner
    bin/test-cli-matrix.sh, `make test-cli-matrix`) runs every implemented
    Redis 7.2.7 command through redis-cli (RESP), ultima-cli (RESP),
    ultima-grpc-cli (gRPC) and ultima-ws-cli (WebSocket) against a live
    ultima-server, in both security modes, with optional real-redis
    validation (-R). See docs/cli-matrix-testing.md.
