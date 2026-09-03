
The HTTP login facility will sit on top of the existing Redis
like security.  The login facility will need to add a TOTP
2-Factor key as an option for login authentication.  
This will sit on the current user and
and ALC system inside Redis-Clone (Ultima) now.  This will need to cover users for
both WebSockets and gRPC.   For WebSockets as an upgrade to HTTPs
this will use a standard JWT authenticated token system with refresh
tokens and authentication tokens.   Because of the likehood of the
WebSoket connection being an unreliable connection a connection
will need to have a connection recovery system so that a client
that looses its connection can recover the connetion without loss
of messages.


