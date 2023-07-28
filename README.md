# About
This a simple Go `database\sql` driver for Redis.
# How to use
```console
$ go get github.com/bonede/go-redis-driver
```
```go
// main.go
package main

import (
	"database/sql"
	_ "github.com/bonede/go-redis-driver"
)
func main(){
    db, err := sql.Open("redis", "<user>:<pass>@localhost:6379/<db>")
    defer db.Close()
}
```