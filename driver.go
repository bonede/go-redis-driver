package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisDriver struct {
}

type RedisConn struct {
	client   *redis.Client
	pipeline redis.Pipeliner
}

type RedisRows struct {
	cmd    *redis.Cmd
	pos    int
	values []string
}

type RedisTx struct {
	conn *RedisConn
	ctx  *context.Context
}

type RedisResult struct {
}

func (r *RedisResult) LastInsertId() (int64, error) {
	return 0, nil
}
func (r *RedisResult) RowsAffected() (int64, error) {
	return 0, nil
}

func init() {
	sql.Register("redis", &RedisDriver{})
}

func (d *RedisDriver) Open(dsn string) (driver.Conn, error) {
	opt, err := redis.ParseURL(fmt.Sprintf("redis://%s", dsn))
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opt)

	return &RedisConn{client: client, pipeline: nil}, nil
}

func (c *RedisConn) Prepare(query string) (driver.Stmt, error) {
	// TODO implement
	return nil, fmt.Errorf("prepare is not implemented")
}

func (c *RedisConn) Begin() (driver.Tx, error) {
	// TODO implement
	return nil, fmt.Errorf("not implemented")
}

func (c *RedisConn) Close() error {
	return c.client.Close()
}

func (c *RedisConn) Ping(ctx context.Context) error {
	_, err := c.client.Ping(ctx).Result()
	if err != nil {
		return driver.ErrBadConn
	}
	return nil
}
func parseQuery(query string) []interface{} {
	query = strings.Trim(query, " \t\n")
	var queryArgs = strings.Split(query, " ")
	q := make([]interface{}, 0, len(queryArgs))
	for _, i := range queryArgs {
		q = append(q, i)
	}
	return q
}
func (c *RedisConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q := parseQuery(query)
	var cmd *redis.Cmd
	if c.pipeline != nil {
		cmd = c.pipeline.Do(ctx, q...)
	} else {
		cmd = c.client.Do(ctx, q...)
	}
	err := cmd.Err()

	if err != nil {
		return nil, err
	}

	return &RedisRows{
		cmd: cmd,
		pos: 0,
	}, nil
}

func (c *RedisConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q := parseQuery(query)
	var cmd *redis.Cmd
	if c.pipeline != nil {
		cmd = c.pipeline.Do(ctx, q...)
	} else {
		cmd = c.client.Do(ctx, q...)
	}
	err := cmd.Err()

	if err != nil {
		return nil, err
	}
	return &RedisResult{}, nil
}

func formatArgs(args []interface{}) string {
	var builder strings.Builder
	for i := range args {
		builder.WriteString(fmt.Sprintf("%v", args[i]))
		if i < len(args)-1 {
			builder.WriteString(" ")
		}
	}
	return builder.String()
}

func (r *RedisRows) Columns() []string {
	return []string{formatArgs(r.cmd.Args())}
}

func (r *RedisRows) Close() error {
	return nil
}

func (r *RedisRows) Next(dest []driver.Value) error {
	if r.values == nil {
		values, err := r.cmd.StringSlice()
		if err != nil {
			value, err2 := r.cmd.Text()
			if err2 != nil {
				return err2
			}
			values = []string{value}
		}
		r.values = values
	}
	if r.pos > (len(r.values) - 1) {
		return io.EOF
	}
	dest[0] = driver.Value(r.values[r.pos])
	r.pos++
	return nil
}

func (r *RedisRows) ColumnTypeDatabaseTypeName(index int) string {
	return "TEXT"
}

func (r *RedisRows) RowsColumnTypeScanType(index int) reflect.Type {
	return reflect.TypeOf("")
}

func (r *RedisRows) RowsColumnTypeLength(index int) (length int64, ok bool) {
	return math.MaxInt64, true
}

func (c *RedisConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	pipeline := c.client.TxPipeline()
	c.pipeline = pipeline
	return &RedisTx{
		conn: c,
		ctx:  &ctx,
	}, nil
}

func (t *RedisTx) Commit() error {
	if t.conn.pipeline == nil {
		return fmt.Errorf("not in a transaction")
	}
	_, err := t.conn.pipeline.Exec(*t.ctx)
	t.conn.pipeline = nil
	if err != nil {
		return err
	}
	return nil
}

func (t *RedisTx) Rollback() error {
	if t.conn.pipeline == nil {
		return fmt.Errorf("not in a transaction")
	}
	t.conn.pipeline.Discard()
	t.conn.pipeline = nil
	return nil
}
