package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/bonede/go-redis-driver"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	db, err := sql.Open("redis", "localhost:6379")
	require.Nil(t, err)
	rows, err := db.Query("set foo bar")
	require.Nil(t, err)
	rows, err = db.Query("get foo")
	require.Nil(t, err)
	rows, err = db.Query("keys *")
	require.Nil(t, err)
	value := ""
	cols, err := rows.Columns()
	require.Nil(t, err)
	t.Logf("%s", value)
	for rows.Next() {
		rows.Scan(&value)
		t.Logf("%s", value)
	}

	t.Logf("%s", cols)
	tx, err := db.Begin()
	require.Nil(t, err)
	_, err = tx.Exec("set foo bar2")
	require.Nil(t, err)
	tx.Commit()
	rows, err = db.Query("get foo")
	require.Nil(t, err)
	for rows.Next() {
		rows.Scan(&value)
	}
	require.Equal(t, "bar2", value)
}
