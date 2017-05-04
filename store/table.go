package store

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/tecbot/gorocksdb"
)

func OpenTable(name string, columns []string) (*gorocksdb.DB, []*TableColumn, error) {
	opt := gorocksdb.NewDefaultOptions()
	opt.SetCreateIfMissing(true)
	opt.SetCreateIfMissingColumnFamilies(true)
	opts := make([]*gorocksdb.Options, len(columns))
	for i := range opts {
		opts[i] = opt
	}
	db, cfs, err := gorocksdb.OpenDbColumnFamilies(opt, name, columns, opts)
	if err != nil {
		return nil, nil, err
	}
	dbColumns := make([]*TableColumn, len(columns))
	for i, c := range columns {
		dbColumns[i] = MakeDBColumn(db, cfs[i], c)
	}
	return db, dbColumns, nil
}

type TableColumn struct {
	db *gorocksdb.DB
	cf *gorocksdb.ColumnFamilyHandle
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions

	column string
}

func MakeDBColumn(db *gorocksdb.DB, cf *gorocksdb.ColumnFamilyHandle, column string) *TableColumn {
	return &TableColumn{
		db: db,
		cf: cf,
		ro: gorocksdb.NewDefaultReadOptions(),
		wo: gorocksdb.NewDefaultWriteOptions(),
	}
}

func (c *TableColumn) Put(key, value string) {
	c.db.PutCF(c.wo, c.cf, []byte(key), []byte(value))
}

func (c *TableColumn) PutBytes(key, value []byte) {
	c.db.PutCF(c.wo, c.cf, key, value)
}

func (c *TableColumn) Get(key string) (string, bool) {
	v, err := c.db.GetCF(c.ro, c.cf, []byte(key))
	if err != nil {
		glog.Warningf("Get from %s.%s failed: %s", c.db.Name(), c.column)
	}
	defer v.Free()
	if v.Data() == nil {
		return "", false
	}
	return string(v.Data()), true
}

func (c *TableColumn) GetBytes(key []byte) ([]byte, bool) {
	v, err := c.db.GetCF(c.ro, c.cf, key)
	if err != nil {
		glog.Warningf("Get from %s.%s failed: %s", c.db.Name(), c.column)
	}
	if v.Data() == nil {
		return nil, false
	}
	return v.Data(), true
}

func (c *TableColumn) String() string {
	return fmt.Sprintf("%s.%s", c.db.Name(), c.column)
}
