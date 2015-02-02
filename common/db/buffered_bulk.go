package db

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const MaxBulkSize = 16790000
const BulkDocLimit = 1000

// BufferedBulkInserter implements a bufio.Writer-like design for queuing up
// documents and inserting them in bulk when the given doc limit (or max
// message size) is reached. Must be flushed at the end to ensure that all
// documents are written.
type BufferedBulkInserter struct {
	bulk            BulkInserter
	collection      *mgo.Collection
	continueOnError bool
	docLimit        int
	byteCount       int
	docCount        int
}

// NewBufferedBulkInserter returns an initialized BufferedBulkInserter
// for writing.
func NewBufferedBulkInserter(collection *mgo.Collection, docLimit int,
	continueOnError bool, bulk BulkInserter) *BufferedBulkInserter {
	bb := &BufferedBulkInserter{
		bulk:            bulk,
		collection:      collection,
		continueOnError: continueOnError,
		docLimit:        docLimit,
	}
	bulk.Reset()
	return bb
}

// throw away the old bulk and init a new one
func (bb *BufferedBulkInserter) resetBulk(bulk BulkInserter) {
	bb.bulk = bulk
	if bb.continueOnError {
		bb.bulk.Unordered()
	}
	bb.byteCount = 0
	bb.docCount = 0
}

// Insert adds a document to the buffer for bulk insertion. If the buffer is
// full, the bulk insert is made, returning any error that occurs.
func (bb *BufferedBulkInserter) Insert(doc interface{}) error {
	rawBytes, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("bson encoding error: %v", err)
	}
	// flush if we are full
	if bb.docCount >= bb.docLimit || bb.byteCount+len(rawBytes) > MaxBulkSize || bb.docCount >= BulkDocLimit {
		err = bb.Flush()
	}
	// buffer the document
	bb.docCount++
	bb.byteCount += len(rawBytes)
	bb.bulk.Insert(bson.Raw{Data: rawBytes})
	return err
}

// Flush writes all buffered documents in one bulk insert then resets the buffer.
func (bb *BufferedBulkInserter) Flush() error {
	if bb.docCount == 0 {
		return nil
	}
	defer bb.bulk.Reset()
	if err := bb.bulk.Run(); err != nil {
		return err
	}
	return nil
}

type BulkInserter interface {
	Unordered()
	Insert(doc interface{})
	Run() error
	Reset()
}

type LegacyBulkInserter struct {
	b         *mgo.Bulk
	c         *mgo.Collection
	unordered bool
}

func (lbi *LegacyBulkInserter) Reset() {
	lbi.b = lbi.c.Bulk()
	if lbi.unordered {
		lbi.b.Unordered()
	}
}

func (lbi *LegacyBulkInserter) Unordered() {
	lbi.unordered = true
	lbi.b.Unordered()
}

func (lbi *LegacyBulkInserter) Insert(d interface{}) {
	lbi.b.Insert(d)
}

func (lbi *LegacyBulkInserter) Run() error {
	_, err := lbi.b.Run()
	return err
}

func NewLegacyBulk(c *mgo.Collection, ordered bool) *LegacyBulkInserter {
	lbi := &LegacyBulkInserter{
		c.Bulk(),
		c,
		!ordered,
	}

	return lbi
}

type WriteCommandBulk struct {
	c            *mgo.Collection
	docs         []interface{}
	unordered    bool
	writeConcern interface{}
}

type ErrWriteCommandBulk struct {
	Errors    []map[string]interface{} `bson:"writeErrors"`
	RootError error
}

func (ewcb *ErrWriteCommandBulk) Error() string {
	var result string
	if ewcb.RootError != nil {
		result += ewcb.RootError.Error()
	}
	if len(ewcb.Errors) > 0 {
		for _, e := range ewcb.Errors {
			result += fmt.Sprintf("\n%#v", e["errmsg"])
		}
	}
	return result
}

func (wcb *WriteCommandBulk) Reset() {
	wcb.docs = []interface{}{}
}

func (wcb *WriteCommandBulk) Unordered() {
	wcb.unordered = true
}

func (wcb *WriteCommandBulk) Insert(d interface{}) {
	wcb.docs = append(wcb.docs, d)
}

func (wcb *WriteCommandBulk) Run() error {
	fmt.Println("len", len(wcb.docs))
	restoreDoc := bson.D{
		bson.DocElem{"insert", wcb.c.Name},
		bson.DocElem{"ordered", !wcb.unordered},
		bson.DocElem{"documents", wcb.docs},
		bson.DocElem{"writeConcern", wcb.writeConcern},
	}
	result := ErrWriteCommandBulk{}
	err := wcb.c.Database.Run(restoreDoc, &result)
	if err != nil {
		return err
	}
	if err != nil || len(result.Errors) > 0 {
		result.RootError = err
		return &result
	}
	return nil
}

func NewWriteCommandBulk(c *mgo.Collection, ordered bool, writeConcern interface{}) *WriteCommandBulk {
	wcb := &WriteCommandBulk{
		c,
		[]interface{}{},
		!ordered,
		writeConcern,
	}

	return wcb
}
