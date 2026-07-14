package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

func main() {
	ctx := context.Background()

	base := "/tmp/iceberg path escaping repro"
	warehouse := filepath.Join(base, "warehouse")
	tableRoot := filepath.Join(warehouse, "default", "triples")

	if err := os.RemoveAll(base); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(warehouse, 0755); err != nil {
		panic(err)
	}

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	icebergSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	cat, err := hadoop.NewCatalog("local-catalog", warehouse, nil)
	if err != nil {
		panic(err)
	}

	ns := catalog.ToIdentifier("default")
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil {
		panic(err)
	}

	ident := catalog.ToIdentifier("default", "triples")
	tbl, err := cat.CreateTable(ctx, ident, icebergSchema)
	if err != nil {
		panic(err)
	}

	pool := memory.NewGoAllocator()
	rb := array.NewRecordBuilder(pool, arrowSchema)
	defer rb.Release()

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", "beta"}, nil)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		rec.Retain()
		yield(rec, nil)
	}

	var dataFiles []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, arrowSchema, records) {
		if err != nil {
			panic(err)
		}
		dataFiles = append(dataFiles, df)
	}

	tx := tbl.NewTransaction()
	if err := tx.AddDataFiles(ctx, dataFiles, nil, table.WithoutDuplicateCheck()); err != nil {
		panic(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Wrote Iceberg table:")
	fmt.Println(tableRoot)
}
