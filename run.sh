#!/bin/sh

go mod tidy
go run repro.go
duckdb -init query.sql