## Purpose

Every application that I ever was a part of produced some sort of data bug that required a delete of data or a backfill of some sort to fix the data. Instead of building a system to prevent this, this tool is the start of building a tool to repair the data. Right now it supports only efficiently deleting data from a large table over time.

```
Usage of ./go-safely-change-data:
  -concurrency int
    	How many concurrent changes at once (default 5)
  -db string
    	Name of the DB (default "yourdb")
  -dry_run
    	Print the changes out (default true)
  -table string
    	Name of the table to delete rows from
  -where string
    	WHERE clause to filter rows to delete (default "1=1")
```

## Features
* Makefile to build consistently in a local environment and remote environment
* Dockerfile for a generic image to build for
* Go Mod (which you should to your project path change)
* options for dry_run

## Installing via brew
* `brew install --verbose --build-from-source brew/Formula/go-safely-change-data.rb`
