package main

// init adds "ydb" backend flags.
func init() {
	handlerFlags["ydb"] = &ydbFlags
}
