package curlyq

import (
	"bytes"

	"github.com/go-redis/redis/v7"
	"github.com/markbates/pkger"
)

// prepScripts initializes pkger with the scripts in the lua directory.
func prepScripts() {
	pkger.Include("/lua")
}

// loadLua uses pkger to load scripts from the lua directory.
// It panics if the requested script has not been properly packaged.
func loadLua(path string) *redis.Script {
	luaFile, err := pkger.Open(path)
	if err != nil {
		panic(err)
	}
	defer luaFile.Close()
	luaBytes := bytes.Buffer{}
	luaBytes.ReadFrom(luaFile)
	return redis.NewScript(luaBytes.String())
}
