package consul

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(M *testing.M) {
	consul, ok := os.LookupEnv(ConsulEnvironment)
	if ok && consul != DefaultAddress {
		fmt.Printf("ERROR: ENV[Consul_HTTP_ADDR] != %s ", DefaultAddress)
		os.Exit(1)
	}
	os.Exit(M.Run())
}
