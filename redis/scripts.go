package redis

import (
	_ "embed"

	rdb "github.com/redis/go-redis/v9"
)

//go:embed scripts/push.lua
var pushScript string

//go:embed scripts/pop.lua
var popScript string

//go:embed scripts/heartbeat.lua
var heartbeatScript string

//go:embed scripts/complete.lua
var completeScript string

//go:embed scripts/fail.lua
var failScript string

//go:embed scripts/release.lua
var releaseScript string

//go:embed scripts/age.lua
var ageScript string

//go:embed scripts/requeue_zombies.lua
var requeueZombiesScript string

type scripts struct {
	push           *rdb.Script
	pop            *rdb.Script
	heartbeat      *rdb.Script
	complete       *rdb.Script
	fail           *rdb.Script
	release        *rdb.Script
	age            *rdb.Script
	requeueZombies *rdb.Script
}

func loadScripts() scripts {
	return scripts{
		push:           rdb.NewScript(pushScript),
		pop:            rdb.NewScript(popScript),
		heartbeat:      rdb.NewScript(heartbeatScript),
		complete:       rdb.NewScript(completeScript),
		fail:           rdb.NewScript(failScript),
		release:        rdb.NewScript(releaseScript),
		age:            rdb.NewScript(ageScript),
		requeueZombies: rdb.NewScript(requeueZombiesScript),
	}
}
