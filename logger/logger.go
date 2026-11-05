package logger

import (
	"fmt"
	"log"
	"os"
)

// file appends to chitchat.log in the current working directory.
var file *log.Logger

func init() {
	if f, err := os.OpenFile("MutualExclusion.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
		file = log.New(f, "", log.LstdFlags)
	}
}

// skal ændres
func Log(component, action, username string) {
	line := fmt.Sprintf("component=%s action=%s client=%s", component, action, username)
	log.Println(line)
	if file != nil {
		file.Println(line)
	}
}
