package logger

import (
	"fmt"
	"log"
	"os"
)

var fileLogger *log.Logger

func init() {
	f, err := os.OpenFile("MutualExclusion.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}

	fileLogger = log.New(f, "", log.LstdFlags)
}

func Info(format string, args ...any) {
	line := fmt.Sprintf("[INFO] "+format, args...)
	log.Println(line)
	fileLogger.Println(line)
}

func Warn(format string, args ...any) {
	line := fmt.Sprintf("[WARN] "+format, args...)
	log.Println(line)
	fileLogger.Println(line)
}

func Error(format string, args ...any) {
	line := fmt.Sprintf("[ERROR] "+format, args...)
	log.Println(line)
	fileLogger.Println(line)
}

func Log(nodeID string, event string, details string) {
	Info("node=%s event=%s %s", nodeID, event, details)
}

/*
examples to be used in the .go file containing the Ricart-Agrawala algorithm:

for requests:
	logger.Info("Node %d requesting CS with timestamp %d", n.id, n.requestTS)
    logger.Info("Node %d sending REQUEST to Node %d", n.id, peerID)

for replies:
	logger.Info("Node %d received REQUEST from %d (ts=%d)", n.id, req.NodeId, req.Timestamp)
    logger.Info("Node %d granting access to Node %d", n.id, req.NodeId)
    logger.Warn("Node %d deferring reply to %d (my ts=%d, their ts=%d)", n.id, req.NodeId, n.requestTS, req.Timestamp)

for the critical section:
	logger.Info("Node %d ENTERING critical section", n.id)
    logger.Info("Node %d EXITING critical section", n.id)

for the deferred replies:
	logger.Info("Node %d sending deferred replies to %v", n.id, deferredList)

for the system events:
	logger.Info("Node %d started at address %s", n.id, n.address)
    logger.Info("Node %d discovered peers: %v", n.id, peerIDs)

*/
