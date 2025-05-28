package redisstore

import (
	"github.com/tidwall/redcon"
	"strings"
)

type CommandHandler func(conn redcon.Conn, cmd redcon.Command) (closeConn bool)

type CommandTable struct {
	table map[string]CommandHandler
}

func NewCommandTable() *CommandTable {
	table := make(map[string]CommandHandler)
	table["ping"] = func(conn redcon.Conn, cmd redcon.Command) bool {
		conn.WriteString("PONG")
		return false
	}
	return &CommandTable{
		table: table,
	}
}

func (ct *CommandTable) RunCommand(conn redcon.Conn, cmd redcon.Command) {
	redisCmd := strings.ToLower(string(cmd.Args[0]))
	handler, exists := ct.table[redisCmd]
	if !exists {
		conn.WriteError("ERR command '" + redisCmd + "' not implemented yet")
	} else {
		closeConn := handler(conn, cmd)
		if closeConn {
			conn.Close()
		}
	}
}
