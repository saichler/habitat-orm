package common

import (
	. "github.com/saichler/orm/golang/orm/registry"
)

type Persistency interface {
	Init(*OrmRegistry) error
	TxStart()
	Marshal()
}
