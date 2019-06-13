package registry

import (
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/hql-schema/golang"
	. "github.com/saichler/utils/golang"
	"reflect"
	"strconv"
	"strings"
)

type Column struct {
	table    *Table
	field    reflect.StructField
	metaData *ColumnMetaData
}

func (c *Column) MetaData() *ColumnMetaData {
	return c.metaData
}

func (c *Column) inspect(parent *SchemaNode) {
	c.parseMetaData()
	if isStruct(c.field.Type) {
		strct := getStruct(c.field.Type)
		c.metaData.columnTableName = strct.Name()
		schemaNode := c.table.ormRegistry.schema.RegisterNode(c.Name(), parent, strct)
		c.table.ormRegistry.register(strct, schemaNode)
	}
}

func (c *Column) Type() reflect.Type {
	return c.field.Type
}

func (c *Column) Name() string {
	return c.field.Name
}

func (c *Column) Table() *Table {
	return c.table
}

func isStruct(typ reflect.Type) bool {
	if typ.Kind() == reflect.Struct {
		return true
	} else if typ.Kind() == reflect.Ptr {
		return isStruct(typ.Elem())
	} else if typ.Kind() == reflect.Slice {
		return isStruct(typ.Elem())
	} else if typ.Kind() == reflect.Map {
		return isStruct(typ.Elem())
	}
	return false
}

func getStruct(typ reflect.Type) reflect.Type {
	if typ.Kind() == reflect.Struct {
		return typ
	} else if typ.Kind() == reflect.Ptr {
		return getStruct(typ.Elem())
	} else if typ.Kind() == reflect.Slice {
		return getStruct(typ.Elem())
	} else if typ.Kind() == reflect.Map {
		return getStruct(typ.Elem())
	}
	return nil
}

func (c *Column) parseMetaData() {
	if c.metaData == nil {
		c.metaData = &ColumnMetaData{}
	}
	c.metaData.title = c.field.Name
	c.metaData.size = 128
	tags := string(c.field.Tag)
	if tags == "" {
		return
	}
	splits := strings.Split(tags, " ")
	for _, tag := range splits {
		c.getTag(tag)
	}
}

func (c *Column) getTag(tag string) {
	if strings.Trim(tag, " ") == "" {
		return
	}
	index := strings.Index(tag, "=")
	if index == -1 {
		return
	}
	name := tag[0:index]
	value := tag[index+1:]
	if name == TITLE {
		c.metaData.title = value
	} else if name == SIZE {
		val, err := strconv.Atoi(value)
		if err != nil {
			Error("Unable to parse field size from:" + value + " in field:" + c.field.Name)
		} else {
			c.metaData.size = val
		}
	} else if name == MASK {
		c.metaData.mask = true
	} else if name == IGNORE {
		c.metaData.ignore = true
	} else if name == PRIMARY_KEY {
		c.metaData.primaryKey = value
	} else if name == UNIQUE_KEY {
		c.metaData.uniqueKeys = value
	} else if name == NON_UNIQUE_KEY {
		c.metaData.nonUniqueKeys = value
	}
}
