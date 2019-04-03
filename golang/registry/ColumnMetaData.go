package registry

type ColumnMetaData struct {
	title           string
	size            int
	ignore          bool
	mask            bool
	primaryKey      string
	uniqueKeys      string
	nonUniqueKeys   string
	columnTableName string
}

func (cm *ColumnMetaData) Title() string {
	return cm.title
}

func (cm *ColumnMetaData) Size() int {
	return cm.size
}

func (cm *ColumnMetaData) Ignore() bool {
	return cm.ignore
}

func (cm *ColumnMetaData) Mask() bool {
	return cm.mask
}

func (cm *ColumnMetaData) ColumnTableName() string {
	return cm.columnTableName
}
