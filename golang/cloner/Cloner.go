package cloner

import "reflect"

var cloners = make(map[reflect.Kind]func(reflect.Value) reflect.Value)

func initCloners() {
	if len(cloners) == 0 {
		cloners[reflect.Int] = intCloner
		cloners[reflect.Int32] = int32Cloner
		cloners[reflect.Ptr] = ptrCloner
		cloners[reflect.Struct] = structCloner
		cloners[reflect.String] = stringCloner
		cloners[reflect.Slice] = sliceCloner
		cloners[reflect.Map] = mapCloner
		cloners[reflect.Bool] = boolCloner
		cloners[reflect.Int64] = int64Cloner
		cloners[reflect.Uint] = uintCloner
		cloners[reflect.Uint32] = uint32Cloner
		cloners[reflect.Uint64] = uint64Cloner
		cloners[reflect.Float32] = float32Cloner
		cloners[reflect.Float64] = float64Cloner
	}
}

func Clone(any interface{}) interface{} {
	initCloners()
	value := reflect.ValueOf(any)
	valueClone := clone(value)
	return valueClone.Interface()
}

func clone(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	kind := value.Kind()
	cloner := cloners[kind]
	if cloner == nil {
		panic("No cloner for kind:" + kind.String())
	}
	return cloner(value)
}

func sliceCloner(value reflect.Value) reflect.Value {
	if value.IsNil() {
		return value
	}
	newSlice := reflect.MakeSlice(reflect.SliceOf(value.Index(0).Type()), value.Len(), value.Len())
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i)
		elemClone := clone(elem)
		newSlice.Index(i).Set(elemClone)
	}
	return newSlice
}

func ptrCloner(value reflect.Value) reflect.Value {
	if value.IsNil() {
		return value
	}
	ptrKind := value.Elem().Kind()
	if ptrKind == reflect.Struct {
		newPtr := reflect.New(value.Elem().Type())
		newPtr.Elem().Set(clone(value.Elem()))
		return newPtr
	} else {
		return clone(value)
	}
}

func structCloner(value reflect.Value) reflect.Value {
	cloneStruct := reflect.New(value.Type()).Elem()
	structType := value.Type()
	for i := 0; i < structType.NumField(); i++ {
		fieldValue := value.Field(i)
		cloned := clone(fieldValue)
		cloneStruct.Field(i).Set(cloned)
	}
	return cloneStruct
}

func mapCloner(value reflect.Value) reflect.Value {
	if value.IsNil() {
		return value
	}
	mapKeys := value.MapKeys()
	mapClone := reflect.MakeMapWithSize(value.Type(), len(mapKeys))
	for _, key := range mapKeys {
		mapElem := value.MapIndex(key)
		mapElemClone := clone(mapElem)
		mapClone.SetMapIndex(key, mapElemClone)
	}
	return mapClone
}

func intCloner(value reflect.Value) reflect.Value {
	i := value.Int()
	return reflect.ValueOf(int(i))
}

func uintCloner(value reflect.Value) reflect.Value {
	i := value.Uint()
	return reflect.ValueOf(uint(i))
}

func uint32Cloner(value reflect.Value) reflect.Value {
	i := value.Uint()
	return reflect.ValueOf(uint32(i))
}

func uint64Cloner(value reflect.Value) reflect.Value {
	i := value.Uint()
	return reflect.ValueOf(uint64(i))
}

func float32Cloner(value reflect.Value) reflect.Value {
	i := value.Float()
	return reflect.ValueOf(float32(i))
}

func float64Cloner(value reflect.Value) reflect.Value {
	i := value.Float()
	return reflect.ValueOf(float64(i))
}

func boolCloner(value reflect.Value) reflect.Value {
	b := value.Bool()
	return reflect.ValueOf(b)
}

func int32Cloner(value reflect.Value) reflect.Value {
	i := value.Int()
	return reflect.ValueOf(int32(i))
}

func int64Cloner(value reflect.Value) reflect.Value {
	i := value.Int()
	return reflect.ValueOf(int64(i))
}

func stringCloner(value reflect.Value) reflect.Value {
	s := value.String()
	return reflect.ValueOf(s)
}
