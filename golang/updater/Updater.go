package updater

import "reflect"

var updaters = make(map[reflect.Kind]func(reflect.Value, reflect.Value))

func initUpdaters() {
	if len(updaters) == 0 {
		updaters[reflect.Int] = intUpdater
		updaters[reflect.Int32] = intUpdater
		updaters[reflect.Ptr] = ptrUpdater
		updaters[reflect.Struct] = structUpdater
		updaters[reflect.String] = stringUpdater
		updaters[reflect.Slice] = sliceUpdater
		updaters[reflect.Map] = mapUpdater
		updaters[reflect.Bool] = boolUpdater
		updaters[reflect.Int64] = intUpdater
		updaters[reflect.Uint] = uintUpdater
		updaters[reflect.Uint32] = uintUpdater
		updaters[reflect.Uint64] = uintUpdater
		updaters[reflect.Float32] = floatUpdater
		updaters[reflect.Float64] = floatUpdater
	}
}

func Update(old, new interface{}) {
	initUpdaters()
	oldValue := reflect.ValueOf(old)
	newValue := reflect.ValueOf(new)
	update(oldValue, newValue)
}

func update(old reflect.Value, new reflect.Value) {
	oldKind := old.Kind()
	newKind := new.Kind()
	if oldKind != newKind {
		panic("Old Kind " + oldKind.String() + " and New Kind " + newKind.String() + " are not the same")
	}
	updater := updaters[oldKind]
	if updater == nil {
		panic("No updater for kind:" + oldKind.String())
	}
	updater(old, new)
}

func sliceUpdater(old, new reflect.Value) {
	if new.IsNil() || new.Len() == 0 {
		return
	}

	if old.IsNil() || old.Len() == 0 {
		old.Set(new)
		return
	}

	if old.Index(0).Kind() == reflect.Ptr && old.Index(0).Elem().Kind() == reflect.Struct {
		//@TODO - Implement ORM logic here if the elements in the slice have a unique key
		old.Set(new)
	} else {
		old.Set(new)
	}
}

func ptrUpdater(old, new reflect.Value) {
	if new.IsNil() {
		return
	}

	if old.IsNil() {
		old.Elem().Set(new.Elem())
		return
	}

	oldKind := old.Elem().Kind()
	newKind := new.Elem().Kind()
	if oldKind != newKind {
		panic("Old pointer kind " + oldKind.String() + " is different than new kind " + newKind.String())
	}

	update(old.Elem(), new.Elem())
}

func structUpdater(old, new reflect.Value) {
	oldType := old.Type().Name()
	newType := new.Type().Name()

	if oldType != newType {
		panic("Old Struct Type:" + oldType + " is not equale to new type:" + newType)
	}

	for i := 0; i < old.Type().NumField(); i++ {
		oldValue := old.Field(i)
		newValue := new.Field(i)
		update(oldValue, newValue)
	}
}

func mapUpdater(old, new reflect.Value) {
	if new.IsNil() || len(new.MapKeys()) == 0 {
		return
	}

	if old.IsNil() || len(old.MapKeys()) == 0 {
		old.Set(new)
		return
	}

	mapKeys := new.MapKeys()
	for _, key := range mapKeys {
		newElem := new.MapIndex(key)
		if newElem.Kind() == reflect.Ptr {
			oldElem := old.MapIndex(key)
			if !oldElem.IsValid() || oldElem.IsNil() {
				old.SetMapIndex(key, newElem)
			} else {
				update(oldElem, newElem)
			}
		} else {
			old.SetMapIndex(key, newElem)
		}
	}
}

func intUpdater(old, new reflect.Value) {
	v := new.Int()
	if v != 0 {
		old.SetInt(v)
	}
}

func uintUpdater(old, new reflect.Value) {
	v := new.Uint()
	if v != 0 {
		old.SetUint(v)
	}
}

func floatUpdater(old, new reflect.Value) {
	v := new.Float()
	if v != 0 {
		old.SetFloat(v)
	}
}

func boolUpdater(old, new reflect.Value) {
	old.SetBool(new.Bool())
}

func stringUpdater(old, new reflect.Value) {
	v := new.String()
	if v != "" {
		old.SetString(v)
	}
}
