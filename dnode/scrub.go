package dnode

import (
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
)

func (s *Scrubber) Scrub(obj interface{}) (callbacks map[string]Path) {
	callbacks = make(map[string]Path)
	s.collectCallbacks(obj, make(Path, 0), callbacks)
	return callbacks
}

// collectCallbacks walks over the rawObj and populates callbackMap
// with callbacks. This is a recursive function. The top level send must
// sends arguments as rawObj, an empty path and empty callbackMap parameter.
func (s *Scrubber) collectCallbacks(rawObj interface{}, path Path, callbackMap map[string]Path) {
	// fmt.Printf("--- collectCallbacks: %#v\n", rawObj)

	// TODO Use reflection and remove this outer switch statement.
	switch obj := rawObj.(type) {
	// skip nil values
	case nil:
	case []interface{}:
		for i, item := range obj {
			s.collectCallbacks(item, append(path, i), callbackMap)
		}
	case map[string]interface{}:
		for key, item := range obj {
			s.collectCallbacks(item, append(path, key), callbackMap)
		}
	// Dereference and continue.
	case *[]interface{}:
		if obj != nil {
			s.collectCallbacks(*obj, path, callbackMap)
		}
	// Dereference and continue.
	case *map[string]interface{}:
		if obj != nil {
			s.collectCallbacks(*obj, path, callbackMap)
		}
	default:
		v := reflect.ValueOf(obj)

		switch v.Kind() {
		case reflect.Func:
			panic("cannot marshal func, use Callback() to wrap it")
			// s.registerCallback(v, path, callbackMap)
		case reflect.Ptr:
			e := v.Elem()
			if e == reflect.ValueOf(nil) {
				return
			}

			v2 := reflect.ValueOf(e.Interface())
			if isFunction(v2) {
				s.registerCallback(v2, path, callbackMap)
				return
			}

			s.collectFields(v2, path, callbackMap)
			s.collectMethods(v, path, callbackMap)
		case reflect.Struct:
			if isFunction(v) {
				s.registerCallback(v, path, callbackMap)
				return
			}

			s.collectFields(v, path, callbackMap)
			s.collectMethods(v, path, callbackMap)
		}
	}
}

var callerType = reflect.TypeOf((*caller)(nil)).Elem()

func isFunction(v reflect.Value) bool {
	if v.Kind() != reflect.Struct {
		return false
	}
	callerField := v.FieldByName("Caller")
	if !callerField.IsValid() {
		return false
	}
	if callerField.Kind() != reflect.Interface {
		return false
	}
	return callerField.Type().Implements(callerType)
}

// collectFields collects callbacks from the exported fields of a struct.
func (s *Scrubber) collectFields(v reflect.Value, path Path, callbackMap map[string]Path) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)

		if f.PkgPath != "" { // unexported
			continue
		}

		// Do not collect callbacks for "-" tagged fields.
		tag := f.Tag.Get("dnode")
		if tag == "-" { // "-" means do not collect callbacks of this field
			continue
		}

		tag = f.Tag.Get("json")
		if tag == "-" {
			continue
		}

		var name string
		if tag != "" {
			name = tag
		} else {
			name = f.Name
		}

		if f.Anonymous {
			s.collectCallbacks(v.Field(i).Interface(), path, callbackMap)
		} else {
			s.collectCallbacks(v.Field(i).Interface(), append(path, name), callbackMap)
		}
	}
}

func (s *Scrubber) collectMethods(v reflect.Value, path Path, callbackMap map[string]Path) {
	for i := 0; i < v.NumMethod(); i++ {
		if v.Type().Method(i).PkgPath == "" { // exported
			name := v.Type().Method(i).Name
			name = strings.ToLower(name[0:1]) + name[1:]
			s.registerCallback(v.Method(i), append(path, name), callbackMap)
		}
	}
}

// registerCallback is called when a function/method is found in arguments array.
func (s *Scrubber) registerCallback(val reflect.Value, path Path, callbackMap map[string]Path) {
	// TODO Refactor registerCallback function.
	// fmt.Printf("--- registerCallback: %#v\n", val.Interface())

	if len(path) == 0 {
		panic("root element must be a struct or slice")
	}

	var cb func(*Partial) // We are going to save this in scubber

	// val can be a type of Function or func(*Partial)
	if isFunction(val) {
		callerValue := val.FieldByName("Caller").Interface()
		if callerValue == nil {
			return
		}
		cb = callerValue.(callback)
	} else if i, ok := val.Interface().(func(*Partial)); ok {
		cb = i
	} else {
		return
	}

	// Subtract one to start counting from zero.
	// This is not absolutely necessary, just cosmetics.
	next := atomic.AddUint64(&s.seq, 1) - 1

	seq := strconv.FormatUint(next, 10)

	// Save in scubber callbacks so we can call it when we receive a call.
	s.callbacks[next] = cb

	// Add to callback map to be sent to remote.
	// Make a copy of path because it is reused in caller.
	pathCopy := make(Path, len(path))
	copy(pathCopy, path)
	callbackMap[seq] = pathCopy

	// fmt.Println("--- REGISTERED")
}
