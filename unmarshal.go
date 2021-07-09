package incite

import (
	"encoding/json"
	"reflect"
	"strconv"
)

// Unmarshal converts the CloudWatch Logs Insights result data and stores the
// result in the value pointed to by v.
//
// If v does not contain a non-nil pointer to a slice, array, or interface
// value, Unmarshal returns an InvalidUnmarshalError. If v contains a pointer to
// a slice or array, and the element type of the slice or array is not one of
// the element types listed below, Unmarshal returns an InvalidUnmarshalError.
//
// TODO: Finish documentation here.
func Unmarshal(data []Result, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return &InvalidUnmarshalError{TargetType: reflect.TypeOf(v)}
	}

	m := len(data)
	a := rv.Elem()

	switch a.Kind() {
	case reflect.Slice: // v is a pointer to a slice
		n := a.Cap()
		if m > n {
			b := reflect.MakeSlice(a.Type(), m, m)
			reflect.Copy(b, a)
			a.Set(b)
		} else if m < n {
			a.SetLen(m)
		}
		return array(data, a, rv)
	case reflect.Array: // v is a pointer to an array
		n := a.Len()
		if n >= m {
			err := array(data, a, rv)
			if err != nil {
				return err
			}
			z := reflect.Zero(a.Type().Elem())
			for i := m; i < n; i++ { // Zero out remainder
				a.Index(i).Set(z)
			}
			return nil
		} else {
			return array(data[:n], a, rv)
		}
	case reflect.Interface: // v is a pointer to an interface
		b := reflect.MakeSlice(reflect.TypeOf(data), m, m)
		a.Set(b)
		return array(data, a, rv)
	default:
		return &InvalidUnmarshalError{TargetType: rv.Type()}
	}
}

func array(data []Result, a reflect.Value, rv reflect.Value) error {
	f, err := selectResultUnpackFunc(a.Elem().Type(), rv)
	if err != nil {
		return err
	}
	for i, r := range data {
		err = f(i, r, a.Index(i))
		if err != nil {
			return err
		}
	}
	return nil
}

type resultUnpackFunc func(i int, r Result, v reflect.Value) error

type fieldUnpackFunc func(i, j int, field, value string) (reflect.Value, error)

var (
	resultType = reflect.TypeOf(Result{})
)

func selectResultUnpackFunc(t reflect.Type, rv reflect.Value) (resultUnpackFunc, error) {
	switch t.Kind() {
	case reflect.Ptr:
		return selectResultUnpackFuncPtr(t, rv)
	case reflect.Interface:
		return unpackResultCopy, nil
	case reflect.Map:
		return selectResultUnpackFuncMap(t, rv)
	case reflect.Struct:
		if t == resultType {
			return unpackResultCopy, nil
		}
		return selectResultUnpackFuncStruct(t, rv)
	default:
		return nil, &InvalidUnmarshalError{TargetType: rv.Type(), ElemType: t}
	}
}

func selectResultUnpackFuncPtr(ptrType reflect.Type, rv reflect.Value) (resultUnpackFunc, error) {
	elemType := ptrType.Elem()
	f, err := selectResultUnpackFunc(elemType, rv)
	if err != nil {
		return nil, err
	}
	return func(i int, r Result, ptr reflect.Value) error {
		elem := reflect.New(elemType)
		err := f(i, r, elem)
		if err != nil {
			return err
		}
		ptr.Set(elem)
		return nil
	}, nil
}

func selectResultUnpackFuncMap(mapType reflect.Type, rv reflect.Value) (resultUnpackFunc, error) {
	keyType := mapType.Key()
	if keyType.Kind() != reflect.String {
		return nil, &InvalidUnmarshalError{TargetType: rv.Type(), ElemType: mapType}
	}
	valueType := mapType.Elem()
	switch valueType.Kind() {
	case reflect.String:
		return unpackResultMapStringString, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return unpackMapStringInt(valueType), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return unpackMapStringUint(valueType), nil
	case reflect.Float32, reflect.Float64:
		return unpackMapStringFloat(valueType), nil
	case reflect.Interface:
		return unpackMapStringJSONFuzzy, nil
	case reflect.Map, reflect.Slice, reflect.Array:
		return unpackMapStringJSONAggregate, nil
	// TODO: Should check if the thingy implements the encoding.TextUnmarshaler interface
	//       and try that too. Note that time.Time implements this interface.
	default:
		return nil, &InvalidUnmarshalError{TargetType: rv.Type(), ElemType: mapType}
	}
}

func selectResultUnpackFuncStruct(structType reflect.Type, rv reflect.Value) (resultUnpackFunc, error) {
	type structField struct {
		fieldIndex int
		unpackFunc fieldUnpackFunc
	}
	// I want to traverse the struct look at the fields and tags:
	//    - Throw an error if any invalid types exist, for example try to put @message into int or it's a channel or something,
	//      or try to put @ingestionTime or @timestamp into a field that's not a string, big enough integer, or time.Time{}.
	//    - Make a map[string]fieldUnpackFunc for all discovered fields in the
	//      struct. That way when we walk through the ResultFields in each Result, it's super trivial
	//      to just lookup the single thing that does the thing.
	n := structType.NumField()
	sfs := make(map[string]structField, n)
	for i := 0; i < n; i++ {
		if false {
			field := "foo"
			sfs[field] = structField{
				fieldIndex: i,
				// TODO need to figure out the unpack func.
			}
		}
	}
	return func(i int, r Result, v reflect.Value) error {
		for j := range r.Fields {
			field := r.Fields[j].Field
			if sf, ok := sfs[field]; ok {
				value, err := sf.unpackFunc(i, j, field, r.Fields[j].Value)
				if err != nil {
					return err // TODO: return the correct error
				}
				v.Field(sf.fieldIndex).Set(value)
			}
		}
		return nil
	}, nil
}

func unpackResultCopy(_ int, r Result, v reflect.Value) error {
	v.Set(reflect.ValueOf(resultCopy(r)))
	return nil
}

func unpackMapStringAnyElem(i int, r Result, mapValue reflect.Value, conv fieldUnpackFunc) error {
	if mapValue.IsNil() {
		mapValue.Set(reflect.MakeMap(mapValue.Type()))
	}
	for j, f := range r.Fields {
		key := reflect.ValueOf(f.Field)
		elem, err := conv(i, j, f.Field, f.Value)
		if err != nil {
			return err
		}
		mapValue.SetMapIndex(key, elem)
	}
	return nil
}

func unpackResultMapStringString(i int, r Result, mapValue reflect.Value) error {
	return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
		return reflect.ValueOf(value), nil
	})
}

func unpackMapStringInt(valueType reflect.Type) resultUnpackFunc {
	return func(i int, r Result, mapValue reflect.Value) error {
		return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || reflect.Zero(valueType).OverflowInt(n) {
				return reflect.Value{}, &UnmarshalResultTypeError{
					UnmarshalResultError: UnmarshalResultError{
						ResultIndex: i,
						FieldIndex:  j,
						Field:       field,
					},
					Value: value,
					Type:  valueType,
				}
			}
			return reflect.ValueOf(n).Convert(valueType), nil
		})
	}
}

func unpackMapStringUint(valueType reflect.Type) resultUnpackFunc {
	return func(i int, r Result, mapValue reflect.Value) error {
		return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil || reflect.Zero(valueType).OverflowUint(n) {
				return reflect.Value{}, &UnmarshalResultTypeError{
					UnmarshalResultError: UnmarshalResultError{
						ResultIndex: i,
						FieldIndex:  j,
						Field:       field,
					},
					Value: value,
					Type:  valueType,
				}
			}
			return reflect.ValueOf(n).Convert(valueType), nil
		})
	}
}

func unpackMapStringFloat(valueType reflect.Type) resultUnpackFunc {
	return func(i int, r Result, mapValue reflect.Value) error {
		return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
			n, err := strconv.ParseFloat(value, 64)
			if err != nil || reflect.Zero(valueType).OverflowFloat(n) {
				return reflect.Value{}, &UnmarshalResultTypeError{
					UnmarshalResultError: UnmarshalResultError{
						ResultIndex: i,
						FieldIndex:  j,
						Field:       field,
					},
					Value: value,
					Type:  valueType,
				}
			}
			return reflect.ValueOf(n).Convert(valueType), nil
		})
	}
}

func unpackMapStringJSONFuzzy(i int, r Result, mapValue reflect.Value) error {
	// TODO: We should be able to unpack @timestamp and @ingestionTime into time.Time in fuzzy mode.
	return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
	For:
		for _, c := range value {
			switch c {
			case '\t', '\n', '\r', ' ': // Might be JSON, keep skipping whitespace to find out.
				break
			case '{', '[', '"': // Might be JSON, try to unpack it.
				v, err := unpackJSONFuzzy(value)
				if err != nil {
					return v, nil
				}
			default: // Definitely not JSON.
				break For
			}
		}
		return reflect.ValueOf(value), nil
	})
}

func unpackMapStringJSONAggregate(i int, r Result, mapValue reflect.Value) error {
	return unpackMapStringAnyElem(i, r, mapValue, func(i, j int, field, value string) (reflect.Value, error) {
		mapType := mapValue.Type()
		elemType := mapType.Elem()
		elemPtr := reflect.New(elemType)
		err := json.Unmarshal([]byte(value), elemPtr.Interface())
		if err != nil {
			return reflect.Value{}, err
		}
		return elemPtr.Elem(), nil
	})
}

func unpackJSONFuzzy(s string) (reflect.Value, error) {
	var i interface{}
	err := json.Unmarshal([]byte(s), &i)
	if err != nil {
		return reflect.Value{}, err
	}
	return reflect.ValueOf(i), nil
}

func resultCopy(r Result) Result {
	fields := make([]ResultField, len(r.Fields))
	copy(fields, r.Fields)
	return Result{
		Ptr:    r.Ptr,
		Fields: fields,
	}
}

// An InvalidUnmarshalError describes an invalid type passed to Unmarshal.
//
// The type of the target argument to Unmarshal must be a non-nil pointer to an
// slice, array, or interface value. If it is a slice or array type, then the
// elements
type InvalidUnmarshalError struct {
	TargetType reflect.Type
	ElemType   reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	// FIXME: This logic is a bit painful.

	//if e.Type == nil {
	//	return "incite: Unmarshal(nil)"
	//}
	//errNotPtrToSliceOrArray := func() string {
	//	return "incite: Unmarshal(not pointer to slice or array: " + e.Type.String() + ")"
	//}
	//if e.Type.Kind() != reflect.Ptr {
	//	return errNotPtrToSliceOrArray()
	//}
	//switch e.Type.Elem().Kind() {
	//case reflect.Slice, reflect.Array:
	//	return "incite: Unmarshal(nil: " + e.Type.String() + ")"
	//default:
	//	return errNotPtrToSliceOrArray()
	//}

	return "TODO: pls make me work properly"
}

// An UnmarshalResultError describes a failure to unmarshal a specific
// ResultField value within a specific Result.
type UnmarshalResultError struct {
	error
	ResultIndex int
	FieldIndex  int
	Field       string
}

// An UnmarshalResultTypeError describes a failure to unmarshal a specific
// non-JSON ResultField value within a specific Result because the ResultField
// value is not appropriate for the target type.
type UnmarshalResultTypeError struct {
	UnmarshalResultError
	Value string
	Type  reflect.Type
}

func (e *UnmarshalResultTypeError) Error() string {
	return "TODO: pls make me work properly"
}

type UnmarshalResultJSONTypeError struct {
	UnmarshalResultError
	Cause *json.UnmarshalTypeError
}

func (e *UnmarshalResultJSONTypeError) Error() string {
	return "TODO: pls make me work properly"
}

type UnmarshalResultJSONSyntaxError struct {
	UnmarshalResultError
	Cause *json.SyntaxError
}

func (e *UnmarshalResultJSONSyntaxError) Error() string {
	return "TODO: pls make me work properly"
}

type UnmarshalJSONError struct {
	Index  int
	Result Result
}
