package incite

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Unmarshal converts the CloudWatch Logs Insights result data into the
// user-defined type indicated by v, and stores the result in the value
// pointed to by v.
//
// The argument v must contain a non-nil pointer whose ultimate target
// is a slice, array, or interface value. If v ultimately targets a
// interface{}, it is treated as if it targets a []map[string]string.
//
// The element type of the array or slice must target a map type, struct
// type. As special cases, elements of type interface{} and Result
// are also allowed. If the element type targets a map, the maps keys
// must be strings and its value type must target a string type,
// interface{}, or any type that implements encoding.TextUnmarshaler.
//
// To unmarshal data into an array or slice of maps, Unmarshal uses
// the ResultField name as the map key and the ResultField value as its
// value. If the map value targets an encoding.TextUnmarshaler, the
// value's UnmarshalText value is used to unmarshal the value. If the
// map value targets a string type, the ResultField's value is directly
// inserted as the field value in the map. As a special case, if the
// map value targets interface{}, Unmarshal first tries to unmarshal
// the value as JSON using json.Unmarshal, and falls back to the plain
// string value if JSON unmarshaling fails.
//
// To unmarshal data into a struct type, Unmarshal uses the following
// rules top-level rules:
//
// • A struct field with an "incite" tag receives the value of the
// ResultField field named in the tag. Unmarshaling of the field value
// is done according to rules discussed below. If the tag is "-" the
// field is ignored. If the field type does not ultimately target a
// struct field unmarshalable type, an InvalidUnmarshalError is
// returned.
//
// • A struct field with a "json" tag receives the the value of the
// ResultField field named in the tag using the json.Unmarshal function
// with the ResultField value as the input JSON and the struct field
// address as the target. If the tag is "-" the field is ignored. The
// field type is not checked for validity.
//
// • The "incite" tag takes precedence over the "json" tag so they
// should not be used together on the same struct field.
//
// • A struct field with no "incite" or "json" tag receives the value
// of the ResultField field sharing the same case-sensitive name as the
// struct field, but only if the field type ultimately targets a
// struct field unmarshablable type. Otherwise the field is ignored.
//
// The following types are considered struct field unmarshalable types:
//
//  bool
//  int8, int16, int32, int64, int
//  uint8, uint16, uint32, uint64, uint
//  float32, float64
//  interface{}
//  []byte
//  Any map, struct, slice, or array type
//
// A struct field targeting interface{} or any map, struct, slice, or
// array type is assumed to contain valid JSON and unmarshaled using
// json.Unmarshal. Any other field is decoded from its string
// representation using the intuitive approach. As a special case, if
// a CloudWatch Logs timestamp field (@timestamp or @ingestionTime) is
// named in an "incite" tag, it may only target a time.Time or string
// value. If it targets a time.Time, the value is decoded using
// TimeLayout with the time.Parse function.
//
// If a target type rule is violated, Unmarshal returns
// InvalidUnmarshalError. If a result field value cannot be decoded,
// Unmarshal stops unmarshaling and returns UnmarshalResultFieldValueError.
//
// The value pointed to by v may have changed even if Unmarshal returns
// an error.
func Unmarshal(data []Result, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return &InvalidUnmarshalError{Type: reflect.TypeOf(v)}
	}

	// TODO: Dig here to cut through any extra pointers.

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
		return array(data, rv, a)
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
			return array(data[:n], rv, a)
		}
	case reflect.Interface: // v is a pointer to an interface
		b := reflect.MakeSlice(reflect.TypeOf([]map[string]string{}), m, m)
		a.Set(reflect.ValueOf(b.Interface()))
		return array(data, rv, b)
	default:
		return &InvalidUnmarshalError{Type: rv.Type()}
	}
}

func array(data []Result, rv, a reflect.Value) error {
	s := decodeState{
		rv:   rv,
		data: data,
	}
	elemType, depth := dig(a.Type().Elem())
	f, err := s.selRowDecodeFunc(elemType)
	if err != nil {
		return err
	}
	for i := range data {
		s.i = i
		s.j = -1
		s.dst = fill(a.Index(i), depth)
		err = f(&s)
		if err != nil {
			return err
		}
	}
	return nil
}

// dig finds and returns the ultimate non-pointer value type at the end
// of a possible chain of pointers. The returned depth is the number of
// pointers traversed: it is zero if the input type is not a pointer and
// positive otherwise. The returned type either the input type itself
// (if the input type is not a pointer type) or the value type obtained
// by traversing all the pointer types otherwise.
//
// Examples
// 	intType := reflect.TypeOf(int(0))
// 	dig(intType) -> (intType, 0)
//
// 	intPtrType := reflect.PtrTo(intType)
// 	dig(intPtrType) -> (intType, 1)
func dig(t reflect.Type) (ultimateType reflect.Type, depth int) {
	for t.Kind() == reflect.Ptr {
		depth++
		t = t.Elem()
	}
	ultimateType = t
	return
}

// fill traverses a chain of pointers, filling nil pointers with newly
// allocated values, and returns an addressable value representing the
// ultimate non-pointer value at the end of the chain. The input value
// MUST be addressable and MAY be a pointer. The input depth specifies
// the number of pointers in the chain, including the input value.
func fill(v reflect.Value, depth int) reflect.Value {
	for i := 0; i < depth; i++ {
		if v.IsNil() {
			p := reflect.New(v.Type().Elem())
			v.Set(p)
			v = p.Elem()
		} else {
			v = v.Elem()
		}
	}
	return v
}

type decodeState struct {
	rv   reflect.Value // Top-level target value
	data []Result      // Source slice of results
	i, j int           // Row and column currently being decoded
	dst  reflect.Value // Current destination value
}

func (s *decodeState) col() *ResultField {
	return &s.data[s.i][s.j]
}

func (s *decodeState) wrap(cause error) error {
	return &UnmarshalResultFieldValueError{
		ResultField: *s.col(),
		Cause:       cause,
		ResultIndex: s.i,
		FieldIndex:  s.j,
	}
}

type selectFunc func(reflect.Type) (decodeFunc, error)
type decodeFunc func(*decodeState) error

var (
	resultType          = reflect.TypeOf(Result{})
	byteSliceType       = reflect.TypeOf([]byte{})
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
)

func (s *decodeState) selRowDecodeFunc(rowType reflect.Type) (decodeFunc, error) {
	switch rowType.Kind() {
	case reflect.Interface:
		return s.selMapRowDecodeFunc(reflect.TypeOf(map[string]string{}))
	case reflect.Map:
		return s.selMapRowDecodeFunc(rowType)
	case reflect.Struct:
		return s.selStructRowDecodeFunc(rowType)
	case reflect.Slice:
		if rowType == resultType {
			return decodeRowByCopying, nil
		}
		fallthrough
	default:
		return nil, &InvalidUnmarshalError{Type: s.rv.Type(), RowType: rowType}
	}
}

func (s *decodeState) selMapRowDecodeFunc(mapRowType reflect.Type) (decodeFunc, error) {
	keyType := mapRowType.Key()
	if keyType.Kind() != reflect.String {
		return nil, &InvalidUnmarshalError{Type: s.rv.Type(), RowType: mapRowType}
	}
	immediateType := mapRowType.Elem()
	ultimateType, depth := dig(mapRowType.Elem())
	var f decodeFunc
	if reflect.PtrTo(ultimateType).Implements(textUnmarshalerType) {
		f = decodeColToTextUnmarshaler
	} else {
		switch ultimateType.Kind() {
		case reflect.String:
			f = decodeColToString
		case reflect.Interface:
			f = decodeColAsJSONFuzzy
		default:
			return nil, &InvalidUnmarshalError{Type: s.rv.Type(), RowType: mapRowType}
		}
	}
	return func(s *decodeState) error {
		dst := s.dst
		defer func() { s.dst = dst }()
		n := len(s.data[s.i])
		m := reflect.MakeMapWithSize(mapRowType, n)
		for s.j = 0; s.j < n; s.j++ {
			x := reflect.New(immediateType).Elem()
			s.dst = fill(x, depth)
			err := f(s)
			if err != nil {
				return err
			}
			m.SetMapIndex(reflect.ValueOf(s.col().Field), x)
		}
		dst.Set(m)
		s.dst = dst
		return nil
	}, nil
}

type decodableStructField struct {
	fieldIndex int
	depth      int
	decodeFunc decodeFunc
}

func (s *decodeState) selStructRowDecodeFunc(structRowType reflect.Type) (decodeFunc, error) {
	n := structRowType.NumField()
	dfs := make(map[string]decodableStructField, n)
	for i := 0; i < n; i++ {
		structField := structRowType.Field(i)
		field, depth, f, err := s.selStructRowColDecodeFunc(structRowType, &structField)
		if err != nil {
			return nil, err
		}
		if f != nil {
			dfs[field] = decodableStructField{
				fieldIndex: i,
				depth:      depth,
				decodeFunc: f,
			}
		}
	}
	return func(s *decodeState) error {
		dst := s.dst
		defer func() { s.dst = dst }()
		for s.j = 0; s.j < len(s.data[s.i]); s.j++ {
			col := s.col()
			if df, ok := dfs[col.Field]; ok {
				s.dst = fill(dst.Field(df.fieldIndex), df.depth)
				err := df.decodeFunc(s)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, nil
}

func (s *decodeState) selStructRowColDecodeFunc(structRowType reflect.Type, structField *reflect.StructField) (field string, depth int, f decodeFunc, err error) {
	var selector selectFunc = selStructRowColDecodeFuncByType

	tag := structField.Tag.Get("incite")
	if tag != "" {
		field = tag
		switch field {
		case "@timestamp", "@ingestionTime":
			selector = selStructRowColDecodeFuncForInsightsTime
		case "-":
			field = ""
			f = nil
			return
		}
	} else {
		tag = structField.Tag.Get("json")
		switch tag {
		case "":
			field = structField.Name
		case "-":
			field = ""
			f = nil
			return
		default:
			comma := strings.IndexByte(tag, ',')
			if comma < 0 {
				comma = len(tag)
			}
			field = tag[:comma]
			f = decodeColAsJSON
			return
		}
	}

	var valueType reflect.Type
	valueType, depth = dig(structField.Type)
	f, err = selector(valueType)
	if err != nil && tag == "" {
		// If the field is untagged and has a bad type, we just ignore it. This
		// allows users to unmarshal into structures that have irrelevant/
		// orthogonal fields.
		field = ""
		f = nil
		err = nil
	} else if err != nil {
		// If the field is tagged and has a bad type, it is an immediate error.
		err = &InvalidUnmarshalError{
			Type:      s.rv.Type(),
			RowType:   structRowType,
			Field:     field,
			FieldType: structField.Type,
			Message:   err.Error(),
		}
	}

	return
}

func selStructRowColDecodeFuncForInsightsTime(colType reflect.Type) (decodeFunc, error) {
	switch colType.Kind() {
	case reflect.String:
		return decodeColToString, nil
	case reflect.Struct:
		if colType == reflect.TypeOf(time.Time{}) {
			return decodeColAsInsightsTime, nil
		}
	}
	return nil, errors.New("timestamp result field does not target string or time.Time in struct")
}

func selStructRowColDecodeFuncByType(colType reflect.Type) (decodeFunc, error) {
	ptrType := reflect.PtrTo(colType)
	if ptrType.NumMethod() > 0 {
		if ptrType.Implements(textUnmarshalerType) {
			return decodeColToTextUnmarshaler, nil
		} else if ptrType.Implements(jsonUnmarshalerType) {
			return decodeColAsJSON, nil
		}
	}

	switch colType.Kind() {
	case reflect.String:
		return decodeColToString, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return decodeColToInt, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return decodeColToUint, nil
	case reflect.Float32, reflect.Float64:
		return decodeColToFloat, nil
	case reflect.Bool:
		return decodeColToBool, nil
	case reflect.Slice:
		if colType == byteSliceType {
			return decodeColToByteSlice, nil
		}
		fallthrough
	case reflect.Interface, reflect.Struct, reflect.Map, reflect.Array:
		return decodeColAsJSON, nil
	default:
		return nil, errors.New("unsupported struct field type")
	}
}

func decodeRowByCopying(s *decodeState) error {
	s.dst.Set(reflect.ValueOf(copyResult(s.data[s.i])))
	return nil
}

func decodeColToString(s *decodeState) error {
	s.dst.SetString(s.col().Value)
	return nil
}

func decodeColToByteSlice(s *decodeState) error {
	s.dst.SetBytes([]byte(s.col().Value))
	return nil
}

func decodeColToInt(s *decodeState) error {
	src := s.col().Value
	n, err := strconv.ParseInt(src, 10, 64)
	valueType := s.dst.Type()
	if err != nil {
		return s.wrap(err)
	} else if reflect.Zero(valueType).OverflowInt(n) {
		return s.wrap(overflow(reflect.ValueOf(n), valueType))
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToUint(s *decodeState) error {
	// Note that byte and uint8 have exactly the same type identity, so
	// a field of type byte will be decoded here.
	src := s.col().Value
	n, err := strconv.ParseUint(src, 10, 64)
	valueType := s.dst.Type()
	if err != nil {
		return s.wrap(err)
	} else if reflect.Zero(valueType).OverflowUint(n) {
		return s.wrap(overflow(reflect.ValueOf(n), valueType))
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToFloat(s *decodeState) error {
	src := s.col().Value
	n, err := strconv.ParseFloat(src, 64)
	valueType := s.dst.Type()
	if err != nil {
		return s.wrap(err)
	} else if reflect.Zero(valueType).OverflowFloat(n) {
		return s.wrap(overflow(reflect.ValueOf(n), valueType))
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToBool(s *decodeState) error {
	src := s.col().Value
	b, err := strconv.ParseBool(src)
	if err != nil {
		return s.wrap(err)
	}
	s.dst.SetBool(b)
	return nil
}

func decodeColToTextUnmarshaler(s *decodeState) error {
	ptrToDst := s.dst.Addr()
	ptrToInterface := ptrToDst.Interface()
	textUnmarshaler := ptrToInterface.(encoding.TextUnmarshaler)
	err := textUnmarshaler.UnmarshalText([]byte(s.col().Value))
	if err != nil {
		return s.wrap(err)
	}
	return nil
}

func decodeColAsJSON(s *decodeState) error {
	value := s.col().Value
	ptr := s.dst.Addr()
	i := ptr.Interface()
	err := json.Unmarshal([]byte(value), i)
	if err != nil {
		return s.wrap(err)
	}
	return nil
}

func decodeColAsJSONFuzzy(s *decodeState) error {
	src := s.col().Value
For:
	for _, c := range src {
		switch c {
		case '\t', '\n', '\r', ' ': // Might be JSON, keep skipping whitespace to find out.
			break
		case '{', '[', '"', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9': // Might be JSON, try to unpack it.
			var i interface{}
			err := json.Unmarshal([]byte(src), &i)
			if err != nil {
				break For
			}
			s.dst.Set(reflect.ValueOf(i))
			return nil
		default: // Definitely not JSON.
			break For
		}
	}
	s.dst.Set(reflect.ValueOf(src))
	return nil
}

func decodeColAsInsightsTime(s *decodeState) error {
	src := s.col().Value
	t, err := time.Parse(TimeLayout, src)
	if err != nil {
		return s.wrap(err)
	}
	s.dst.Set(reflect.ValueOf(t))
	return nil
}

func copyResult(r Result) Result {
	var r2 Result
	if r != nil {
		r2 = make([]ResultField, len(r))
		copy(r2, r)
	}
	return r2
}

// An InvalidUnmarshalError occurs when a value with an invalid type
// is passed to to Unmarshal.
type InvalidUnmarshalError struct {
	Type      reflect.Type
	RowType   reflect.Type
	Field     string
	FieldType reflect.Type
	Message   string
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "incite: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "incite: Unmarshal(non-pointer type: " + e.Type.String() + ")"
	}

	ultimateType, _ := dig(e.Type)
	switch ultimateType.Kind() {
	case reflect.Slice, reflect.Array, reflect.Interface:
		break
	default:
		return "incite: Unmarshal(pointer does not target a slice, array, or interface{}: " + e.Type.String() + ")"
	}

	switch e.RowType.Kind() {
	case reflect.Map:
		if e.RowType.Key().Kind() != reflect.String {
			return "incite: Unmarshal(map key type not string: " + e.Type.String() + ")"
		} else {
			return "incite: Unmarshal(map value type unsupported: " + e.Type.String() + ")"
		}
	case reflect.Slice:
		return "incite: Unmarshal(slice type is not " + resultType.String() + ": " + e.Type.String() + ")"
	}

	return fmt.Sprintf("incite: Unmarshal(struct field %s: %s)", e.Field, e.Message)
}

// An UnmarshalResultFieldValueError describes a failure to unmarshal a
// specific ResultField value within a specific Result.
type UnmarshalResultFieldValueError struct {
	ResultField
	Cause       error
	ResultIndex int
	FieldIndex  int
}

func (e *UnmarshalResultFieldValueError) Error() string {
	return fmt.Sprintf("incite: can't unmarshal field data[%d][%d] (name %s) value %q: %s",
		e.ResultIndex, e.FieldIndex, e.Field, e.Value, e.Cause.Error())
}

func overflow(v reflect.Value, t reflect.Type) error {
	return fmt.Errorf("%v overflows %s", v, t)
}
