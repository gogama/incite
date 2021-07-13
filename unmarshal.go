package incite

import (
	"encoding"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
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
		return &InvalidUnmarshalError{TargetType: rv.Type()}
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

// =====================================================================
// BEGIN NEW SENSIBLE REFACTORED STUFF.
//     In here we will use the following naming conventions:
//
//        row = one Result
//        col = one ResultField
//        decodeFunc <TYPE> = a function to decode something
//        sel = prefix for a function that selects a function
//
// examples:
//        selRowDecodeFunc
//        selMapRowDecodeFunc
//        selStructRowDecodeFunc
//
//        selStructRowColDecodeFuncForInsightsTime
//        selStructRowColDecodeFuncByType
//
//        decodeInterfaceRow
//
//        decodeMapStrStrRow
//        decodeMapStrInterfaceRow
//        decodeMapStrTextUnmarshalerRow
//
//        decodeStructRow
//
//        decodeColAsJSON             - struct field tagged with "json"
//        decodeColAsJSONFuzzy        - target IN {  interface[}, map[string]interface{}; or an untagged struct field of type interface{}  }
//        decodeColAsInsightsTime
//
//        decodeColToString
//        decodeColToTextUnmarshaler
//        decodeColToInt, decodeColToUint, decodeColToFloat, decodeColToBool
//
// Essential playgrounds:
//        How pointers and chains of pointers Unmarshal: https://play.golang.org/p/d0jZKzJTl7r (any length of chain OK, intermediate pointers reused, this work is done by indirect function in unmarshal.go)
//            Indirect does it "dynamically" (on values, not types) because in general the structure is not known in advance.
//            We can do it "statically" (on types) because we don't need to look down below the top level structure.
//        Insights @timestamp format and why it doesn't work with time.Time and json packages: https://play.golang.org/p/USdNBPM-mVv
//        How tags work in reflection: https://play.golang.org/p/tt8t4zN8KWO
//        How to set a certain index of a slice to a non-nil map value: https://play.golang.org/p/R2nDxvnkFum
//        How addressability works in a bunch of circumstances: https://play.golang.org/p/tYXku9BFkWx
//            It is obviously correct when you think about it, but also a bit strange.
//
// For map stores, the process should be:
//     1. Get the value from the map.
//     2. If wasn't there, create a zero value.
//     3. Traverse the pointer chain, creating as necessary, until we
//        have the final addressable value.
//     4. DECODE INTO THAT VALUE.
//     5. Store that value into the map.
//
// For struct field stores, the process is similar but you start with
// the ValueOf a field that exists.
//
// Ultimately I want:
//
//      func dig(t reflect.Type) (depth int, reflect.Type)
//          Return final non-pointer value type and number of indirections.
// =====================================================================

type decodeState struct {
	rv   reflect.Value // Top-level target value
	data []Result      // Source slice of results
	i, j int           // Row and column currently being decoded
	dst  reflect.Value // Current destination value
}

func (s *decodeState) col() *ResultField {
	return &s.data[s.i][s.j]
}

type selectFunc func(reflect.Type) (decodeFunc, error)
type decodeFunc func(*decodeState) error

var (
	resultType          = reflect.TypeOf(Result{})
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
		return nil, &InvalidUnmarshalError{TargetType: s.rv.Type(), ElemType: rowType}
	}
}

func (s *decodeState) selMapRowDecodeFunc(mapRowType reflect.Type) (decodeFunc, error) {
	keyType := mapRowType.Key()
	if keyType.Kind() != reflect.String {
		return nil, &InvalidUnmarshalError{TargetType: s.rv.Type(), ElemType: mapRowType}
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
			return nil, &InvalidUnmarshalError{TargetType: s.rv.Type(), ElemType: mapRowType}
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
		field, depth, f, err := selStructRowColDecodeFunc(&structField)
		if err != nil {
			return nil, err // TODO: Return the correct InvalidUnmarshal type error.
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
					// TODO: return real error
					return err
				}
			}
		}
		return nil
	}, nil
}

func selStructRowColDecodeFunc(structField *reflect.StructField) (field string, depth int, f decodeFunc, err error) {
	var selector selectFunc = selStructRowColDecodeFuncByType

	tag := structField.Tag.Get("incite")
	if tag != "" {
		field = tag
		switch field {
		case "@timestamp", "@ingestionTime":
			selector = selStructRowColDecodeFuncForInsightsTime
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
	if err != nil {
		// Convert err into the correct desired error.
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
	return nil, &UnmarshalResultTypeError{
		// TODO: Properly implement this.
	}
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
	case reflect.Interface, reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
		return decodeColAsJSON, nil
	default:
		return nil, errors.New("TODO: put a real error here.")
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

func decodeColToInt(s *decodeState) error {
	src := s.col().Value
	n, err := strconv.ParseInt(src, 10, 64)
	valueType := s.dst.Type()
	if err != nil || reflect.Zero(valueType).OverflowInt(n) {
		return errors.New("TODO: put error") // error
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToUint(s *decodeState) error {
	src := s.col().Value
	n, err := strconv.ParseUint(src, 10, 64)
	valueType := s.dst.Type()
	if err != nil || reflect.Zero(valueType).OverflowUint(n) {
		return errors.New("TODO: put error") // error
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToFloat(s *decodeState) error {
	src := s.col().Value
	n, err := strconv.ParseFloat(src, 64)
	valueType := s.dst.Type()
	if err != nil || reflect.Zero(valueType).OverflowFloat(n) {
		return errors.New("TODO: put error") // error
	}
	s.dst.Set(reflect.ValueOf(n).Convert(valueType))
	return nil
}

func decodeColToBool(s *decodeState) error {
	src := s.col().Value
	b, err := strconv.ParseBool(src)
	if err != nil {
		return errors.New("TODO: put error")
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
		// TODO: wrap it
		return err
	}
	return nil
}

func decodeColAsJSON(s *decodeState) error {
	value := s.col().Value
	ptr := s.dst.Addr()
	i := ptr.Interface()
	return json.Unmarshal([]byte(value), i)
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
	t, err := time.Parse("TODO: put layout here", src)
	if err != nil {
		return &UnmarshalResultTypeError{
			// TODO properly implement this
		}
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
