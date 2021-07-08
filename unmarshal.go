package incite

import "reflect"

// Unmarshal converts the CloudWatch Logs Insights result data and stores the
// result in the value pointed to by v. If v is nil or nor a pointer, Unmarshal
// returns InvalidUnmarshalError.
//
// TODO: Finish documentation here.
func Unmarshal(data []Result, v interface{}) error {
	// TODO: v must be a pointer-to-a-slice-of-structs.

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}
	//	if rv.
	//rv.Fi
	/*
		data is like a JSON string where the top level item is an array of objects.

		e.g.:

		[]map[string]interface{}{
			{
				{
					"@ptr": "foo",
					"ComplexValue": "<json>"
				}
			}
		}

		Concept: For each result in `data`:
			For each K/V in the result.
				For each tag:
					If the tag matches the key:
						Deserialize the value into the key.

		Value mappings:
			string - Any string value
			integer type - Try to deserialize the string
			floating point type - Try to deserialize the string
			slice, map, struct type - Try to JSON decode the value within the thing.

			??? byte[] - Any string value

	*/
	return nil
}

type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "incite: Unmarshal(nil)"
	}
	if e.Type.Kind() != reflect.Ptr {
		return "incite: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "incite: Unmarshal(nil " + e.Type.String() + ")"
}
