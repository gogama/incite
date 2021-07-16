package incite_test

import (
	"fmt"

	"github.com/gogama/incite"
)

// TODO: More examples: Unmarshal struct with/without tags.
//                    : Timestamps.
//                    : Copy.
//                    : Show that pointers don't matter.
//
// https://blog.golang.org/examples

func ExampleUnmarshal_mapStringString() {
	data := []incite.Result{
		[]incite.ResultField{{Field: "@ptr", Value: "foo"}, {Field: "@message", Value: "bar"}},
	}
	var v []map[string]string
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(v)
	// Output: [map[@message:bar @ptr:foo]]
}

func ExampleUnmarshal_interface() {
	// An interface{} is treated as []map[string]string. The Object
	// key's value is not deserialized from JSON, it remains a string.
	data := []incite.Result{
		[]incite.ResultField{{Field: "@ptr", Value: `abc123`}, {Field: "Object", Value: `{"key":"value"}`}},
	}
	var v interface{}
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(v)
	// Output: [map[@ptr:abc123 Object:{"key":"value"}]]
}

func ExampleUnmarshal_mapStringInterface() {
	// As a special case, the data are unmarshalled fuzzily if the target
	// is a map[string]interface{}. If a value is valid JSON it is
	// unmarshalled as JSON, otherwise it is kept as a string. Here the
	// Object and QuotedString fields are contain valid JSON so they
	// unmarshal as a map and string, respectively. UnquotedString is
	// not valid JSON and stays as a string.
	data := []incite.Result{
		[]incite.ResultField{
			{Field: "Object", Value: `{"key":"value"}`},
			{Field: "QuotedString", Value: `"hello"`},
			{Field: "UnquotedString", Value: `world`},
		},
	}
	var v []map[string]interface{}
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(v)
	// Output: [map[Object:map[key:value] QuotedString:hello UnquotedString:world]]
}
