package incite_test

import (
	"fmt"
	"time"

	"github.com/gogama/incite"
)

func ExampleUnmarshal_mapStringString() {
	data := []incite.Result{
		{{"@ptr", "foo"}, {"@message", "bar"}},
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
		{{"@ptr", "abc123"}, {"Object", `{"key":"value"}`}},
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
		{
			{"Object", `{"key":"value"}`},
			{"QuotedString", `"hello"`},
			{"UnquotedString", `world`},
		},
	}
	var v []map[string]interface{}
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(v)
	// Output: [map[Object:map[key:value] QuotedString:hello UnquotedString:world]]
}

func ExampleUnmarshal_struct() {
	data := []incite.Result{
		{
			{"@ptr", "row1"}, {"@timestamp", "2021-07-17 01:00:01.012"},
			{"@message", `{}`}, {"DiscoveredField", "1234.5"},
		},
		{
			{"@ptr", "row2"}, {"@timestamp", "2021-07-17 01:00:03.999"},
			{"@message", `{"foo":"bar","ham":"eggs"}`},
		},
	}
	var v []struct {
		Timestamp       time.Time              `incite:"@timestamp"`
		Message         map[string]interface{} `json:"@message"`
		DiscoveredField float64
		// Many other mappings are possible. See Unmarshal documentation
		// for details.
	}
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(v)
	// Output: [{2021-07-17 01:00:01.012 +0000 UTC map[] 1234.5} {2021-07-17 01:00:03.999 +0000 UTC map[foo:bar ham:eggs] 0}]
}

func ExampleUnmarshal_pointer() {
	// Pointers are followed in the intuitive manner.
	data := []incite.Result{{{"hello", "world"}}}
	var v *[]map[string]**string
	_ = incite.Unmarshal(data, &v) // Error ignored for simplicity.
	fmt.Println(**(*v)[0]["hello"])
	// Output: world
}