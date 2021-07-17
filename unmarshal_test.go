package incite

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshal(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		testCases := []struct {
			name string
			data []Result
			v, w interface{}
		}{
			{
				name: "interface{},data:nil",
				v:    ip(nil),
				w:    ip([]map[string]string{}),
			},
			{
				name: "interface{},data:empty",
				data: []Result{},
				v:    ip([]map[string]interface{}{}),
				w:    ip([]map[string]string{}),
			},
			{
				name: "interface{},data:single",
				data: single,
				v:    ip("irrelevant value"),
				w: ip([]map[string]string{
					{
						"@ptr": "foo",
					},
				}),
			},

			{
				name: "[]Result,data:nil",
				v:    &[]Result{},
				w:    &[]Result{},
			},
			{
				name: "[]Result,data:empty",
				data: []Result{},
				v:    &[]Result{},
				w:    &[]Result{},
			},
			{
				name: "[]Result,data:single",
				data: single,
				v: &[]Result{
					{},
					{},
				},
				w: &single,
			},
			{
				name: "[]Result,data:multiple",
				data: []Result{
					{},
					r("thomas", "gray"),
					r("elegy", "written", "in", "a", "country", "churchyard"),
				},
				v: &[]Result{},
				w: &[]Result{
					{},
					r("thomas", "gray"),
					r("elegy", "written", "in", "a", "country", "churchyard"),
				},
			},

			{
				name: "[]*Result,data:nil",
				v:    &[]*Result{},
				w:    &[]*Result{},
			},
			{
				name: "[]*Result,data:empty",
				data: []Result{},
				v: &[]*Result{
					rp(r("extra", "to", "be", "removed")),
				},
				w: &[]*Result{},
			},
			{
				name: "[]*Result,data:single",
				data: single,
				v:    &[]*Result{},
				w: &[]*Result{
					&single[0],
				},
			},

			{
				name: "[]**Result,data:nil",
				v:    &[]**Result{},
				w:    &[]**Result{},
			},
			{
				name: "[]**Result,data:empty",
				data: []Result{},
				v:    &[]**Result{},
				w:    &[]**Result{},
			},
			{
				name: "[]**Result,data:single",
				data: single,
				v:    &[]**Result{},
				w: &[]**Result{
					rpp(single[0]),
				},
			},

			{
				name: "[]interface{},data:nil",
				v:    &[]interface{}{},
				w:    &[]interface{}{},
			},
			{
				name: "[]interface{},data:empty",
				data: []Result{},
				v:    &[]interface{}{},
				w:    &[]interface{}{},
			},
			{
				name: "[]interface{},data:single",
				data: single,
				v:    &[]interface{}{},
				w: &[]interface{}{
					map[string]string{
						"@ptr": "foo",
					},
				},
			},

			{
				name: "[]*interface{},data:nil",
				v:    &[]*interface{}{},
				w:    &[]*interface{}{},
			},
			{
				name: "[]*interface{},data:empty",
				data: []Result{},
				v:    &[]*interface{}{},
				w:    &[]*interface{}{},
			},
			{
				name: "[]interface{},data:single",
				data: single,
				v:    &[]*interface{}{},
				w: &[]*interface{}{
					ip(map[string]string{
						"@ptr": "foo",
					}),
				},
			},

			{
				name: "[]map[interface{}],data:nil",
				v:    &[]map[string]interface{}{},
				w:    &[]map[string]interface{}{},
			},
			{
				name: "[]map[interface{}],data:empty",
				data: []Result{},
				v:    &[]map[string]interface{}{},
				w:    &[]map[string]interface{}{},
			},
			{
				name: "[]map[interface{}],data:single",
				data: single,
				v:    &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"@ptr": "foo",
					},
				},
			},
			{
				name: "[]map[interface{}],data:multiple",
				data: []Result{
					r(
						"@ptr", "bar", "@message", "bar message", "@timestamp", "",
						"DiscoveredKey", "DiscoveredStringValue", "DiscoveredKey2", "-123"),
					r(
						"@ptr", "baz", "@timestamp", "2021-06-19 03:59:59.936",
						"DiscoveredKey", `{"k":"string","k2":1,"k3":["another string",null,10]}`,
						"@message", "baz message", "DiscoveredKey2", "1.5"),
				},
				v: &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"@ptr":           "bar",
						"@message":       "bar message",
						"@timestamp":     "",
						"DiscoveredKey":  "DiscoveredStringValue",
						"DiscoveredKey2": -123.0,
					},
					{
						"@ptr":       "baz",
						"@message":   "baz message",
						"@timestamp": "2021-06-19 03:59:59.936",
						"DiscoveredKey": map[string]interface{}{
							"k":  "string",
							"k2": 1.0,
							"k3": []interface{}{"another string", nil, 10.0},
						},
						"DiscoveredKey2": 1.5,
					},
				},
			},

			{
				name: "[]map[*interface{}],data:nil",
				v:    &[]map[string]*interface{}{},
				w:    &[]map[string]*interface{}{},
			},
			{
				name: "[]map[*interface{}],data:empty",
				data: []Result{},
				v:    &[]map[string]*interface{}{},
				w:    &[]map[string]*interface{}{},
			},
			{
				name: "[]map[*interface{}],data:multiple",
				data: []Result{r("@ptr", "bar", "Discovered1Key", "Discovered1Value")},
				v: &[]map[string]*interface{}{
					{
						"Discovered1Key": ip("baz"),
					},
				},
				w: &[]map[string]*interface{}{
					{
						"@ptr":           ip("bar"),
						"Discovered1Key": ip("Discovered1Value"),
					},
				},
			},

			{
				name: "[]map[*interface{}],data:nil",
				v:    &[]map[string]**interface{}{},
				w:    &[]map[string]**interface{}{},
			},
			{
				name: "[]map[*interface{}],data:empty",
				data: []Result{},
				v:    &[]map[string]**interface{}{},
				w:    &[]map[string]**interface{}{},
			},
			{
				name: "[]map[*interface{}],data:single",
				data: []Result{r("@ptr", "bar", "Discovered1Key", "Discovered1Value")},
				v: &[]map[string]**interface{}{
					{
						"Discovered1Key": ipp("baz"),
					},
				},
				w: &[]map[string]**interface{}{
					{
						"@ptr":           ipp("bar"),
						"Discovered1Key": ipp("Discovered1Value"),
					},
				},
			},

			{
				name: "[]map[string],data:nil",
				v:    &[]map[string]string{},
				w:    &[]map[string]string{},
			},
			{
				name: "[]map[string],data:empty",
				data: []Result{},
				v:    &[]map[string]string{},
				w:    &[]map[string]string{},
			},
			{
				name: "[]map[string],data:single",
				data: single,
				v: &[]map[string]string{
					{
						"this": "will be deleted",
						"and":  "this will too",
					},
					{
						"this": "whole slice element will go away",
					},
				},
				w: &[]map[string]string{
					{
						"@ptr": "foo",
					},
				},
			},
			{
				name: "[]map[string],data:multiple",
				data: []Result{r("@message", `["world"]`, "@ptr", "hello", "DiscoveredKey", "10")},
				v:    &[]map[string]string{},
				w: &[]map[string]string{
					{
						"@ptr":          "hello",
						"@message":      `["world"]`,
						"DiscoveredKey": "10",
					},
				},
			},
			{
				name: "[]map[string],data:multiple",
				data: []Result{
					r("@message", `["world"]`, "@ptr", "hello", "DiscoveredKey", "10"),
					r("@log", "111100001111:/some/log", "@logStream", "fizzle-fizzle",
						"@ingestionTime", "2021-06-19 03:59:59.936", "DiscoveredKey", "null",
						"@ptr", "Bonjour!"),
				},
				v: &[]map[string]string{},
				w: &[]map[string]string{
					{
						"@ptr":          "hello",
						"@message":      `["world"]`,
						"DiscoveredKey": "10",
					},
					{
						"@ptr":           "Bonjour!",
						"@log":           "111100001111:/some/log",
						"@logStream":     "fizzle-fizzle",
						"@ingestionTime": "2021-06-19 03:59:59.936",
						"DiscoveredKey":  "null",
					},
				},
			},

			{
				name: "[]map[*string],data:nil",
				v:    &[]map[string]*string{},
				w:    &[]map[string]*string{},
			},
			{
				name: "[]map[*string],data:empty",
				data: []Result{},
				v:    &[]map[string]*string{},
				w:    &[]map[string]*string{},
			},
			{
				name: "[]map[*string],data:multiple",
				data: []Result{r("@message", `["world"]`, "@ptr", "hello", "DiscoveredKey", "10")},
				v: &[]map[string]*string{
					{
						"@message": nil,
					},
				},
				w: &[]map[string]*string{
					{
						"@ptr":          sp("hello"),
						"@message":      sp(`["world"]`),
						"DiscoveredKey": sp("10"),
					},
				},
			},

			{
				name: "[]map[**string],data:nil",
				v:    &[]map[string]**string{},
				w:    &[]map[string]**string{},
			},
			{
				name: "[]map[**string],data:empty",
				data: []Result{},
				v:    &[]map[string]**string{},
				w:    &[]map[string]**string{},
			},
			{
				name: "[]map[**string],data:multiple",
				data: []Result{r("@message", `["world"]`, "@ptr", "hello", "DiscoveredKey", "10")},
				v: &[]map[string]**string{
					{
						"@ptr":     nil,
						"@message": spp("forgotten value"),
					},
				},
				w: &[]map[string]**string{
					{
						"@ptr":          spp("hello"),
						"@message":      spp(`["world"]`),
						"DiscoveredKey": spp("10"),
					},
				},
			},

			{
				name: "[]map[directDummyTextUnmarshaler],data:nil",
				v:    &[]map[string]directDummyTextUnmarshaler{},
				w:    &[]map[string]directDummyTextUnmarshaler{},
			},
			{
				name: "[]map[directDummyTextUnmarshaler],data:empty",
				data: []Result{},
				v:    &[]map[string]directDummyTextUnmarshaler{},
				w:    &[]map[string]directDummyTextUnmarshaler{},
			},
			{
				name: "[]map[directDummyTextUnmarshaler],data:single",
				data: single,
				v:    &[]map[string]directDummyTextUnmarshaler{},
				w: &[]map[string]directDummyTextUnmarshaler{
					{
						"@ptr": nil,
					},
				},
			},

			{
				name: "[]map[*directDummyTextUnmarshaler],data:nil",
				v:    &[]map[string]*directDummyTextUnmarshaler{},
				w:    &[]map[string]*directDummyTextUnmarshaler{},
			},
			{
				name: "[]map[*directDummyTextUnmarshaler],data:empty",
				data: []Result{},
				v:    &[]map[string]*directDummyTextUnmarshaler{},
				w:    &[]map[string]*directDummyTextUnmarshaler{},
			},
			{
				name: "[]map[*directDummyTextUnmarshaler],data:single",
				data: single,
				v:    &[]map[string]*directDummyTextUnmarshaler{},
				w: &[]map[string]*directDummyTextUnmarshaler{
					{
						"@ptr": new(directDummyTextUnmarshaler),
					},
				},
			},

			{
				name: "[]map[indirectDummyTextUnmarshaler],data:nil",
				v:    &[]map[string]indirectDummyTextUnmarshaler{},
				w:    &[]map[string]indirectDummyTextUnmarshaler{},
			},
			{
				name: "[]map[indirectDummyTextUnmarshaler],data:empty",
				data: []Result{},
				v:    &[]map[string]indirectDummyTextUnmarshaler{},
				w:    &[]map[string]indirectDummyTextUnmarshaler{},
			},
			{
				name: "[]map[indirectDummyTextUnmarshaler],data:single",
				data: single,
				v:    &[]map[string]indirectDummyTextUnmarshaler{},
				w: &[]map[string]indirectDummyTextUnmarshaler{
					{
						"@ptr": indirectDummyTextUnmarshaler{"foo"},
					},
				},
			},

			{
				name: "[]map[*indirectDummyTextUnmarshaler],data:nil",
				v:    &[]map[string]*indirectDummyTextUnmarshaler{},
				w:    &[]map[string]*indirectDummyTextUnmarshaler{},
			},
			{
				name: "[]map[*indirectDummyTextUnmarshaler],data:empty",
				data: []Result{},
				v:    &[]map[string]*indirectDummyTextUnmarshaler{},
				w:    &[]map[string]*indirectDummyTextUnmarshaler{},
			},
			{
				name: "[]map[*indirectDummyTextUnmarshaler],data:single",
				data: single,
				v:    &[]map[string]*indirectDummyTextUnmarshaler{},
				w: &[]map[string]*indirectDummyTextUnmarshaler{
					{
						"@ptr": &indirectDummyTextUnmarshaler{"foo"},
					},
				},
			},

			{
				name: "[]struct{},data:nil",
				v:    &[]struct{}{},
				w:    &[]struct{}{},
			},
			{
				name: "[]struct{},data:empty",
				data: []Result{},
				v:    &[]struct{}{},
				w:    &[]struct{}{},
			},
			{
				name: "[]struct{},data:single",
				data: single,
				v:    &[]struct{}{},
				w:    &[]struct{}{{}},
			},
			{
				name: "[]struct{},data:multiple",
				data: []Result{
					{
						{
							Field: "foo",
							Value: "bar",
						},
					},
					{
						{
							Field: "hello",
							Value: "world",
						},
					},
				},
				v: &[]struct{}{},
				w: &[]struct{}{{}, {}},
			},

			{
				name: "[]*struct{},data:nil",
				v:    &[]*struct{}{},
				w:    &[]*struct{}{},
			},
			{
				name: "[]*struct{},data:empty",
				data: []Result{},
				v:    &[]*struct{}{},
				w:    &[]*struct{}{},
			},
			{
				name: "[]*struct{},data:single",
				data: single,
				v:    &[]*struct{}{},
				w:    &[]*struct{}{{}},
			},
			{
				name: "[]*struct{},data:multiple",
				data: []Result{
					{
						{
							Field: "foo",
							Value: "bar",
						},
					},
					{
						{
							Field: "hello",
							Value: "world",
						},
					},
				},
				v: &[]*struct{}{},
				w: &[]*struct{}{{}, {}},
			},

			{
				name: "[]strongestTypedStruct,data:nil",
				v:    &[]strongestTypedStruct{},
				w:    &[]strongestTypedStruct{},
			},
			{
				name: "[]strongestTypedStruct{},data:empty",
				data: []Result{},
				v:    &[]strongestTypedStruct{},
				w:    &[]strongestTypedStruct{},
			},
			{
				name: "[]strongestTypedStruct{},data:single",
				data: single,
				v:    &[]strongestTypedStruct{},
				w:    &[]strongestTypedStruct{{Ptr: "foo"}},
			},
			{
				name: "[]strongestTypedStruct{},data:multiple",
				data: []Result{
					{
						{"@ptr", "first row"},
						{"@timestamp", "2021-07-17 00:28:40.123"},
						{"@ingestionTime", "2021-07-17 00:29:01.500"},
						{"@deleted", "true"},
						{"ObjectOfJSON", `{"key":"value"}`},
						{"ArrayOfJSON", `["first","second"]`},
						{"StringOfJSON", `"str"`},
						{"NumberOfJSON", `-99.9`},
						{"BoolOfJSON", `true`},
						{"TaggedText", "tagged text"},
						{"Text2", "untagged text"},
						{"Int", "-999"},
						{"Int8", "-128"},
						{"Int16", "32767"},
						{"Int32", "100"},
						{"Int64", "-200"},
						{"Uint", "999"},
						{"Uint8", "250"},
						{"Uint16", "65000"},
						{"Uint32", "500"},
						{"Uint64", "600"},
						{"Float32", "-100"},
						{"Float64", "-200"},
						{"Bool", "1"},
						{"TaggedString", "tagged string"},
						{"String2", "untagged string"},
						{"ignored field 1", "ignored value 1"},
					},
					{
						{"@timestamp", "2021-07-17 00:39:00.000"},
						{"@ptr", "second row"},
						{"@deleted", "false"},
						{"ObjectOfJSON", "{}"},
						{"Uint64", "111"},
						{"Strings", `["a","bb","CCC"]`},
					},
				},
				v: &[]strongestTypedStruct{},
				w: &[]strongestTypedStruct{
					{
						Ptr:           "first row",
						Timestamp:     time.Date(2021, 7, 17, 0, 28, 40, 123000000, time.UTC),
						IngestionTime: tmp(time.Date(2021, 7, 17, 0, 29, 1, 500000000, time.UTC)),
						Deleted:       true,
						JSONObject:    map[string]interface{}{"key": "value"},
						JSONArray:     []interface{}{"first", "second"},
						JSONString:    "str",
						JSONNumber:    -99.9,
						JSONBool:      true,
						Text1:         indirectDummyTextUnmarshaler{"tagged text"},
						Text2:         indirectDummyTextUnmarshaler{"untagged text"},
						Int:           -999,
						Int8:          -128,
						Int16:         32767,
						Int32:         100,
						Int64:         -200,
						Uint:          999,
						Uint8:         250,
						Uint16:        65000,
						Uint32:        500,
						Uint64:        600,
						Float32:       -100,
						Float64:       -200,
						Bool:          true,
						String1:       "tagged string",
						String2:       "untagged string",
					},
					{
						Ptr:        "second row",
						Timestamp:  time.Date(2021, 7, 17, 0, 39, 0, 0, time.UTC),
						JSONObject: map[string]interface{}{},
						Uint64:     111,
						Strings:    []string{"a", "bb", "CCC"},
					},
				},
			},

			{
				name: "[]*strongestTypedStruct,data:nil",
				v:    &[]*strongestTypedStruct{},
				w:    &[]*strongestTypedStruct{},
			},
			{
				name: "[]*strongestTypedStruct{},data:empty",
				data: []Result{},
				v:    &[]*strongestTypedStruct{},
				w:    &[]*strongestTypedStruct{},
			},
			{
				name: "[]*strongestTypedStruct{},data:single",
				data: single,
				v:    &[]*strongestTypedStruct{},
				w:    &[]*strongestTypedStruct{{Ptr: "foo"}},
			},
			{
				name: "[]*strongestTypedStruct{},data:multiple",
				data: []Result{
					{
						{"@ptr", "first row"},
						{"@timestamp", "2021-07-17 00:28:40.123"},
						{"@ingestionTime", "2021-07-17 00:29:01.500"},
						{"@deleted", "true"},
						{"ObjectOfJSON", `{"key":"value"}`},
						{"ArrayOfJSON", `["first","second"]`},
						{"StringOfJSON", `"str"`},
						{"NumberOfJSON", `-99.9`},
						{"BoolOfJSON", `true`},
						{"TaggedText", "tagged text"},
						{"Text2", "untagged text"},
						{"Int", "-999"},
						{"Int8", "-128"},
						{"Int16", "32767"},
						{"Int32", "100"},
						{"Int64", "-200"},
						{"Uint", "999"},
						{"Uint8", "250"},
						{"Uint16", "65000"},
						{"Uint32", "500"},
						{"Uint64", "600"},
						{"Float32", "-100"},
						{"Float64", "-200"},
						{"Bool", "1"},
						{"TaggedString", "tagged string"},
						{"String2", "untagged string"},
						{"ignored field 1", "ignored value 1"},
					},
					{
						{"@timestamp", "2021-07-17 00:39:00.000"},
						{"@ptr", "second row"},
						{"@deleted", "false"},
						{"ObjectOfJSON", "{}"},
						{"Uint64", "111"},
						{"Strings", `["a","bb","CCC"]`},
					},
				},
				v: &[]*strongestTypedStruct{},
				w: &[]*strongestTypedStruct{
					{
						Ptr:           "first row",
						Timestamp:     time.Date(2021, 7, 17, 0, 28, 40, 123000000, time.UTC),
						IngestionTime: tmp(time.Date(2021, 7, 17, 0, 29, 1, 500000000, time.UTC)),
						Deleted:       true,
						JSONObject:    map[string]interface{}{"key": "value"},
						JSONArray:     []interface{}{"first", "second"},
						JSONString:    "str",
						JSONNumber:    -99.9,
						JSONBool:      true,
						Text1:         indirectDummyTextUnmarshaler{"tagged text"},
						Text2:         indirectDummyTextUnmarshaler{"untagged text"},
						Int:           -999,
						Int8:          -128,
						Int16:         32767,
						Int32:         100,
						Int64:         -200,
						Uint:          999,
						Uint8:         250,
						Uint16:        65000,
						Uint32:        500,
						Uint64:        600,
						Float32:       -100,
						Float64:       -200,
						Bool:          true,
						String1:       "tagged string",
						String2:       "untagged string",
					},
					{
						Ptr:        "second row",
						Timestamp:  time.Date(2021, 7, 17, 0, 39, 0, 0, time.UTC),
						JSONObject: map[string]interface{}{},
						Uint64:     111,
						Strings:    []string{"a", "bb", "CCC"},
					},
				},
			},
		}

		// TODO: Data longer than input slice case.
		// TODO: Data shorter than input slice case.
		// TODO: Array cases.
		// TODO: Struct cases ensuring irrelevant/untagged fields are ignored even if wrong type.
		// TODO: Keep the []byte case or not? And should it be base64 decoding or what?
		// TODO: Handling for "-" and "-," tags.

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				err := Unmarshal(testCase.data, testCase.v)

				require.NoError(t, err)
				assert.Equal(t, testCase.w, testCase.v)
			})
		}
	})
	t.Run("Error", func(t *testing.T) {
		testCases := []struct {
			name string
			data []Result
			v    interface{}
			err  error
		}{
			{
				name: "InvalidUnmarshalError(nil)",
				err:  &InvalidUnmarshalError{},
			},
			{
				name: "InvalidUnmarshalError(Non-Pointer)",
				v:    0,
				err: &InvalidUnmarshalError{
					Type: reflect.TypeOf(0),
				},
			},
			{
				name: "InvalidUnmarshalError(Invalid Type, Depth 1)",
				v:    sp("foo"),
				err: &InvalidUnmarshalError{
					Type: reflect.PtrTo(reflect.TypeOf("")),
				},
			},
			{
				name: "InvalidUnmarshalError(Invalid Type, Depth 2)",
				v:    spp("foo"),
				err: &InvalidUnmarshalError{
					Type: reflect.PtrTo(reflect.PtrTo(reflect.TypeOf(""))),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Key, Depth 0)",
				v:    &[]map[int]string{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]map[int]string{})),
					RowType: reflect.TypeOf(map[int]string{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Key, Depth 1)",
				v:    &[]*map[int]string{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]*map[int]string{})),
					RowType: reflect.TypeOf(map[int]string{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Key, Depth 2)",
				v:    &[]**map[int]string{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]**map[int]string{})),
					RowType: reflect.TypeOf(map[int]string{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 0.0)",
				v:    &[]map[string]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]map[string]int{})),
					RowType: reflect.TypeOf(map[string]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 0.1)",
				v:    &[]map[string]*int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]map[string]*int{})),
					RowType: reflect.TypeOf(map[string]*int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 0.2)",
				v:    &[]map[string]**int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]map[string]**int{})),
					RowType: reflect.TypeOf(map[string]**int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 1.0)",
				v:    &[]*map[string]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]*map[string]int{})),
					RowType: reflect.TypeOf(map[string]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 1.1)",
				v:    &[]*map[string]*int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]*map[string]*int{})),
					RowType: reflect.TypeOf(map[string]*int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 2.0)",
				v:    &[]**map[string]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]**map[string]int{})),
					RowType: reflect.TypeOf(map[string]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Value, Depth 2.2)",
				v:    &[]**map[string]**int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]**map[string]**int{})),
					RowType: reflect.TypeOf(map[string]**int{}),
				},
			},
			{
				name: "Bad Slice Value, Depth 0",
				v:    &[][]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([][]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "Bad Slice Value, Depth 1",
				v:    &[]*[]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]*[]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "Bad Slice Value, Depth 2",
				v:    &[]**[]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]**[]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "Bad Struct Field: @timestamp",
				v:    &[]badStructTimestamp{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructTimestamp{})),
					RowType:   reflect.TypeOf(badStructTimestamp{}),
					Field:     "@timestamp",
					FieldType: reflect.TypeOf(0),
					Message:   "timestamp result field does not target time.Time or string in struct",
				},
			},
			{
				name: "Bad Struct Field: @ingestionTime",
				v:    &[]badStructIngestionTime{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructIngestionTime{})),
					RowType:   reflect.TypeOf(badStructIngestionTime{}),
					Field:     "@ingestionTime",
					FieldType: reflect.TypeOf(false),
					Message:   "timestamp result field does not target time.Time or string in struct",
				},
			},
			{
				name: "Bad Struct Field: @deleted",
				v:    &[]badStructDeleted{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructDeleted{})),
					RowType:   reflect.TypeOf(badStructDeleted{}),
					Field:     "@deleted",
					FieldType: reflect.TypeOf([]int{}),
					Message:   "deleted flag field does not target bool or string in struct",
				},
			},
			{
				name: "Bad Struct Field: chan, Tagged",
				v:    &[]badStructChanTagged{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructChanTagged{})),
					RowType:   reflect.TypeOf(badStructChanTagged{}),
					Field:     "MyField",
					FieldType: reflect.TypeOf(make(chan struct{})),
					Message:   "unsupported struct field type",
				},
			},
			{
				name: "Bad Struct Field: func, Tagged",
				v:    &[]badStructFuncTagged{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructFuncTagged{})),
					RowType:   reflect.TypeOf(badStructFuncTagged{}),
					Field:     "MyFunc",
					FieldType: reflect.TypeOf(func() {}),
					Message:   "unsupported struct field type",
				},
			},
			{
				name: "Bad Struct Field: complex64, Tagged",
				v:    &[]badStructComplex64Tagged{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructComplex64Tagged{})),
					RowType:   reflect.TypeOf(badStructComplex64Tagged{}),
					Field:     "MyComplex",
					FieldType: reflect.TypeOf(complex64(0)),
					Message:   "unsupported struct field type",
				},
			},
			// notPointer[...various...]
			// notSlice[...various...]
			// notStruct[...various...]
			// TODO: Can't target the same ResultField twice with two different struct fields???
			// TODO: Numeric overflow cases?
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				err := Unmarshal(testCase.data, testCase.v)

				assert.Error(t, err)
				assert.Equal(t, testCase.err, err)
			})
		}
	})
}

func TestInvalidUnmarshalError_Error(t *testing.T) {
	testCases := []struct {
		name     string
		err      InvalidUnmarshalError
		expected string
	}{
		{
			name:     "nil",
			expected: "incite: Unmarshal(nil)",
		},
		{
			name: "Non-Pointer",
			err: InvalidUnmarshalError{
				Type: reflect.TypeOf(0),
			},
			expected: "incite: Unmarshal(non-pointer type: int)",
		},
		{
			name: "Invalid Type, Depth 1",
			err: InvalidUnmarshalError{
				Type: reflect.PtrTo(reflect.TypeOf(0)),
			},
			expected: "incite: Unmarshal(pointer does not target a slice, array, or interface{}: *int)",
		},
		{
			name: "Invalid Type, Depth 2",
			err: InvalidUnmarshalError{
				Type: reflect.PtrTo(reflect.PtrTo(reflect.TypeOf(0))),
			},
			expected: "incite: Unmarshal(pointer does not target a slice, array, or interface{}: **int)",
		},
		{
			name: "Bad Map Key, Depth 0",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]map[int]string{})),
				RowType: reflect.TypeOf(map[int]string{}),
			},
			expected: "incite: Unmarshal(map key type not string: *[]map[int]string)",
		},
		{
			name: "Bad Map Key, Depth 1",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]*map[int]string{})),
				RowType: reflect.TypeOf(map[int]string{}),
			},
			expected: "incite: Unmarshal(map key type not string: *[]*map[int]string)",
		},
		{
			name: "Bad Map Key, Depth 2",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]**map[int]string{})),
				RowType: reflect.TypeOf(map[int]string{}),
			},
			expected: "incite: Unmarshal(map key type not string: *[]**map[int]string)",
		},
		{
			name: "Bad Map Value, Depth 0",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]map[string]int{})),
				RowType: reflect.TypeOf(map[string]int{}),
			},
			expected: "incite: Unmarshal(map value type unsupported: *[]map[string]int)",
		},
		{
			name: "Bad Map Value, Depth 1",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]*map[string]int{})),
				RowType: reflect.TypeOf(map[string]int{}),
			},
			expected: "incite: Unmarshal(map value type unsupported: *[]*map[string]int)",
		},
		{
			name: "Bad Map Value, Depth 2",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]**map[string]int{})),
				RowType: reflect.TypeOf(map[string]int{}),
			},
			expected: "incite: Unmarshal(map value type unsupported: *[]**map[string]int)",
		},
		{
			name: "Bad Slice Value, Depth 0",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([][]int{})),
				RowType: reflect.TypeOf([]int{}),
			},
			expected: "incite: Unmarshal(slice type is not incite.Result: *[][]int)",
		},
		{
			name: "Bad Slice Value, Depth 1",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]*[]int{})),
				RowType: reflect.TypeOf([]int{}),
			},
			expected: "incite: Unmarshal(slice type is not incite.Result: *[]*[]int)",
		},
		{
			name: "Bad Slice Value, Depth 2",
			err: InvalidUnmarshalError{
				Type:    reflect.PtrTo(reflect.TypeOf([]**[]int{})),
				RowType: reflect.TypeOf([]int{}),
			},
			expected: "incite: Unmarshal(slice type is not incite.Result: *[]**[]int)",
		},
		{
			name: "Invalid Struct Field",
			err: InvalidUnmarshalError{
				Type:      reflect.PtrTo(reflect.TypeOf([]badStructTimestamp{})),
				RowType:   reflect.TypeOf(badStructTimestamp{}),
				Field:     "@timestamp",
				FieldType: reflect.TypeOf(0),
				Message:   "foo",
			},
			expected: "incite: Unmarshal(struct field @timestamp: foo)",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := testCase.err.Error()

			assert.Equal(t, testCase.expected, actual)
		})
	}
}

type badStructTimestamp struct {
	TS int `incite:"@timestamp"`
}

type badStructIngestionTime struct {
	IT bool `incite:"@ingestionTime"`
}

type badStructDeleted struct {
	DelFlag []int `incite:"@deleted"`
}

type badStructChanTagged struct {
	ChanField chan struct{} `incite:"MyField"`
}

type badStructFuncTagged struct {
	FuncField func() `incite:"MyFunc"`
}

type badStructComplex64Tagged struct {
	Complex64Field complex64 `incite:"MyComplex"`
}

type strongestTypedStruct struct {
	Ptr           string     `incite:"@ptr"`
	Timestamp     time.Time  `incite:"@timestamp"`
	IngestionTime *time.Time `incite:"@ingestionTime"`
	Deleted       bool       `incite:"@deleted"`

	JSONObject map[string]interface{} `json:"ObjectOfJSON"`
	JSONArray  []interface{}          `json:"ArrayOfJSON"`
	JSONString string                 `json:"StringOfJSON"`
	JSONNumber float64                `json:"NumberOfJSON"`
	JSONBool   bool                   `json:"BoolOfJSON"`

	Text1 indirectDummyTextUnmarshaler `incite:"TaggedText"`
	Text2 indirectDummyTextUnmarshaler

	Int       int
	Int8      int8
	Int16     int16
	Int32     int32
	Int64     int64
	Uint      uint
	Uint8     uint8
	Uint16    uint16
	Uint32    uint32
	Uint64    uint64
	Float32   float32
	Float64   float64
	Bool      bool
	String1   string `incite:"TaggedString"`
	String2   string
	Strings   []string
	ByteArray []byte // TODO: I'mt not sure what to do with this.
}

func ip(i interface{}) *interface{} {
	return &i
}

func ipp(i interface{}) **interface{} {
	p := ip(i)
	return &p
}

func sp(s string) *string {
	return &s
}

func spp(s string) **string {
	p := sp(s)
	return &p
}

func tmp(t time.Time) *time.Time {
	return &t
}

type directDummyTextUnmarshaler []string

func (dummy directDummyTextUnmarshaler) UnmarshalText(t []byte) error {
	if len(dummy) > 0 {
		dummy[0] = string(t)
	}
	return nil
}

type indirectDummyTextUnmarshaler struct {
	s string
}

func (dummy *indirectDummyTextUnmarshaler) UnmarshalText(t []byte) error {
	dummy.s = string(t)
	return nil
}

var single = []Result{r("@ptr", "foo")}

func r(pairs ...string) (result Result) {
	for i := 1; i < len(pairs); i += 2 {
		field := pairs[i-1]
		value := pairs[i]
		result = append(result, ResultField{
			Field: field,
			Value: value,
		})
	}
	return
}

func rp(r Result) *Result {
	return &r
}

func rpp(r Result) **Result {
	p := rp(r)
	return &p
}
