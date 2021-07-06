package incite

import (
	"testing"
	"time"

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
				name: "map[value:interface{},data:nil]",
				v:    &[]map[string]interface{}{},
				w:    &[]map[string]interface{}{},
			},
			{
				name: "map[value:interface{},data:empty]",
				data: []Result{},
				v:    &[]map[string]interface{}{},
				w:    &[]map[string]interface{}{},
			},
			{
				name: "map[value:interface{},data:single]",
				data: []Result{
					{
						Ptr: "foo",
						Fields: []ResultField{
							{
								Field: "@ptr",
								Value: "foo",
							},
						},
					},
				},
				v: &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"@ptr": "foo",
					},
				},
			},
			{
				name: "map[value:interface{},data:multiple]",
				data: []Result{
					{
						Ptr: "bar",
						Fields: []ResultField{
							{
								Field: "@ptr",
								Value: "bar",
							},
							{
								Field: "@message",
								Value: "bar message",
							},
							{
								Field: "@timestamp",
								Value: "",
							},
							{
								Field: "DiscoveredKey",
								Value: "DiscoveredStringValue",
							},
							{
								Field: "DiscoveredKey2",
								Value: "-123",
							},
						},
					},
					{
						Ptr: "baz",
						Fields: []ResultField{
							{
								Field: "@ptr",
								Value: "baz",
							},
							{
								Field: "@timestamp",
								Value: "TODO: timestamp",
							},
							{
								Field: "DiscoveredKey",
								Value: `{"k":"string","k2":1,"k3":["another string",null,10]}`,
							},
							{
								Field: "@message",
								Value: "baz message",
							},
							{
								Field: "DiscoveredKey2",
								Value: "1.5",
							},
						},
					},
				},
				v: &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"@ptr":           "bar",
						"@message":       "bar message",
						"@timestamp":     time.Time{},
						"DiscoveredKey":  "DiscoveredStringValue",
						"DiscoveredKey2": -123.0,
					},
					{
						"@ptr":       "baz",
						"@message":   "baz message",
						"@timestamp": time.Time{},
						"DiscoveredKey": map[string]interface{}{
							"k":  "string",
							"k2": 1,
							"k3": []interface{}{"another string", nil, 10},
						},
						"DiscoveredKey2": 1.5,
					},
				},
			},

			{
				name: "map[value:string,data:nil]",
				v:    &[]map[string]string{},
				w:    &[]map[string]string{},
			},
			{
				name: "map[value:string,data:empty]",
				data: []Result{},
				v:    &[]map[string]string{},
				w:    &[]map[string]string{},
			},
			{
				name: "map[value:string,data:single]",
				data: []Result{
					{
						Fields: []ResultField{
							{
								Field: "@message",
								Value: `["world"]`,
							},
							{
								Field: "@ptr",
								Value: "hello",
							},
							{
								Field: "DiscoveredKey",
								Value: "10",
							},
						},
					},
				},
				v: &[]map[string]string{},
				w: &[]map[string]string{
					{
						"@ptr":          "hello",
						"@message":      `["world"]`,
						"DiscoveredKey": "10",
					},
				},
			},
			{
				name: "map[value:string,data:multiple]",
				data: []Result{
					{
						Fields: []ResultField{
							{
								Field: "@message",
								Value: `["world"]`,
							},
							{
								Field: "@ptr",
								Value: "hello",
							},
							{
								Field: "DiscoveredKey",
								Value: "10",
							},
						},
					},
					{
						Fields: []ResultField{
							{
								Field: "@log",
								Value: "111100001111:/some/log",
							},
							{
								Field: "@logStream",
								Value: "fizzle-fizzle",
							},
							{
								Field: "@ingestionTime",
								Value: "TODO: put timestamp",
							},
							{
								Field: "DiscoveredKey",
								Value: "null",
							},
							{
								Field: "@ptr",
								Value: "Bonjour!",
							},
						},
					},
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
						"@ingestionTime": "TODO: put timestamp",
						"@DiscoveredKey": "null",
					},
				},
			},

			{
				name: "struct[value:json->string,data:nil]",
			},
			{
				name: "struct[value:json->string,data:empty]",
			},
			{
				name: "struct[value:json->string,data:single]",
			},
			{
				name: "struct[value:json->string,data:multiple]",
			},

			{
				name: "struct[value:json->map,data:nil]",
			},
			{
				name: "struct[value:json->map,data:empty]",
			},
			{
				name: "struct[value:json->map,data:single]",
			},
			{
				name: "struct[value:json->map,data:multiple]",
			},

			{
				name: "struct[value:json->structured,data:nil]",
			},
			{
				name: "struct[value:json->structured,data:empty]",
			},
			{
				name: "struct[value:json->structured,data:single]",
			},
			{
				name: "struct[value:json->structured,data:multiple]",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				err := Unmarshal(testCase.data, testCase.v)

				assert.NoError(t, err)
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
			// nil
			// notPointer[...various...]
			// notSlice[...various...]
			// notStruct[...various...]
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
