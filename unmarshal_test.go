package incite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshal(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		testCases := []struct {
			name string
			data []Result
			v, w interface{}
		}{
			// TODO: Put copy cases here.

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
								Value: "2021-06-19 03:59:59.936",
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
				data: []Result{
					{
						Ptr: "foo",
						Fields: []ResultField{
							{
								Field: "@ptr",
								Value: "bar",
							},
							{
								Field: "Discovered1Key",
								Value: "Discovered1Value",
							},
						},
					},
				},
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
				data: []Result{
					{
						Ptr: "foo",
						Fields: []ResultField{
							{
								Field: "@ptr",
								Value: "bar",
							},
							{
								Field: "Discovered1Key",
								Value: "Discovered1Value",
							},
						},
					},
				},
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
				name: "[]map[string],data:multiple",
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
								Value: "2021-06-19 03:59:59.936",
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
		}

		// TODO: Case where input slice contains pointers to maps, structs, etc.
		// TODO: Data longer than input slice case.
		// TODO: Data shorter than input slice case.
		// TODO: Array cases.

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

var single = []Result{
	{
		Ptr: "foo",
		Fields: []ResultField{
			{
				Field: "@ptr",
				Value: "foo",
			},
		},
	},
}
