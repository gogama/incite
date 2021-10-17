// Copyright 2021 The incite Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package incite

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

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
					{{"thomas", "gray"}},
					{{"elegy", "written"}, {"in", "a"}, {"country", "churchyard"}},
				},
				v: &[]Result{},
				w: &[]Result{
					{},
					{{"thomas", "gray"}},
					{{"elegy", "written"}, {"in", "a"}, {"country", "churchyard"}},
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
					rp(Result{{"extra", "to"}, {"be", "removed"}}),
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
					{
						{"@ptr", "bar"},
						{"@message", "bar message"},
						{"@timestamp", "hello"},
						{"DiscoveredKey", "DiscoveredStringValue"},
						{"DiscoveredKey2", "-123"},
					},
					{
						{"@ptr", "baz"},
						{"@timestamp", "2021-06-19 03:59:59.936"},
						{"DiscoveredKey", `{"k":"string","k2":1,"k3":["another string",null,10]}`},
						{"@message", "baz message"},
						{"DiscoveredKey2", "1.5"},
					},
					{
						{"@timestamp", ""},
						{"@ingestionTime", "2021-10-17 14:20:00.000"},
					},
				},
				v: &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"@ptr":           "bar",
						"@message":       "bar message",
						"@timestamp":     "hello",
						"DiscoveredKey":  "DiscoveredStringValue",
						"DiscoveredKey2": -123.0,
					},
					{
						"@ptr":       "baz",
						"@message":   "baz message",
						"@timestamp": time.Date(2021, 6, 19, 3, 59, 59, int(936*time.Millisecond), time.UTC),
						"DiscoveredKey": map[string]interface{}{
							"k":  "string",
							"k2": 1.0,
							"k3": []interface{}{"another string", nil, 10.0},
						},
						"DiscoveredKey2": 1.5,
					},
					{
						"@timestamp":     "",
						"@ingestionTime": time.Date(2021, 10, 17, 14, 20, 0, 0, time.UTC),
					},
				},
			},
			{
				name: "[]map[interface{}],data:fuzzyJSON",
				data: []Result{
					{
						{"Whitespace", " \t\n\r"},
						{"WhitespacePrefixingJSONNull", "  null"},
						{"WhitespacePrefixingJSONFalse", "\nfalse"},
						{"WhitespacePrefixingJSONTrue", "\ntrue"},
						{"WhitespacePrefixingJSONNumber", "\r123"},
						{"WhitespacePrefixingJSONString", `  "string"`},
						{"WhitespacePrefixingJSONArray", "\t\t[1,1,1]"},
						{"WhitespacePrefixingJSONObject", ` {"ham":1,"eggs":2}`},
						{"WhitespacePrefixingGarbage", " non-literal garbage"},
					},
				},
				v: &[]map[string]interface{}{},
				w: &[]map[string]interface{}{
					{
						"Whitespace":                    " \t\n\r",
						"WhitespacePrefixingJSONNull":   nil,
						"WhitespacePrefixingJSONFalse":  false,
						"WhitespacePrefixingJSONTrue":   true,
						"WhitespacePrefixingJSONNumber": 123.0,
						"WhitespacePrefixingJSONString": "string",
						"WhitespacePrefixingJSONArray":  []interface{}{1.0, 1.0, 1.0},
						"WhitespacePrefixingJSONObject": map[string]interface{}{
							"ham":  1.0,
							"eggs": 2.0,
						},
						"WhitespacePrefixingGarbage": " non-literal garbage",
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
				data: []Result{{{"@ptr", "bar"}, {"Discovered1Key", "Discovered1Value"}}},
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
				data: []Result{{{"@ptr", "bar"}, {"Discovered1Key", "Discovered1Value"}}},
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
				data: []Result{{{"@message", `["world"]`}, {"@ptr", "hello"}, {"DiscoveredKey", "10"}}},
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
					{
						{"@message", `["world"]`},
						{"@ptr", "hello"},
						{"DiscoveredKey", "10"},
					},
					{
						{"@log", "111100001111:/some/log"},
						{"@logStream", "fizzle-fizzle"},
						{"@ingestionTime", "2021-06-19 03:59:59.936"},
						{"DiscoveredKey", "null"},
						{"@ptr", "Bonjour!"},
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
				data: []Result{{{"@message", `["world"]`}, {"@ptr", "hello"}, {"DiscoveredKey", "10"}}},
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
				data: []Result{{{"@message", `["world"]`}, {"@ptr", "hello"}, {"DiscoveredKey", "10"}}},
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
						{"AnyOfJSON", `"string"`},
						{"ObjectOfJSON", `{"key":"value"}`},
						{"ArrayOfJSON", `["first","second"]`},
						{"StringOfJSON", `"str"`},
						{"NumberOfJSON", `-99.9`},
						{"BoolOfJSON", `true`},
						{"TaggedJSON", `null`},
						{"JSON2", `{"foo":123,"bar":false}`},
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
						{"AnyOfJSON", "[false, true]"},
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
						JSONAny:       "string",
						JSONObject:    map[string]interface{}{"key": "value"},
						JSONArray:     []interface{}{"first", "second"},
						JSONString:    "str",
						JSONNumber:    -99.9,
						JSONBool:      true,
						JSON1:         indirectDummyJSONUnmarshaler{"null"},
						JSON2:         indirectDummyJSONUnmarshaler{`{"foo":123,"bar":false}`},
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
						JSONAny:    []interface{}{false, true},
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

			{
				name: "[]*specialFieldStringStruct,data:nil",
				v:    &[]*specialFieldStringStruct{},
				w:    &[]*specialFieldStringStruct{},
			},
			{
				name: "[]*specialFieldStringStruct{},data:empty",
				data: []Result{},
				v:    &[]*specialFieldStringStruct{},
				w:    &[]*specialFieldStringStruct{},
			},
			{
				name: "[]*specialFieldStringStruct{},data:single",
				data: single,
				v:    &[]*specialFieldStringStruct{},
				w:    &[]*specialFieldStringStruct{{}},
			},
			{
				name: "[]*specialFieldStringStruct{},data:multiple",
				data: []Result{
					{{"@deleted", "false"}, {"@timestamp", "foo"}},
					{{"@deleted", "true"}, {"@ingestionTime", "bar"}},
					{{"@deleted", "wacky"}, {"@ingestionTime", "2006-01-02 15:04:05.000"}},
				},
				v: &[]*specialFieldStringStruct{},
				w: &[]*specialFieldStringStruct{
					{Deleted: "false", Timestamp: "foo"},
					{Deleted: "true", IngestionTime: sp("bar")},
					{Deleted: "wacky", IngestionTime: sp("2006-01-02 15:04:05.000")},
				},
			},
			{
				name: "[0]map[string],data:nil",
				v:    &[0]map[string]string{},
				w:    &[0]map[string]string{},
			},
			{
				name: "[0]map[string],data:single",
				data: single,
				v:    &[0]map[string]string{},
				w:    &[0]map[string]string{},
			},

			{
				name: "[1]map[string],data:nil",
				data: nil,
				v: &[1]map[string]string{
					{
						"The map containing this key": "will be zeroed out to nil"},
				},
				w: &[1]map[string]string{},
			},
			{
				name: "[1]map[string],data:single",
				data: single,
				v: &[1]map[string]string{
					{"This map will be": "replaced with an unmarshalled map"},
				},
				w: &[1]map[string]string{
					{
						"@ptr": "foo",
					},
				},
			},
			{
				name: "[1]map[string],data:multiple",
				data: []Result{
					{{"@ptr", "1"}, {"DiscoveredField", "DiscoveredValue1"}},
					{{"@ptr", "2"}, {"DiscoveredField", "DiscoveredValue2"}},
				},
				v: &[1]map[string]string{},
				w: &[1]map[string]string{
					{
						"@ptr":            "1",
						"DiscoveredField": "DiscoveredValue1",
					},
				},
			},

			{
				name: "[2]map[string],data:single",
				data: single,
				v:    &[2]map[string]string{},
				w: &[2]map[string]string{
					{
						"@ptr": "foo",
					},
				},
			},
			{
				name: "[2]map[string],data:double",
				data: []Result{
					{{"@ptr", "1"}, {"DiscoveredField", "DiscoveredValue1"}},
					{{"@ptr", "2"}, {"DiscoveredField", "DiscoveredValue2"}},
				},
				v: &[2]map[string]string{},
				w: &[2]map[string]string{
					{
						"@ptr":            "1",
						"DiscoveredField": "DiscoveredValue1",
					},
					{
						"@ptr":            "2",
						"DiscoveredField": "DiscoveredValue2",
					},
				},
			},
			{
				name: "[2]map[string],data:triple",
				data: []Result{
					{{"@ptr", "1"}, {"DiscoveredField", "DiscoveredValue1"}},
					{{"@ptr", "2"}, {"DiscoveredField", "DiscoveredValue2"}},
					{{"@ptr", "3"}, {"DiscoveredField", "DiscoveredValue3"}},
				},
				v: &[2]map[string]string{},
				w: &[2]map[string]string{
					{
						"@ptr":            "1",
						"DiscoveredField": "DiscoveredValue1",
					},
					{
						"@ptr":            "2",
						"DiscoveredField": "DiscoveredValue2",
					},
				},
			},

			{
				name: "[]ignorableFieldStruct",
				data: []Result{
					{},
					{
						{"@ptr", "ignored"},
						{"Ignore1", "ignored"},
						{"Ignore2", "ignored"},
						{"Ignore3", "ignored"},
						{"Ignore4", "ignored"},
						{"Ignore5", "ignored"},
						{"String", "string1"},
					},
					{
						{"Ignore1", "ignored"},
					},
					{
						{"String", "string2"},
					},
				},
				v: &[]ignorableFieldStruct{},
				w: &[]ignorableFieldStruct{
					{},
					{String: "string1"},
					{},
					{String: "string2"},
				},
			},

			{
				name: "[]dashTagFieldStruct",
				data: []Result{
					{
						{"", `"Assigned to JSONNamedEmpty only"`},
						{"-", `"Assigned to JSONNamedDash only"`},
						{"foo", `"Assigned to JSONNamedFoo only"`},
					},
				},
				v: &[]dashTagFieldStruct{},
				w: &[]dashTagFieldStruct{
					{
						JSONNamedEmpty: "Assigned to JSONNamedEmpty only",
						JSONNamedDash:  "Assigned to JSONNamedDash only",
						JSONNamedFoo:   "Assigned to JSONNamedFoo only",
					},
				},
			},
		}

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
				name: "InvalidUnmarshalError(Bad Map Key, Depth 0, Slice)",
				v:    &[]map[int]string{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]map[int]string{})),
					RowType: reflect.TypeOf(map[int]string{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Map Key, Depth 0, Array)",
				v:    &[1]map[int]string{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([1]map[int]string{})),
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
				name: "InvalidUnmarshalError(Bad Slice Value, Depth 0)",
				v:    &[][]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([][]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Slice Value, Depth 1)",
				v:    &[]*[]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]*[]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Slice Value, Depth 2)",
				v:    &[]**[]int{},
				err: &InvalidUnmarshalError{
					Type:    reflect.PtrTo(reflect.TypeOf([]**[]int{})),
					RowType: reflect.TypeOf([]int{}),
				},
			},
			{
				name: "InvalidUnmarshalError(Bad Struct Field: @timestamp)",
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
				name: "InvalidUnmarshalError(Bad Struct Field: @ingestionTime)",
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
				name: "InvalidUnmarshalError(Bad Struct Field: @deleted)",
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
				name: "InvalidUnmarshalError(Bad Struct Field: chan, Tagged)",
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
				name: "InvalidUnmarshalError(Bad Struct Field: func, Tagged)",
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
				name: "InvalidUnmarshalError(Bad Struct Field: complex64, Tagged)",
				v:    &[]badStructComplex64Tagged{},
				err: &InvalidUnmarshalError{
					Type:      reflect.PtrTo(reflect.TypeOf([]badStructComplex64Tagged{})),
					RowType:   reflect.TypeOf(badStructComplex64Tagged{}),
					Field:     "MyComplex",
					FieldType: reflect.TypeOf(complex64(0)),
					Message:   "unsupported struct field type",
				},
			},

			{
				name: "UnmarshalResultFieldValueError(Overflow Int8)",
				data: []Result{
					{{"Int8", "999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Int8", "999"},
					Cause:       overflow("999", reflect.TypeOf(int8(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Int16)",
				data: []Result{
					{{"Int16", "99999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Int16", "99999"},
					Cause:       overflow("99999", reflect.TypeOf(int16(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Int32)",
				data: []Result{
					{{"Int32", "9999999999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Int32", "9999999999"},
					Cause:       overflow("9999999999", reflect.TypeOf(int32(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Uint8)",
				data: []Result{
					{{"Uint8", "999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Uint8", "999"},
					Cause:       overflow("999", reflect.TypeOf(uint8(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Uint16)",
				data: []Result{
					{{"Uint16", "99999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Uint16", "99999"},
					Cause:       overflow("99999", reflect.TypeOf(uint16(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Uint32)",
				data: []Result{
					{{"Uint32", "9999999999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Uint32", "9999999999"},
					Cause:       overflow("9999999999", reflect.TypeOf(uint32(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Overflow Float32)",
				data: []Result{
					{{"Float32", "999999999999999999999999999999999999999"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Float32", "999999999999999999999999999999999999999"},
					Cause:       overflow("999999999999999999999999999999999999999", reflect.TypeOf(float32(0))),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error Int64)",
				data: []Result{
					{{"Int64", "1"}},
					{{"String1", "1"}, {"String2", "2"}, {"Int64", "foo"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Int64", "foo"},
					Cause:       intParseError("foo"),
					ResultIndex: 1,
					FieldIndex:  2,
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error Uint64)",
				data: []Result{
					{{"Uint64", "bar"}, {"String1", "1"}},
					{{"String2", "2"}, {"Uint64", "123"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Uint64", "bar"},
					Cause:       uintParseError("bar"),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error Float64)",
				data: []Result{
					{{"Uint64", "88"}, {"String1", "1"}},
					{{"String2", "2"}, {"Float64", "baz"}, {"Float64", "10"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Float64", "baz"},
					Cause:       floatParseError("baz"),
					ResultIndex: 1,
					FieldIndex:  1,
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error Bool)",
				data: []Result{
					{{"Bool", "Supercalifragilisticexpialidocious"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"Bool", "Supercalifragilisticexpialidocious"},
					Cause:       boolParseError("Supercalifragilisticexpialidocious"),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error TextUnmarshaler)",
				data: []Result{
					{{"TaggedText", "error:testing error path"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"TaggedText", "error:testing error path"},
					Cause:       errors.New("testing error path"),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error JSON)",
				data: []Result{
					{{"AnyOfJSON", "horrible attempt at JSON"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"AnyOfJSON", "horrible attempt at JSON"},
					Cause:       jsonParseError("horrible attempt at JSON"),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(Parse Error Insights Time)",
				data: []Result{
					{{"@timestamp", "horrible attempt at Insights Timestamp"}},
				},
				v: &[]strongestTypedStruct{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"@timestamp", "horrible attempt at Insights Timestamp"},
					Cause:       timeParseError("horrible attempt at Insights Timestamp"),
				},
			},
			{
				name: "UnmarshalResultFieldValueError(TextUnmarshaler Error)",
				data: []Result{
					{{"foo", "error:bar"}},
				},
				v: &[]map[string]indirectDummyTextUnmarshaler{},
				err: &UnmarshalResultFieldValueError{
					ResultField: ResultField{"foo", "error:bar"},
					Cause:       errors.New("bar"),
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				err := Unmarshal(testCase.data, testCase.v)

				assert.Error(t, err)
				assert.Equal(t, testCase.err, err)

				if _, ok := testCase.err.(*UnmarshalResultFieldValueError); ok {
					v := reflect.ValueOf(testCase.v).Elem()
					assert.Equal(t, len(testCase.data), v.Len())
				}
			})
		}
	})

	t.Run("Special", func(t *testing.T) {
		t.Run("ShortSlice", func(t *testing.T) {
			data := []Result{
				{{"@ptr", "1"}, {"foo", "baz"}},
			}

			t.Run("SufficientCapacity", func(t *testing.T) {
				// ARRANGE.
				v := []map[string]string{
					{"foo": "bar"},
					{"ham": "eggs"},
				}[0:0]
				require.Len(t, v, 0)
				require.Equal(t, 2, cap(v))

				// ACT.
				err := Unmarshal(data, &v)

				// ASSERT.
				assert.NoError(t, err)
				assert.Equal(t, []map[string]string{
					{
						"@ptr": "1",
						"foo":  "baz",
					},
				}, v)
				assert.Equal(t, []map[string]string{{"ham": "eggs"}}, v[1:2])
			})

			t.Run("InsufficientCapacity", func(t *testing.T) {
				// ARRANGE.
				var v []map[string]string

				// ACT.
				err := Unmarshal(data, &v)

				// ASSERT.
				assert.NoError(t, err)
				assert.Equal(t, []map[string]string{
					{
						"@ptr": "1",
						"foo":  "baz",
					},
				}, v)
				assert.Equal(t, 1, cap(v))
			})
		})

		t.Run("BestEffort", func(t *testing.T) {
			data := []Result{
				{{"@deleted", "foo"}, {"String2", "value"}, {"TaggedText", "error:boom"}},
				{{"@deleted", "true"}},
			}

			t.Run("Map", func(t *testing.T) {
				// ARRANGE.
				var v []map[string]indirectDummyTextUnmarshaler

				// ACT.
				err := Unmarshal(data, &v)

				// ASSERT.
				assert.Equal(t, &UnmarshalResultFieldValueError{
					ResultField: data[0][2],
					Cause:       errors.New("boom"),
					ResultIndex: 0,
					FieldIndex:  2,
				}, err)
				assert.Equal(t, []map[string]indirectDummyTextUnmarshaler{
					{"@deleted": indirectDummyTextUnmarshaler{"foo"}, "String2": indirectDummyTextUnmarshaler{"value"}},
					{"@deleted": indirectDummyTextUnmarshaler{"true"}},
				}, v)
			})

			t.Run("Struct", func(t *testing.T) {
				// ARRANGE.
				var v []strongestTypedStruct

				// ACT.
				err := Unmarshal(data, &v)

				// ASSERT.
				assert.Equal(t, &UnmarshalResultFieldValueError{
					ResultField: data[0][0],
					Cause:       boolParseError("foo"),
				}, err)
				assert.Equal(t, []strongestTypedStruct{
					{String2: "value"},
					{Deleted: true},
				}, v)
			})
		})
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

func TestUnmarshalResultFieldValueError_Error(t *testing.T) {
	err := &UnmarshalResultFieldValueError{
		ResultField: ResultField{"badFieldName", "badValue"},
		Cause:       errors.New("some underlying problem"),
		ResultIndex: 8,
		FieldIndex:  13,
	}

	s := err.Error()

	assert.Equal(t, `incite: can't unmarshal field data[8][13] (name badFieldName) value "badValue": some underlying problem`, s)
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

	JSONAny    interface{}            `json:"AnyOfJSON"`
	JSONObject map[string]interface{} `json:"ObjectOfJSON"`
	JSONArray  []interface{}          `json:"ArrayOfJSON"`
	JSONString string                 `json:"StringOfJSON"`
	JSONNumber float64                `json:"NumberOfJSON"`
	JSONBool   bool                   `json:"BoolOfJSON"`

	JSON1 indirectDummyJSONUnmarshaler `json:"TaggedJSON"`
	JSON2 indirectDummyJSONUnmarshaler

	Text1 indirectDummyTextUnmarshaler `incite:"TaggedText"`
	Text2 indirectDummyTextUnmarshaler

	Int     int
	Int8    int8
	Int16   int16
	Int32   int32
	Int64   int64
	Uint    uint
	Uint8   uint8
	Uint16  uint16
	Uint32  uint32
	Uint64  uint64
	Float32 float32
	Float64 float64
	Bool    bool
	String1 string `incite:"TaggedString"`
	String2 string
	Strings []string
}

type ignorableFieldStruct struct {
	Ignore1 complex64
	Ignore2 complex128
	Ignore3 chan struct{}
	Ignore4 func()
	Ignore5 unsafe.Pointer
	String  string
}

type dashTagFieldStruct struct {
	Ignore1        string `incite:"-"`
	Ignore2        string `json:"-"`
	JSONNamedEmpty string `json:","`
	JSONNamedDash  string `json:"-,"`
	JSONNamedFoo   string `json:"foo,bar"`
}

type specialFieldStringStruct struct {
	Deleted       string  `incite:"@deleted"`
	Timestamp     string  `incite:"@timestamp"`
	IngestionTime *string `incite:"@ingestionTime"`
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
	s := string(t)
	if strings.HasPrefix(s, "error:") {
		return errors.New(s[6:])
	}
	dummy.s = s
	return nil
}

type indirectDummyJSONUnmarshaler struct {
	s string
}

func (dummy *indirectDummyJSONUnmarshaler) UnmarshalJSON(t []byte) error {
	dummy.s = string(t)
	return nil
}

var single = []Result{{{"@ptr", "foo"}}}

func rp(r Result) *Result {
	return &r
}

func rpp(r Result) **Result {
	p := rp(r)
	return &p
}

func intParseError(s string) (err error) {
	_, err = strconv.ParseInt(s, 10, 64)
	return
}

func uintParseError(s string) (err error) {
	_, err = strconv.ParseUint(s, 10, 64)
	return
}

func floatParseError(s string) (err error) {
	_, err = strconv.ParseFloat(s, 64)
	return
}

func boolParseError(s string) (err error) {
	_, err = strconv.ParseBool(s)
	return
}

func jsonParseError(s string) (err error) {
	var i interface{}
	err = json.Unmarshal([]byte(s), &i)
	return
}

func timeParseError(s string) (err error) {
	_, err = time.Parse(TimeLayout, s)
	return
}
