package dht

import (
	"reflect"
	"testing"
)

func TestDecodeString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantIdx int
		wantErr bool
	}{
		{"simple", "5:hello", "hello", 7, false},
		{"empty", "0:", "", 2, false},
		{"with digits", "3:abc", "abc", 5, false},
		{"invalid no colon", "5hello", "", 0, true},
		{"invalid length", "9:hello", "", 0, true},
		{"invalid start", "x:hello", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, idx, err := DecodeString([]byte(tt.input), 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if result.(string) != tt.want {
					t.Errorf("DecodeString() = %v, want %v", result, tt.want)
				}
				if idx != tt.wantIdx {
					t.Errorf("DecodeString() idx = %v, want %v", idx, tt.wantIdx)
				}
			}
		})
	}
}

func TestDecodeInt(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int
		wantErr bool
	}{
		{"positive", "i42e", 42, false},
		{"zero", "i0e", 0, false},
		{"negative", "i-7e", -7, false},
		{"invalid no i", "42e", 0, true},
		{"invalid no e", "i42", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := DecodeInt([]byte(tt.input), 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result.(int) != tt.want {
				t.Errorf("DecodeInt() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDecodeList(t *testing.T) {
	result, _, err := DecodeList([]byte("l5:helloi42ee"), 0)
	if err != nil {
		t.Fatalf("DecodeList() error = %v", err)
	}
	list := result.([]interface{})
	if len(list) != 2 {
		t.Fatalf("DecodeList() len = %d, want 2", len(list))
	}
	if list[0].(string) != "hello" {
		t.Errorf("DecodeList()[0] = %v, want hello", list[0])
	}
	if list[1].(int) != 42 {
		t.Errorf("DecodeList()[1] = %v, want 42", list[1])
	}
}

func TestDecodeDict(t *testing.T) {
	result, _, err := DecodeDict([]byte("d3:foo3:bar3:bazi99ee"), 0)
	if err != nil {
		t.Fatalf("DecodeDict() error = %v", err)
	}
	dict := result.(map[string]interface{})
	if dict["foo"].(string) != "bar" {
		t.Errorf("DecodeDict()[foo] = %v, want bar", dict["foo"])
	}
	if dict["baz"].(int) != 99 {
		t.Errorf("DecodeDict()[baz] = %v, want 99", dict["baz"])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := map[string]interface{}{
		"id": "abcdefghij0123456789",
		"q":  "ping",
		"t":  "aa",
		"y":  "q",
		"a": map[string]interface{}{
			"id": "mnopqrstuvwxyz123456",
		},
	}

	encoded := Encode(original)
	decoded, err := Decode([]byte(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	result := decoded.(map[string]interface{})
	if result["y"].(string) != "q" {
		t.Errorf("roundtrip y = %v, want q", result["y"])
	}
	if result["q"].(string) != "ping" {
		t.Errorf("roundtrip q = %v, want ping", result["q"])
	}

	a := result["a"].(map[string]interface{})
	if a["id"].(string) != "mnopqrstuvwxyz123456" {
		t.Errorf("roundtrip a.id = %v, want mnopqrstuvwxyz123456", a["id"])
	}
}

func TestEncodeString(t *testing.T) {
	if got := EncodeString("hello"); got != "5:hello" {
		t.Errorf("EncodeString(hello) = %v, want 5:hello", got)
	}
	if got := EncodeString(""); got != "0:" {
		t.Errorf("EncodeString() = %v, want 0:", got)
	}
}

func TestEncodeInt(t *testing.T) {
	if got := EncodeInt(42); got != "i42e" {
		t.Errorf("EncodeInt(42) = %v, want i42e", got)
	}
	if got := EncodeInt(0); got != "i0e" {
		t.Errorf("EncodeInt(0) = %v, want i0e", got)
	}
}

func TestEncodeList(t *testing.T) {
	data := []interface{}{"hello", 42}
	got := Encode(data)
	if got != "l5:helloi42ee" {
		t.Errorf("Encode(list) = %v, want l5:helloi42ee", got)
	}
}

func TestDecodeInvalid(t *testing.T) {
	_, err := Decode([]byte(""))
	if err == nil {
		t.Error("Decode('') should return error")
	}

	_, err = Decode([]byte("x"))
	if err == nil {
		t.Error("Decode('x') should return error")
	}
}

func TestDecodeNested(t *testing.T) {
	input := "ld5:helloi1eed5:worldi2eee"
	result, err := Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	list := result.([]interface{})
	if len(list) != 2 {
		t.Fatalf("nested list len = %d, want 2", len(list))
	}

	d1 := list[0].(map[string]interface{})
	if d1["hello"].(int) != 1 {
		t.Errorf("nested d1[hello] = %v, want 1", d1["hello"])
	}

	d2 := list[1].(map[string]interface{})
	if d2["world"].(int) != 2 {
		t.Errorf("nested d2[world] = %v, want 2", d2["world"])
	}
}

func TestEncodeDictKeyOrder(t *testing.T) {
	data := map[string]interface{}{
		"z": "last",
		"a": "first",
		"m": "middle",
	}
	encoded := Encode(data)
	expected := "d1:a5:first1:m6:middle1:z4:laste"
	if encoded != expected {
		t.Errorf("Encode(dict) = %v, want %v (keys should be sorted)", encoded, expected)
	}
}

func TestDecodeStringMidSlice(t *testing.T) {
	data := []byte("xx5:helloyy")
	result, idx, err := DecodeString(data, 2)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	if result.(string) != "hello" {
		t.Errorf("result = %v, want hello", result)
	}
	if idx != 9 {
		t.Errorf("idx = %v, want 9", idx)
	}
}

func TestParseKey(t *testing.T) {
	data := map[string]interface{}{
		"name": "test",
		"age":  25,
		"tags": []interface{}{"a", "b"},
		"meta": map[string]interface{}{"k": "v"},
	}

	if err := ParseKey(data, "name", "string"); err != nil {
		t.Errorf("ParseKey name string: %v", err)
	}
	if err := ParseKey(data, "age", "int"); err != nil {
		t.Errorf("ParseKey age int: %v", err)
	}
	if err := ParseKey(data, "tags", "list"); err != nil {
		t.Errorf("ParseKey tags list: %v", err)
	}
	if err := ParseKey(data, "meta", "map"); err != nil {
		t.Errorf("ParseKey meta map: %v", err)
	}
	if err := ParseKey(data, "missing", "string"); err == nil {
		t.Error("ParseKey missing key should error")
	}
	if err := ParseKey(data, "name", "int"); err == nil {
		t.Error("ParseKey wrong type should error")
	}
}

func TestParseKeys(t *testing.T) {
	data := map[string]interface{}{
		"t": "aa",
		"y": "q",
		"q": "ping",
	}
	err := ParseKeys(data, [][]string{
		{"t", "string"},
		{"y", "string"},
		{"q", "string"},
	})
	if err != nil {
		t.Errorf("ParseKeys() error = %v", err)
	}
}

func TestEncodeDecodeTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"string", "hello"},
		{"int", 42},
		{"empty list", []interface{}{}},
		{"empty dict", map[string]interface{}{}},
		{"nested", map[string]interface{}{
			"list": []interface{}{1, "two", 3},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := Encode(tt.input)
			decoded, err := Decode([]byte(encoded))
			if err != nil {
				t.Fatalf("Decode error: %v", err)
			}
			if !reflect.DeepEqual(decoded, tt.input) {
				t.Errorf("roundtrip mismatch: got %v, want %v", decoded, tt.input)
			}
		})
	}
}
