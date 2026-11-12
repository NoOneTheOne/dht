package dht

import (
	"fmt"
	"reflect"
	"strings"
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

func nestedBencode(pattern string, depth int, leaf string) string {
	var b strings.Builder
	for i := 0; i < depth; i++ {
		if pattern[i%len(pattern)] == 'd' {
			b.WriteByte('d')
			if i < depth-1 || leaf != "" {
				b.WriteString("1:k")
			}
		} else {
			b.WriteByte('l')
		}
	}
	b.WriteString(leaf)
	b.WriteString(strings.Repeat("e", depth))
	return b.String()
}

func TestDecodeContainerDepth(t *testing.T) {
	for _, pattern := range []string{"l", "d", "ld", "dl"} {
		for _, depth := range []int{1, 63, 64, 65, 4096} {
			for _, leaf := range []string{"", "i1e"} {
				t.Run(fmt.Sprintf("%s/depth_%d/leaf_%q", pattern, depth, leaf), func(t *testing.T) {
					input := nestedBencode(pattern, depth, leaf)
					wantErr := depth > 64
					got, err := Decode([]byte(input))
					if (err != nil) != wantErr {
						t.Fatalf("Decode() error = %v, wantErr %v", err, wantErr)
					}

					decode := DecodeList
					if pattern[0] == 'd' {
						decode = DecodeDict
					}
					// Public entry points count the root identically and still decode prefixes.
					prefixed := []byte("xx" + input + "\x00metadata")
					result, index, err := decode(prefixed, 2)
					if (err != nil) != wantErr {
						t.Fatalf("container decoder error = %v, wantErr %v", err, wantErr)
					}
					if !wantErr {
						if index != 2+len(input) {
							t.Errorf("consumed index = %d, want %d", index, 2+len(input))
						}
						if !reflect.DeepEqual(result, got) {
							t.Error("container decoder result differs from Decode")
						}
					}
				})
			}
		}
	}

	// The limit is nesting depth, not the number of containers visited.
	input := "l" + strings.Repeat(nestedBencode("ld", 63, "i1e"), 100) + "e"
	if _, err := Decode([]byte(input)); err != nil {
		t.Fatalf("sibling containers at allowed depth: %v", err)
	}
}

func TestDecodeInvalidStart(t *testing.T) {
	for _, tt := range []struct {
		name   string
		input  string
		decode func([]byte, int) (interface{}, int, error)
	}{
		{"string", "0:", DecodeString},
		{"int", "i0e", DecodeInt},
		{"list", "le", DecodeList},
		{"dict", "de", DecodeDict},
	} {
		t.Run(tt.name, func(t *testing.T) {
			maxInt := int(^uint(0) >> 1)
			for _, start := range []int{-1, -64, -maxInt - 1, len(tt.input), len(tt.input) + 1, maxInt} {
				if _, _, err := tt.decode([]byte(tt.input), start); err == nil {
					t.Errorf("start %d: expected error", start)
				}
				if _, _, err := tt.decode(nil, start); err == nil {
					t.Errorf("nil input, start %d: expected error", start)
				}
			}
		})
	}
}

func TestDecodeTrailingData(t *testing.T) {
	for _, input := range []string{"0:", "i1e", "le", "de", "d1:ali1eee"} {
		for _, suffix := range []string{"x", "i2e", "e", "\n", "\x00"} {
			if _, err := Decode([]byte(input + suffix)); err == nil {
				t.Errorf("Decode(%q) accepted trailing data", input+suffix)
			}
		}
	}
}

func TestDecodeContainerPrefix(t *testing.T) {
	for _, tt := range []struct {
		name   string
		input  string
		want   interface{}
		decode func([]byte, int) (interface{}, int, error)
	}{
		{"list", "li42ee", []interface{}{42}, DecodeList},
		{"metadata header", "d8:msg_typei1e5:piecei0ee", map[string]interface{}{"msg_type": 1, "piece": 0}, DecodeDict},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, start := range []int{0, 2} {
				const payload = "\x00\xffraw metadata"
				data := []byte(strings.Repeat("x", start) + tt.input + payload)
				got, index, err := tt.decode(data, start)
				if err != nil {
					t.Fatalf("container decoder: %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("result = %v, want %v", got, tt.want)
				}
				if index != start+len(tt.input) {
					t.Fatalf("consumed index = %d, want %d", index, start+len(tt.input))
				}
				if string(data[index:]) != payload {
					t.Error("consumed index did not preserve payload")
				}
			}
		})
	}
}

func TestDecodeMalformedTruncation(t *testing.T) {
	for _, input := range []string{
		"5:hello", "i42e", "l5:helloi42ee", "d1:ali42eee",
		nestedBencode("l", 64, "i1e"),
		nestedBencode("d", 64, "i1e"),
		nestedBencode("ld", 64, "i1e"),
	} {
		for end := 0; end < len(input); end++ {
			data := []byte(input[:end])
			if _, err := Decode(data); err == nil {
				t.Errorf("Decode accepted truncated input %q", data)
			}
			var decode func([]byte, int) (interface{}, int, error)
			switch input[0] {
			case 'l':
				decode = DecodeList
			case 'd':
				decode = DecodeDict
			case 'i':
				decode = DecodeInt
			default:
				decode = DecodeString
			}
			if _, _, err := decode(data, 0); err == nil {
				t.Errorf("public decoder accepted truncated input %q", data)
			}
		}
	}
	for _, input := range []string{
		"lxe", "d1:ae", "d1:a", "d1:ai1e", "dliee", "d1:al", "li1e",
		"999999999999999999999999999999:", "9223372036854775807:x", "l\xffe",
	} {
		if _, err := Decode([]byte(input)); err == nil {
			t.Errorf("Decode accepted malformed input %q", input)
		}
	}
}

func FuzzBencodeDecode(f *testing.F) {
	for _, input := range []string{
		"", "0:", "5:hello", "i-7e", "le", "de", "d1:ali1eee",
		"d8:msg_typei1e5:piecei0ee\x00\xffmetadata", "xxli1eejunk",
		"d1:ae", "d1:al", "li1e", "i", "5:hell", "l\xffe",
		"999999999999999999999999999999:", "9223372036854775807:x",
		nestedBencode("l", 64, "i1e"), nestedBencode("d", 65, "i1e"),
		nestedBencode("ld", 4096, "i1e"),
	} {
		for _, start := range []int{-1, 0, 2, len(input)} {
			f.Add([]byte(input), start)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, start int) {
		// Any panic fails the fuzz test, including calls with arbitrary indices.
		if got, err := Decode(data); err == nil {
			decoded, err := Decode([]byte(Encode(got)))
			if err != nil || !reflect.DeepEqual(decoded, got) {
				t.Fatalf("successful Decode failed roundtrip: %v", err)
			}
			if _, err := Decode(append(append([]byte(nil), data...), 0)); err == nil {
				t.Fatal("Decode accepted trailing byte")
			}
		}
		for _, decode := range []func([]byte, int) (interface{}, int, error){
			DecodeString, DecodeInt, DecodeList, DecodeDict,
		} {
			for _, offset := range []int{0, start} {
				got, index, err := decode(data, offset)
				if err != nil {
					continue
				}
				if offset < 0 || index <= offset || index > len(data) {
					t.Fatalf("successful decoder returned invalid span [%d:%d] for length %d", offset, index, len(data))
				}
				decoded, err := Decode(data[offset:index])
				if err != nil || !reflect.DeepEqual(decoded, got) {
					t.Fatalf("consumed span does not decode to the same value: %v", err)
				}
			}
		}
	})
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
