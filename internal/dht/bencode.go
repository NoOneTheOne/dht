package dht

import (
	"bytes"
	"errors"
	"sort"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// maxBencodeDepth limits simultaneously nested containers, including the root.
const maxBencodeDepth = 64

// find returns the index of first target in data starting from `start`.
// It returns -1 if target not found.
func find(data []byte, start int, target rune) (index int) {
	index = bytes.IndexRune(data[start:], target)
	if index != -1 {
		return index + start
	}
	return index
}

// DecodeString decodes a string in the data. It returns a tuple
// (decoded result, the end position, error).
func DecodeString(data []byte, start int) (
	result interface{}, index int, err error) {

	if start < 0 || start >= len(data) || data[start] < '0' || data[start] > '9' {
		err = errors.New("invalid string bencode")
		return
	}

	i := find(data, start, ':')
	if i == -1 {
		err = errors.New("':' not found when decode string")
		return
	}

	length, err := strconv.Atoi(string(data[start:i]))
	if err != nil {
		return
	}

	if length < 0 {
		err = errors.New("invalid length of string")
		return
	}

	index = i + 1 + length

	if index > len(data) || index < i+1 {
		err = errors.New("out of range")
		return
	}

	result = string(data[i+1 : index])
	return
}

// DecodeInt decodes int value in the data.
func DecodeInt(data []byte, start int) (
	result interface{}, index int, err error) {

	if start < 0 || start >= len(data) || data[start] != 'i' {
		err = errors.New("invalid int bencode")
		return
	}

	index = find(data, start+1, 'e')

	if index == -1 {
		err = errors.New("':' not found when decode int")
		return
	}

	parsed, err := strconv.ParseInt(string(data[start+1:index]), 10, 64)
	if err != nil {
		return nil, 0, err
	}
	if strconv.IntSize == 64 || (parsed >= -1<<31 && parsed <= 1<<31-1) {
		result = int(parsed)
	} else {
		result = parsed
	}
	index++
	return
}

// decodeItem decodes an item with depth enclosing containers.
func decodeItem(data []byte, i, depth int) (
	result interface{}, index int, err error) {

	if i < 0 || i >= len(data) {
		err = errors.New("invalid bencode when decode item")
		return
	}

	// First-byte dispatch instead of trying each decoder sequentially
	switch {
	case data[i] >= '0' && data[i] <= '9':
		return DecodeString(data, i)
	case data[i] == 'i':
		return DecodeInt(data, i)
	case data[i] == 'l':
		return decodeList(data, i, depth)
	case data[i] == 'd':
		return decodeDict(data, i, depth)
	default:
		err = errors.New("invalid bencode when decode item")
		return
	}
}

// DecodeList decodes a list prefix and returns the position after its closing 'e'.
func DecodeList(data []byte, start int) (
	result interface{}, index int, err error) {
	return decodeList(data, start, 0)
}

func decodeList(data []byte, start, depth int) (
	result interface{}, index int, err error) {

	if start < 0 || start >= len(data) || data[start] != 'l' {
		err = errors.New("invalid list bencode")
		return
	}
	if depth >= maxBencodeDepth {
		err = errors.New("bencode nesting too deep")
		return
	}

	var item interface{}
	r := make([]interface{}, 0, 8)

	index = start + 1
	for index < len(data) {
		char, _ := utf8.DecodeRune(data[index:])
		if char == 'e' {
			break
		}

		item, index, err = decodeItem(data, index, depth+1)
		if err != nil {
			return
		}
		r = append(r, item)
	}

	if index == len(data) {
		err = errors.New("'e' not found when decode list")
		return
	}
	index++

	result = r
	return
}

// DecodeDict decodes a dictionary prefix and returns the position after its closing 'e'.
func DecodeDict(data []byte, start int) (
	result interface{}, index int, err error) {
	return decodeDict(data, start, 0)
}

func decodeDict(data []byte, start, depth int) (
	result interface{}, index int, err error) {

	if start < 0 || start >= len(data) || data[start] != 'd' {
		err = errors.New("invalid dict bencode")
		return
	}
	if depth >= maxBencodeDepth {
		err = errors.New("bencode nesting too deep")
		return
	}

	var item, key interface{}
	r := make(map[string]interface{})

	index = start + 1
	for index < len(data) {
		char, _ := utf8.DecodeRune(data[index:])
		if char == 'e' {
			break
		}

		if !unicode.IsDigit(char) {
			err = errors.New("invalid dict bencode")
			return
		}

		key, index, err = DecodeString(data, index)
		if err != nil {
			return
		}

		if index >= len(data) {
			err = errors.New("out of range")
			return
		}

		item, index, err = decodeItem(data, index, depth+1)
		if err != nil {
			return
		}

		r[key.(string)] = item
	}

	if index == len(data) {
		err = errors.New("'e' not found when decode dict")
		return
	}
	index++

	result = r
	return
}

// Decode decodes exactly one bencoded string, int, list or map.
func Decode(data []byte) (result interface{}, err error) {
	result, index, err := decodeItem(data, 0, 0)
	if err != nil {
		return nil, err
	}
	if index != len(data) {
		return nil, errors.New("trailing data after bencode value")
	}
	return result, nil
}

// EncodeString encodes a string value.
func EncodeString(data string) string {
	return strconv.Itoa(len(data)) + ":" + data
}

// EncodeInt encodes a int value.
func EncodeInt(data int) string {
	return "i" + strconv.Itoa(data) + "e"
}

// Encode encodes a string, int, dict or list value to a bencoded string.
func Encode(data interface{}) string {
	var buf bytes.Buffer
	encodeToBuffer(&buf, data)
	return buf.String()
}

func encodeToBuffer(buf *bytes.Buffer, data interface{}) {
	switch v := data.(type) {
	case string:
		buf.WriteString(strconv.Itoa(len(v)))
		buf.WriteByte(':')
		buf.WriteString(v)
	case int:
		buf.WriteByte('i')
		buf.WriteString(strconv.Itoa(v))
		buf.WriteByte('e')
	case []interface{}:
		buf.WriteByte('l')
		for _, item := range v {
			encodeToBuffer(buf, item)
		}
		buf.WriteByte('e')
	case map[string]interface{}:
		buf.WriteByte('d')
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			buf.WriteString(strconv.Itoa(len(key)))
			buf.WriteByte(':')
			buf.WriteString(key)
			encodeToBuffer(buf, v[key])
		}
		buf.WriteByte('e')
	default:
		panic("invalid type when encode")
	}
}
