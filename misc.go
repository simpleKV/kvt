package kvt

import (
	"reflect"
	"runtime"
	"strings"
	"unsafe"
)

type Ptr = unsafe.Pointer
type Size_t = uintptr

func Bytes(s Ptr, size Size_t) []byte {
	p := (*[1<<31 - 1]byte)(s)
	return (*p)[0:size]
}

func RealErr(err error) bool {
	return (*[2]uintptr)(unsafe.Pointer(&err))[1] != 0
}

// make up several field bytes into a index key
func MakeIndexKey(dst, k1 []byte, slc ...[]byte) []byte {

	dst = joinKeyWithTokenEscaped(defaultKeyJoiner, defaultKeyEscaper, dst, k1)
	//always append a token after a key end
	dst = append(dst, defaultKeyJoiner)

	for i := range slc {
		dst = joinKeyWithTokenEscaped(defaultKeyJoiner, defaultKeyEscaper, dst, slc[i])
		//always append a token after a key end
		dst = append(dst, defaultKeyJoiner)
	}
	return dst
}

// split a index key to several field bytes
func SplitIndexKey(content []byte) (result [][]byte) {

	result = doSplitIndexKey(defaultKeyEscaper, defaultKeyJoiner, content, []byte{}, [][]byte{})
	return result
}

// without a tail token compare with MakeIndexKey
func AppendLastKey(dst, raw []byte) []byte {
	return joinKeyWithTokenEscaped(defaultKeyJoiner, defaultKeyEscaper, dst, raw)
}

func joinKeyWithTokenEscaped(token, escaper byte, dst, k1 []byte) []byte {
	for i := range k1 {
		switch k1[i] {
		case token, escaper:
			dst = append(dst, escaper, k1[i])
		default:
			dst = append(dst, k1[i])
		}
	}
	return dst
}

func JoinKeysWithTokenEscaped(token, escaper byte, dst, k1 []byte, slc ...[]byte) []byte {
	for i := range k1 {
		switch k1[i] {
		case token, escaper:
			dst = append(dst, escaper, k1[i])
		default:
			dst = append(dst, k1[i])
		}
	}
	//always append a token after a key end
	dst = append(dst, token)

	for i := range slc {
		dst = JoinKeysWithTokenEscaped(token, escaper, dst, slc[i])
	}
	return dst
}

func doSplitIndexKey(escaper, token byte, content []byte, key []byte, result [][]byte) [][]byte {
	switch len(content) {
	case 0:
		result = append(result, key)
		return result
	case 1:
		switch content[0] {
		case escaper, token:
			result = append(result, key)
			return result
		default:
			key = append(key, content[0])
			result = append(result, key)
			return result
		}
	default: // len > 1
		for i := 0; i < len(content)-1; i++ {
			switch content[i] {
			case escaper:
				key = append(key, content[i+1])
				return doSplitIndexKey(escaper, token, content[i+2:], key, result)
			case token:
				result = append(result, key)
				key = []byte{}
			default:
				key = append(key, content[i])
			}
		}
		return doSplitIndexKey(escaper, token, content[len(content)-1:], key, result)
	}
}

func getFunctionName(i any) string {
	return basename(runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name())
}

// packageName.functiong -> function
func basename(str string) string {

	i := strings.LastIndexByte(str, '.')
	switch i {
	case -1:
		return str
	case len(str) - 1:
		return ""
	default:
		return str[i+1:]
	}
}
