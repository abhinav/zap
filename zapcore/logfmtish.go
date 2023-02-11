// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/internal/bufferpool"
)

// logfmtishEncoder is an encoder for Zap
// that writes output in a format similar to logfmt.
type logfmtishEncoder struct {
	ctx *buffer.Buffer // context for this key
	buf *buffer.Buffer

	idx int64 // used only if encoding an array
}

// NewLogfmtishEncoder builds a new logfmt-style encoder.
func NewLogfmtishEncoder() Encoder {
	// TODO: Rename logfmtishEncoder to something else, export that,
	// and return it from NewLogfmtishEncoder as a struct.

	return &logfmtishEncoder{
		ctx: bufferpool.Get(),
		buf: bufferpool.Get(),
	}
}

func (enc *logfmtishEncoder) addKey(key string) {
	if enc.buf.Len() > 0 {
		enc.buf.WriteByte(' ')
	}
	if enc.ctx.Len() > 0 {
		enc.buf.Write(enc.ctx.Bytes())
		enc.buf.AppendByte('.')
	}
	enc.buf.AppendString(key)
	enc.buf.AppendByte('=')
}

func (enc *logfmtishEncoder) AddArray(key string, v ArrayMarshaler) error {
	arrenc := logfmtishEncoder{ctx: bufferpool.Get(), buf: enc.buf}
	if enc.ctx.Len() > 0 {
		arrenc.ctx.Write(enc.ctx.Bytes())
		arrenc.ctx.AppendByte('.')
	}
	arrenc.ctx.AppendString(key)
	return v.MarshalLogArray(&arrenc)
}

func (enc *logfmtishEncoder) AddObject(key string, v ObjectMarshaler) error {
	objenc := logfmtishEncoder{ctx: bufferpool.Get(), buf: enc.buf}
	if enc.ctx.Len() > 0 {
		objenc.ctx.Write(enc.ctx.Bytes())
		objenc.ctx.AppendByte('.')
	}
	objenc.ctx.AppendString(key)
	return v.MarshalLogObject(&objenc)
}

func (enc *logfmtishEncoder) AddBinary(k string, v []byte) {
	enc.AddString(k, base64.StdEncoding.EncodeToString(v))
}

func (enc *logfmtishEncoder) AddByteString(k string, v []byte) {
	enc.addKey(k)
	enc.buf.AppendString(string(v)) // TODO: handle special charactesr
}

func (enc *logfmtishEncoder) AddBool(k string, v bool) {
	enc.addKey(k)
	enc.buf.AppendBool(v)
}

func (enc *logfmtishEncoder) AddComplex128(k string, v complex128) {
	enc.addKey(k)
	enc.appendComplex(v, 64)
}

func (enc *logfmtishEncoder) AddComplex64(k string, v complex64) {
	enc.addKey(k)
	enc.appendComplex(complex128(v), 32)
}

func (enc *logfmtishEncoder) AddDuration(k string, v time.Duration) {
	enc.addKey(k)
	enc.appendInt(v.Milliseconds()) // TODO: customize
}

func (enc *logfmtishEncoder) AddFloat64(k string, v float64) {
	enc.addKey(k)
	enc.appendFloat(v, 64)
}

func (enc *logfmtishEncoder) AddFloat32(k string, v float32) {
	enc.addKey(k)
	enc.appendFloat(float64(v), 32)
}

func (enc *logfmtishEncoder) AddInt64(k string, v int64) {
	enc.addKey(k)
	enc.appendInt(v)
}

func (enc *logfmtishEncoder) AddInt(k string, v int)     { enc.AddInt64(k, int64(v)) }
func (enc *logfmtishEncoder) AddInt32(k string, v int32) { enc.AddInt64(k, int64(v)) }
func (enc *logfmtishEncoder) AddInt16(k string, v int16) { enc.AddInt64(k, int64(v)) }
func (enc *logfmtishEncoder) AddInt8(k string, v int8)   { enc.AddInt64(k, int64(v)) }

func (enc *logfmtishEncoder) AddString(k, v string) {
	enc.addKey(k)
	enc.buf.AppendString(v) // TODO: special characters
}

func (enc *logfmtishEncoder) AddTime(k string, v time.Time) {
	enc.addIndex()
	// TODO: customizable encoder
	enc.appendInt(v.UnixMilli())
}

func (enc *logfmtishEncoder) AddUint64(k string, v uint64) {
	enc.addKey(k)
	enc.buf.AppendUint(v)
}

func (enc *logfmtishEncoder) AddUint(k string, v uint)       { enc.AddUint64(k, uint64(v)) }
func (enc *logfmtishEncoder) AddUint32(k string, v uint32)   { enc.AddUint64(k, uint64(v)) }
func (enc *logfmtishEncoder) AddUint16(k string, v uint16)   { enc.AddUint64(k, uint64(v)) }
func (enc *logfmtishEncoder) AddUint8(k string, v uint8)     { enc.AddUint64(k, uint64(v)) }
func (enc *logfmtishEncoder) AddUintptr(k string, v uintptr) { enc.AddUint64(k, uint64(v)) }

func (enc *logfmtishEncoder) AddReflected(k string, x interface{}) error {
	if x == nil {
		enc.addKey(k)
		enc.buf.AppendString("null")
		return nil
	}

	v := reflect.ValueOf(x)
	return getReflectMarshaler(v.Type(), false /* match interface */).AddTo(enc, k, v)
}

func (enc *logfmtishEncoder) OpenNamespace(k string) {
	if enc.ctx.Len() > 0 {
		enc.ctx.AppendByte('.')
	}
	enc.ctx.AppendString(k)
}

func (enc *logfmtishEncoder) Clone() Encoder {
	clone := logfmtishEncoder{
		ctx: bufferpool.Get(),
		buf: bufferpool.Get(),
		idx: enc.idx,
	}
	clone.ctx.Write(enc.ctx.Bytes())
	clone.buf.Write(enc.buf.Bytes())
	return &clone
}

func (enc *logfmtishEncoder) EncodeEntry(ent Entry, fs []Field) (*buffer.Buffer, error) {
	line := bufferpool.Get()

	// TODO: time
	line.WriteString(ent.Level.CapitalString())
	line.WriteByte('\t')
	line.WriteString(ent.Message)
	if len(fs) > 0 {
		line.WriteByte('\t')
	}

	enc = enc.Clone().(*logfmtishEncoder)
	for _, f := range fs {
		f.AddTo(enc)
	}

	line.Write(enc.buf.Bytes())
	line.AppendByte('\n')
	return line, nil
}

func (enc *logfmtishEncoder) addIndex() {
	if enc.buf.Len() > 0 {
		enc.buf.WriteByte(' ')
	}
	enc.buf.Write(enc.ctx.Bytes())
	enc.buf.AppendByte('[')
	enc.buf.AppendInt(enc.idx)
	enc.buf.AppendString("]=")

	enc.idx++
}

func (enc *logfmtishEncoder) AppendBool(v bool) {
	enc.addIndex()
	enc.buf.AppendBool(v)
}

func (enc *logfmtishEncoder) AppendByteString(v []byte) {
	enc.addIndex()
	enc.buf.AppendString(string(v)) // TOOD: handle special chars
}

func (enc *logfmtishEncoder) appendComplex(val complex128, precision int) {
	// Cast to a platform-independent, fixed-size type.
	r, i := float64(real(val)), float64(imag(val))
	// Because we're always in a quoted string, we can use strconv without
	// special-casing NaN and +/-Inf.
	enc.buf.AppendFloat(r, precision)
	// If imaginary part is less than 0, minus (-) sign is added by default
	// by AppendFloat.
	if i >= 0 {
		enc.buf.AppendByte('+')
	}
	enc.buf.AppendFloat(i, precision)
	enc.buf.AppendByte('i')
}

func (enc *logfmtishEncoder) AppendComplex128(v complex128) {
	enc.addIndex()
	enc.appendComplex(v, 64)
}

func (enc *logfmtishEncoder) AppendComplex64(v complex64) {
	enc.addIndex()
	enc.appendComplex(complex128(v), 32)
}

func (enc *logfmtishEncoder) appendFloat(val float64, bitSize int) {
	switch {
	case math.IsNaN(val):
		enc.buf.AppendString(`NaN`)
	case math.IsInf(val, 1):
		enc.buf.AppendString(`+Inf`)
	case math.IsInf(val, -1):
		enc.buf.AppendString(`-Inf`)
	default:
		enc.buf.AppendFloat(val, bitSize)
	}
}

func (enc *logfmtishEncoder) AppendFloat64(v float64) {
	enc.addIndex()
	enc.appendFloat(v, 64)
}

func (enc *logfmtishEncoder) AppendFloat32(v float32) {
	enc.addIndex()
	enc.appendFloat(float64(v), 32)
}

func (enc *logfmtishEncoder) appendInt(v int64) {
	enc.buf.AppendInt(v)
}

func (enc *logfmtishEncoder) AppendInt64(v int64) {
	enc.addIndex()
	enc.appendInt(v)
}

func (enc *logfmtishEncoder) AppendInt(v int)     { enc.AppendInt64(int64(v)) }
func (enc *logfmtishEncoder) AppendInt32(v int32) { enc.AppendInt64(int64(v)) }
func (enc *logfmtishEncoder) AppendInt16(v int16) { enc.AppendInt64(int64(v)) }
func (enc *logfmtishEncoder) AppendInt8(v int8)   { enc.AppendInt64(int64(v)) }

func (enc *logfmtishEncoder) AppendString(v string) {
	enc.addIndex()
	enc.buf.AppendString(v) // TODO: handle special characters
}

func (enc *logfmtishEncoder) AppendUint64(v uint64) {
	enc.addIndex()
	enc.buf.AppendUint(v)
}

func (enc *logfmtishEncoder) AppendUint(v uint)       { enc.AppendUint64(uint64(v)) }
func (enc *logfmtishEncoder) AppendUint32(v uint32)   { enc.AppendUint64(uint64(v)) }
func (enc *logfmtishEncoder) AppendUint16(v uint16)   { enc.AppendUint64(uint64(v)) }
func (enc *logfmtishEncoder) AppendUint8(v uint8)     { enc.AppendUint64(uint64(v)) }
func (enc *logfmtishEncoder) AppendUintptr(v uintptr) { enc.AppendUint64(uint64(v)) }

func (enc *logfmtishEncoder) AppendDuration(v time.Duration) {
	enc.addIndex()
	// TODO: customizable encoder
	enc.AppendInt64(v.Milliseconds())
}

func (enc *logfmtishEncoder) AppendTime(v time.Time) {
	enc.addIndex()
	// TODO: customizable encoder
	enc.AppendInt64(v.UnixMilli())
}

func (enc *logfmtishEncoder) AppendArray(v ArrayMarshaler) error {
	arrenc := logfmtishEncoder{ctx: bufferpool.Get(), buf: enc.buf}
	arrenc.ctx.Write(enc.ctx.Bytes())
	arrenc.ctx.AppendByte('[')
	arrenc.ctx.AppendInt(enc.idx)
	arrenc.ctx.AppendByte(']')
	return v.MarshalLogArray(&arrenc)
}

func (enc *logfmtishEncoder) AppendObject(v ObjectMarshaler) error {
	objenc := logfmtishEncoder{ctx: bufferpool.Get(), buf: enc.buf}
	objenc.ctx.Write(enc.ctx.Bytes())
	objenc.ctx.AppendByte('[')
	objenc.ctx.AppendInt(enc.idx)
	objenc.ctx.AppendByte(']')
	return v.MarshalLogObject(&objenc)
}

func (enc *logfmtishEncoder) AppendReflected(x interface{}) error {
	if x == nil {
		enc.buf.AppendString("null")
		return nil
	}

	v := reflect.ValueOf(x)
	return getReflectMarshaler(v.Type(), false /* match interface */).AppendTo(enc, v)
}

// reflectMarshaler marshals a reflected value
// into an ObjectEncoder or an ArrayEncoder.
type reflectMarshaler interface {
	// AddTo adds value to the ObjectEncoder under the given name.
	AddTo(enc ObjectEncoder, name string, v reflect.Value) error

	// AppendTo appends value to the ArrayEncoder.
	AppendTo(enc ArrayEncoder, v reflect.Value) error
}

var (
	// Holds reflectMarshalers that marshal by the Kind of value being written.
	// This covers a majority of the cases for reflected types.
	//
	// These are stored by index of the Kind value.
	_kindReflectMarshalers []reflectMarshaler

	_typeOfByteSlice           = reflect.TypeOf([]byte(nil))
	_byteSliceReflectMarshaler = &simpleReflectMarshaler[[]byte]{
		get: func(v reflect.Value) []byte {
			return v.Interface().([]byte)
		},
		addTo:    (ObjectEncoder).AddByteString,
		appendTo: (ArrayEncoder).AppendByteString,
	}

	_typeOfObjectMarshaler           = reflect.TypeOf((*ObjectMarshaler)(nil)).Elem()
	_objectMarshalerReflectMarshaler = &fallibleReflectMarshaler[ObjectMarshaler]{
		get: func(v reflect.Value) ObjectMarshaler {
			return v.Interface().(ObjectMarshaler)
		},
		addTo: func(enc ObjectEncoder, name string, m ObjectMarshaler) error {
			return enc.AddObject(name, m)
		},
		appendTo: func(enc ArrayEncoder, m ObjectMarshaler) error {
			return enc.AppendObject(m)
		},
	}

	_typeOfArrayMarshaler           = reflect.TypeOf((*ArrayMarshaler)(nil)).Elem()
	_arrayMarshalerReflectMarshaler = &fallibleReflectMarshaler[ArrayMarshaler]{
		get: func(v reflect.Value) ArrayMarshaler {
			return v.Interface().(ArrayMarshaler)
		},
		addTo:    (ObjectEncoder).AddArray,
		appendTo: (ArrayEncoder).AppendArray,
	}
)

// getReflectMarshaler returns the reflectMarshaler for the given type.
func getReflectMarshaler(t reflect.Type, matchIface bool) (m reflectMarshaler) {
	// TODO: meaningful error objects?

	// Interfacing matching is conditional because
	// the entry-point to this function is AddReflected;
	// we've already checked for interface compliance by that point.
	// This should use the reflected routes first,
	// but recursive calls via structs and other values it encounters
	// should match the ObjectMarshaler or ArrayMarshaler interfaces.
	if matchIface {
		// TODO: Handle case when PointerTo(t) implements the interfaces.
		if t.Implements(_typeOfObjectMarshaler) {
			return _objectMarshalerReflectMarshaler
		}

		if t.Implements(_typeOfArrayMarshaler) {
			return _arrayMarshalerReflectMarshaler
		}
	}

	if k := t.Kind(); int(k) < len(_kindReflectMarshalers) {
		m = _kindReflectMarshalers[int(k)]
	}
	if m != nil {
		return m
	}

	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		if t == _typeOfByteSlice {
			return _byteSliceReflectMarshaler
		}

		elem := getReflectMarshaler(t.Elem(), true)
		if elem == nil {
			panic("TODO: return error")
		}

		return &fallibleReflectMarshaler[ArrayMarshaler]{
			get: func(v reflect.Value) ArrayMarshaler {
				return &reflectArrayMarshaler{
					elem: elem,
					v:    v,
				}
			},
			addTo:    (ObjectEncoder).AddArray,
			appendTo: (ArrayEncoder).AppendArray,
		}
	}

	// TODO: map marshaler should support any scalar or TextMarshaler key.

	// TODO: map and interface marshalers
	// TODO: struct marshalers when cached by type

	return nil
}

// Specifies the reflectMarshaler for the given kinds.
func setKindReflectMarshaler(m reflectMarshaler, kinds ...reflect.Kind) {
	// Ensure there's enough room.
	for _, k := range kinds {
		if len(_kindReflectMarshalers) < int(k)+1 {
			ms := make([]reflectMarshaler, int(k)*2)
			copy(ms, _kindReflectMarshalers)
		}
		_kindReflectMarshalers[int(k)] = m
	}
}

func init() {
	setKindReflectMarshaler(&simpleReflectMarshaler[bool]{
		get:      (reflect.Value).Bool,
		addTo:    (ObjectEncoder).AddBool,
		appendTo: (ArrayEncoder).AppendBool,
	}, reflect.Bool)
	setKindReflectMarshaler(&simpleReflectMarshaler[int64]{
		get:      (reflect.Value).Int,
		addTo:    (ObjectEncoder).AddInt64,
		appendTo: (ArrayEncoder).AppendInt64,
	}, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64)
	setKindReflectMarshaler(&simpleReflectMarshaler[uint64]{
		get:      (reflect.Value).Uint,
		addTo:    (ObjectEncoder).AddUint64,
		appendTo: (ArrayEncoder).AppendUint64,
	}, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64)
	setKindReflectMarshaler(&simpleReflectMarshaler[uintptr]{
		get:      (reflect.Value).Pointer,
		addTo:    (ObjectEncoder).AddUintptr,
		appendTo: (ArrayEncoder).AppendUintptr,
	}, reflect.Uintptr)
	setKindReflectMarshaler(&simpleReflectMarshaler[float64]{
		get:      (reflect.Value).Float,
		addTo:    (ObjectEncoder).AddFloat64,
		appendTo: (ArrayEncoder).AppendFloat64,
	}, reflect.Float32, reflect.Float64)
	setKindReflectMarshaler(&simpleReflectMarshaler[complex128]{
		get:      (reflect.Value).Complex,
		addTo:    (ObjectEncoder).AddComplex128,
		appendTo: (ArrayEncoder).AppendComplex128,
	}, reflect.Complex64, reflect.Complex128)
	setKindReflectMarshaler(&ifaceReflectMarshaler{}, reflect.Interface)
	setKindReflectMarshaler(&simpleReflectMarshaler[string]{
		get:      (reflect.Value).String,
		addTo:    (ObjectEncoder).AddString,
		appendTo: (ArrayEncoder).AppendString,
	}, reflect.String)

	// TODO: struct marshaler should be cached per type.
	setKindReflectMarshaler(&fallibleReflectMarshaler[ObjectMarshaler]{
		get: func(v reflect.Value) ObjectMarshaler {
			return &reflectStructMarshaler{v}
		},
		addTo:    (ObjectEncoder).AddObject,
		appendTo: (ArrayEncoder).AppendObject,
	}, reflect.Struct)
}

// simpleReflectMarshaler is a reflectMarshaler
// for values that can be marshalled without any error.
// It's parameterized over the kind of value it marshals.
type simpleReflectMarshaler[T any] struct {
	// Retrieves the native value from the reflected value.
	// Typically, this will be an unbound method reference
	// in the form,
	//
	//	(reflect.Value).Bool
	get func(reflect.Value) T

	// Adds the value returned by get to the ObjectEncoder.
	// Typically, this will be an unbound method reference
	// in the form,
	//
	//	(ObjectEncoder).AddBool
	addTo func(ObjectEncoder, string, T)

	// Appends the value returned by get to the ArrayEncoder.
	// Typically, this will be an unbound method reference
	// in the form,
	//
	//	(ArrayEncoder).AppendBool
	appendTo func(ArrayEncoder, T)
}

var _ reflectMarshaler = (*simpleReflectMarshaler[any])(nil)

func (m *simpleReflectMarshaler[T]) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	m.addTo(enc, name, m.get(v))
	return nil
}

func (m *simpleReflectMarshaler[T]) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	m.appendTo(enc, m.get(v))
	return nil
}

// fallibleReflectMarshaler is a reflectMarshaler
// for values that could fail to marshal -- structs and arrays.
type fallibleReflectMarshaler[T any] struct {
	// Retrieves the native value from the reflected value.
	get func(reflect.Value) T

	// Adds the value returned by get to the ObjectEncoder.
	addTo func(ObjectEncoder, string, T) error

	// Appends the value returned by get to the ArrayEncoder.
	appendTo func(ArrayEncoder, T) error
}

var _ reflectMarshaler = (*fallibleReflectMarshaler[any])(nil)

func (m *fallibleReflectMarshaler[T]) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	return m.addTo(enc, name, m.get(v))
}

func (m *fallibleReflectMarshaler[T]) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	return m.appendTo(enc, m.get(v))
}

// ifaceReflectMarshaler is a reflectMarshaler that inspects the type
// of the value to delegate to the appropriate reflectMarshaler.
type ifaceReflectMarshaler struct{}

var _ reflectMarshaler = (*ifaceReflectMarshaler)(nil)

func (*ifaceReflectMarshaler) AddTo(enc ObjectEncoder, k string, v reflect.Value) error {
	v = v.Elem()
	m := getReflectMarshaler(v.Type(), true /* match iface */)
	return m.AddTo(enc, k, v)
}

func (*ifaceReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	v = v.Elem()
	m := getReflectMarshaler(v.Type(), true /* match iface */)
	return m.AppendTo(enc, v)
}

// reflectArrayMarshaler is a Zap ArrayMarshaler
// that encodes its elements using a reflectMarshaler.
type reflectArrayMarshaler struct {
	elem reflectMarshaler // element marshaler
	v    reflect.Value    // slice or array
}

var _ ArrayMarshaler = (*reflectArrayMarshaler)(nil)

func (m *reflectArrayMarshaler) MarshalLogArray(enc ArrayEncoder) error {
	v := m.v
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if err := m.elem.AppendTo(enc, item); err != nil {
			return err
		}
	}
	return nil
}

type reflectStructMarshaler struct{ v reflect.Value }

// TODO: This should be a cached, reflection-based ObjectMarshaler.

func (m *reflectStructMarshaler) MarshalLogObject(enc ObjectEncoder) error {
	// TODO: Look at "json" tags, cache that information somewhere.
	v := m.v
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		field := v.Field(i)
		fieldm := getReflectMarshaler(f.Type, true)
		if fieldm == nil {
			return fmt.Errorf("unsupported type: %v", f.Type.Kind())
		}

		if err := fieldm.AddTo(enc, f.Name, field); err != nil {
			return err
		}
	}
	return nil
}
