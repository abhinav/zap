// Copyright (c) 2023 Uber Technologies, Inc.
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
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ReflectedEncoder serializes log fields that can't be serialized with Zap's
// JSON encoder. These have the ReflectType field type.
// Use EncoderConfig.NewReflectedEncoder to set this.
type ReflectedEncoder interface {
	// Encode encodes and writes to the underlying data stream.
	Encode(interface{}) error
}

func defaultReflectedEncoder(w io.Writer) ReflectedEncoder {
	enc := json.NewEncoder(w)
	// For consistency with our custom JSON encoder.
	enc.SetEscapeHTML(false)
	return enc
}

// reflectMarshaler marshals a value into an ObjectEncoder or an ArrayEncoder
// using reflection.
//
// This provides a Zap-native means of converting an arbitrary value
// into a series of calls on an ObjectEncoder or an ArrayEncoder.
//
// To avoid confusion with the ArrayMarshaler and ObjectMarshaler interfaces,
// all reflectMarshalers are named in the pattern "XReflectMarshaler".
// For example, the reflectMarshaler for ArrayMarshalers is named
// arrayMarshalerReflectMarshaler.
type reflectMarshaler interface {
	// AddTo adds value to the ObjectEncoder under the given name.
	//
	// It MUST NOT call AddReflected on the ObjectEncoder.
	AddTo(enc ObjectEncoder, name string, v reflect.Value) error

	// AppendTo appends value to the ArrayEncoder.
	//
	// It MUST NOT call AppendReflected on the ArrayEncoder.
	AppendTo(enc ArrayEncoder, v reflect.Value) error
}

var _reflectMarshalerCache sync.Map // reflect.Type => reflectMarshaler

// reflectMarshalerFor returns a reflectMarshaler for the given type.
func reflectMarshalerFor(t reflect.Type) reflectMarshaler {
	if m, ok := _reflectMarshalerCache.Load(t); ok {
		return m.(reflectMarshaler)
	}

	// Before we start building the marshaler,
	// place an indirect reference to it in the cache.
	// That will break the infinite loop if the type is self-referential.
	// This relies on the guarantee that
	// AddTo and AppendTo will not be called
	// until the marshaler is fully constructed.
	var m reflectMarshaler
	if o, ok := _reflectMarshalerCache.LoadOrStore(t, &indirectReflectMarshaler{m: &m}); ok {
		// Another goroutine already placed a marshaler in the cache.
		// It may or may not be indirect.
		return o.(reflectMarshaler)
	}

	m = buildReflectMarshaler(t)
	// Unconditionally replace the indirect reference with the real marshaler.
	// It's fine if multiple goroutines race to do this;
	// the result is the same
	_reflectMarshalerCache.Store(t, m)
	return m
}

var (
	_typeOfObjectMarshaler = reflect.TypeOf((*ObjectMarshaler)(nil)).Elem()
	_typeOfArrayMarshaler  = reflect.TypeOf((*ArrayMarshaler)(nil)).Elem()
	_typeOfTextMarshaler   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

// buildReflectMarshaler builds and returns a reflectMarshaler for a value
// of the given type.
// Don't use this directly. Use reflectMarshalerFor instead.
//
// The returned Marshaler will prefer
// ObjectMarshaler, ArrayMarshaler, or TextMarshaler implementations
// if available.
func buildReflectMarshaler(t reflect.Type) (m reflectMarshaler) {
	defer func() {
		if t.Kind() == reflect.Pointer {
			return
		}

		// If the pointer to the type implements the interfaces,
		// prefer that over the value type.
		// This avoids the allocation from converting
		// the value to an interface.
		addrm := matchIfaceReflectMarshaler(reflect.PointerTo(t))
		if addrm != nil {
			m = &canAddrReflectMarshaler{
				addrm: addrm,
				valm:  m,
			}
		}
	}()

	m = matchIfaceReflectMarshaler(t)
	if m != nil {
		return m
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolReflectMarshaler
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intReflectMarshaler
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintReflectMarshaler
	case reflect.Uintptr:
		return uintptrReflectMarshaler
	case reflect.Float32, reflect.Float64:
		return floatReflectMarshaler
	case reflect.Complex64, reflect.Complex128:
		return complexReflectMarshaler
	case reflect.String:
		return stringReflectMarshaler
	case reflect.Interface:
		return _ifaceReflectMarshaler
	case reflect.Pointer:
		return &ptrReflectMarshaler{
			elem: reflectMarshalerFor(t.Elem()),
		}

	case reflect.Slice, reflect.Array:
		if el := t.Elem(); el.Kind() == reflect.Uint8 {
			// Special-case []byte to encode as base64.
			return byteSliceReflectMarshaler
		}

		el := reflectMarshalerFor(t.Elem())
		return newArrayMarshalerReflectMarshaler(func(v reflect.Value) ArrayMarshaler {
			// TODO: pre-compile "[]E => ArrayMarshaler" function
			// and use that here.
			return newReflectedSliceArrayMarshaler(el, v)
		})

	case reflect.Map:
		return newMapReflectMarshaler(t)

	case reflect.Struct:
		return newStructReflectMarshaler(t)

	}
	return &unsupportedTypeReflectMarshaler{t: t}
}

// Matches the given type against supported marshaling interfaces
// and returns a reflectMarshaler for that case.
func matchIfaceReflectMarshaler(t reflect.Type) reflectMarshaler {
	switch {
	case t.Implements(_typeOfObjectMarshaler):
		return defaultObjectMarshalerReflectMarshaler
	case t.Implements(_typeOfArrayMarshaler):
		return defaultArrayMarshalerReflectMarshaler
	case t.Implements(_typeOfTextMarshaler):
		return defaultTextMarshalerReflectMarshaler
	}
	return nil
}

// unsupportedTypeReflectMarshaler is a reflectMarshaler
// returned for types that cannot be marshaled to a Zap encoder.
type unsupportedTypeReflectMarshaler struct{ t reflect.Type }

var _ reflectMarshaler = (*unsupportedTypeReflectMarshaler)(nil)

func (m *unsupportedTypeReflectMarshaler) AddTo(ObjectEncoder, string, reflect.Value) error {
	return fmt.Errorf("unsupported type: %v", m.t)
}

func (m *unsupportedTypeReflectMarshaler) AppendTo(ArrayEncoder, reflect.Value) error {
	return fmt.Errorf("unsupported type: %v", m.t)
}

// simpleReflectMarshaler helps build stateless reflectMarshalers
// where two function references are sufficient.
//
// Prefer to build concrete types if the marshaler needs to capture
// variables from the surrounding scope.
type simpleReflectMarshaler struct {
	addTo    func(ObjectEncoder, string, reflect.Value) error
	appendTo func(ArrayEncoder, reflect.Value) error
}

var _ reflectMarshaler = (*simpleReflectMarshaler)(nil)

func (f *simpleReflectMarshaler) AddTo(enc ObjectEncoder, k string, v reflect.Value) error {
	return f.addTo(enc, k, v)
}

func (f *simpleReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	return f.appendTo(enc, v)
}

// primitiveReflectMarshaler is a reflectMarshaler
// for values that can be marshalled without an error.
// It's parameterized over the kind of value it marshals.
type primitiveReflectMarshaler[T any] struct {
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

var _ reflectMarshaler = (*primitiveReflectMarshaler[any])(nil)

func (m *primitiveReflectMarshaler[T]) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	m.addTo(enc, name, m.get(v))
	return nil
}

func (m *primitiveReflectMarshaler[T]) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	m.appendTo(enc, m.get(v))
	return nil
}

// Primitive marshalers based on primitiveReflectMarshaler.
//
// For numbers, higher-precision variants are used for all cases.
// e.g., int64 is used for int8, int16, and int32.
var (
	boolReflectMarshaler = &primitiveReflectMarshaler[bool]{
		get:      (reflect.Value).Bool,
		addTo:    (ObjectEncoder).AddBool,
		appendTo: (ArrayEncoder).AppendBool,
	}
	intReflectMarshaler = &primitiveReflectMarshaler[int64]{
		get:      (reflect.Value).Int,
		addTo:    (ObjectEncoder).AddInt64,
		appendTo: (ArrayEncoder).AppendInt64,
	}
	uintReflectMarshaler = &primitiveReflectMarshaler[uint64]{
		get:      (reflect.Value).Uint,
		addTo:    (ObjectEncoder).AddUint64,
		appendTo: (ArrayEncoder).AppendUint64,
	}
	uintptrReflectMarshaler = &primitiveReflectMarshaler[uintptr]{
		get:      (reflect.Value).Pointer,
		addTo:    (ObjectEncoder).AddUintptr,
		appendTo: (ArrayEncoder).AppendUintptr,
	}
	floatReflectMarshaler = &primitiveReflectMarshaler[float64]{
		get:      (reflect.Value).Float,
		addTo:    (ObjectEncoder).AddFloat64,
		appendTo: (ArrayEncoder).AppendFloat64,
	}
	complexReflectMarshaler = &primitiveReflectMarshaler[complex128]{
		get:      (reflect.Value).Complex,
		addTo:    (ObjectEncoder).AddComplex128,
		appendTo: (ArrayEncoder).AppendComplex128,
	}
	stringReflectMarshaler = &primitiveReflectMarshaler[string]{
		get:      (reflect.Value).String,
		addTo:    (ObjectEncoder).AddString,
		appendTo: (ArrayEncoder).AppendString,
	}

	byteSliceReflectMarshaler = &primitiveReflectMarshaler[[]byte]{
		get: func(v reflect.Value) []byte {
			return v.Interface().([]byte)
		},
		addTo: (ObjectEncoder).AddBinary,
		appendTo: func(ae ArrayEncoder, b []byte) {
			// ArrayEncoder is missing AppendBinary.
			ae.AppendString(base64.StdEncoding.EncodeToString(b))
		},
	}
)

// indirectReflectMarshaler is a reflectMarshaler that
// dereferences the value before calling the underlying marshaler.
//
// This is used for self-referential types.
type indirectReflectMarshaler struct {
	m *reflectMarshaler
}

var _ reflectMarshaler = (*indirectReflectMarshaler)(nil)

func (m *indirectReflectMarshaler) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	return (*m.m).AddTo(enc, name, v.Elem())
}

func (m *indirectReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	return (*m.m).AppendTo(enc, v.Elem())
}

// canAddrReflectMarshaler is a reflectMarshaler
// that uses the Addr marshaler if the value is addressable (CanAddr()),
// and the Val marshaler otherwise.
type canAddrReflectMarshaler struct {
	addrm reflectMarshaler
	valm  reflectMarshaler
}

var _ reflectMarshaler = (*canAddrReflectMarshaler)(nil)

func (m *canAddrReflectMarshaler) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	if v.CanAddr() {
		return m.addrm.AddTo(enc, name, v.Addr())
	}
	return m.valm.AddTo(enc, name, v)
}

func (m *canAddrReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	if v.CanAddr() {
		return m.addrm.AppendTo(enc, v.Addr())
	}
	return m.valm.AppendTo(enc, v)
}

// reflectMarshaler for types that implement ArrayMarshaler directly.
var defaultArrayMarshalerReflectMarshaler = newArrayMarshalerReflectMarshaler(func(v reflect.Value) ArrayMarshaler {
	return v.Interface().(ArrayMarshaler)
})

// newArrayMarshalerReflectMarshaler builds a reflectMarshaler that encodes
// ArrayMarshalers.
//
// This may be used for types that implement ArrayMarshaler,
// or those that can be adapted to ArrayMarshaler,
// e.g. with [reflectedSliceArrayMarshaler].
func newArrayMarshalerReflectMarshaler(toArray func(reflect.Value) ArrayMarshaler) reflectMarshaler {
	return &compositeReflectMarshaler[ArrayMarshaler]{
		get: func(v reflect.Value) ArrayMarshaler {
			return toArray(v)
		},
		addTo:    (ObjectEncoder).AddArray,
		appendTo: (ArrayEncoder).AppendArray,
	}
}

// reflectedSliceArrayMarshaler is a Zap ArrayMarshaler
// that encodes the elements of a reflected slice or array.
type reflectedSliceArrayMarshaler struct {
	elem reflectMarshaler // element marshaler
	v    reflect.Value    // slice or array
}

var _ ArrayMarshaler = (*reflectedSliceArrayMarshaler)(nil)

// arrayMarshaler builds a Zap arrayMarshaler from a reflected slice or array.
func newReflectedSliceArrayMarshaler(el reflectMarshaler, v reflect.Value) ArrayMarshaler {
	return &reflectedSliceArrayMarshaler{elem: el, v: v}
}

func (m *reflectedSliceArrayMarshaler) MarshalLogArray(enc ArrayEncoder) error {
	v := m.v
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if err := m.elem.AppendTo(enc, item); err != nil {
			return err
		}
	}
	return nil
}

// reflectMarshaler for types that implement ObjectMarshaler directly.
var defaultObjectMarshalerReflectMarshaler = newObjectMarshalerReflectMarshaler(func(v reflect.Value) ObjectMarshaler {
	return v.Interface().(ObjectMarshaler)
})

// newObjectMarshalerReflectMarshaler builds a reflectMarshaler that encodes
// ObjectMarshalers
//
// This may be used for types that implement ObjectMarshaler
// or those that can be adapted into one.
func newObjectMarshalerReflectMarshaler(toObject func(reflect.Value) ObjectMarshaler) reflectMarshaler {
	return &compositeReflectMarshaler[ObjectMarshaler]{
		get: func(v reflect.Value) ObjectMarshaler {
			return toObject(v)
		},
		addTo:    (ObjectEncoder).AddObject,
		appendTo: (ArrayEncoder).AppendObject,
	}
}

// newMapReflectMarshaler builds a reflectMarshaler for the given map type.
func newMapReflectMarshaler(t reflect.Type) reflectMarshaler {
	var encodeKey func(reflect.Value) (string, error)
	switch kt := t.Key(); kt.Kind() {
	case reflect.String:
		encodeKey = func(v reflect.Value) (string, error) {
			return v.String(), nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		encodeKey = func(v reflect.Value) (string, error) {
			return strconv.FormatInt(v.Int(), 10), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		encodeKey = func(v reflect.Value) (string, error) {
			return strconv.FormatUint(v.Uint(), 10), nil
		}
	default:
		if !kt.Implements(_typeOfTextMarshaler) {
			return &unsupportedTypeReflectMarshaler{t: t}
		}
		encodeKey = func(v reflect.Value) (string, error) {
			bs, err := v.Interface().(encoding.TextMarshaler).MarshalText()
			return string(bs), err
		}
	}

	val := reflectMarshalerFor(t.Elem())
	return newObjectMarshalerReflectMarshaler(func(v reflect.Value) ObjectMarshaler {
		return &reflectedMapObjectMarshaler{
			key: encodeKey,
			val: val,
			v:   v,
		}
	})
}

// reflectedMapObjectMarshaler is a Zap ObjectMarshaler
// that encodes the elements of a reflected map.
type reflectedMapObjectMarshaler struct {
	key func(reflect.Value) (string, error)
	val reflectMarshaler

	v reflect.Value
}

var _ ObjectMarshaler = (*reflectedMapObjectMarshaler)(nil)

func (m *reflectedMapObjectMarshaler) MarshalLogObject(enc ObjectEncoder) error {
	iter := m.v.MapRange()
	for iter.Next() {
		k, err := m.key(iter.Key())
		if err != nil {
			return err
		}
		if err := m.val.AddTo(enc, k, iter.Value()); err != nil {
			return err
		}
	}
	return nil
}

type structFieldMarshaler struct {
	name  string
	index []int
	m     reflectMarshaler

	omitempty bool
}

// AddTo adds the value for this field to the encoder,
// extracting it from the given reflected struct.
func (f *structFieldMarshaler) AddTo(enc ObjectEncoder, v reflect.Value) error {
	// The following block is equivalent to v.FieldByIndex(f.index),
	// except instead of panicking on embedded nil values,
	// it'll skip the field.
	fv := v
	for _, i := range f.index {
		if fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				// Embedded pointer struct is nil.
				// Ignore.
				return nil
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}

	if f.omitempty && fv.IsZero() {
		return nil
	}

	return f.m.AddTo(enc, f.name, fv)
}

// newStructReflectMarshaler builds a reflectMarshaler to encode arbitrary
// structs.
func newStructReflectMarshaler(t reflect.Type) reflectMarshaler {
	var fields []structFieldMarshaler
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		// TODO: embedded fields
		if !sf.IsExported() {
			continue
		}

		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}

		var (
			name      string
			omitempty bool
		)
		if toks := strings.Split(tag, ","); len(toks) > 0 {
			// TODO: only valid names
			name = toks[0]
			for _, opt := range toks[1:] {
				if opt == "omitempty" {
					omitempty = true
					break
				}
			}
		}

		if name == "" {
			name = sf.Name
		}

		field := structFieldMarshaler{
			name:      name,
			index:     []int{i}, // TODO
			m:         reflectMarshalerFor(sf.Type),
			omitempty: omitempty,
		}
		fields = append(fields, field)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].name < fields[j].name
	})

	return newObjectMarshalerReflectMarshaler(func(v reflect.Value) ObjectMarshaler {
		return &reflectedStructObjectMarshaler{
			fields: fields,
			v:      v,
		}
	})
}

// reflectedStructObjectMarshaler adapts a reflected struct
// into an ObjectMarshaler.
type reflectedStructObjectMarshaler struct {
	fields []structFieldMarshaler
	v      reflect.Value
}

var _ ObjectMarshaler = (*reflectedStructObjectMarshaler)(nil)

func (m *reflectedStructObjectMarshaler) MarshalLogObject(enc ObjectEncoder) error {
	for _, f := range m.fields {
		if err := f.AddTo(enc, m.v); err != nil {
			return err
		}
	}
	return nil
}

// compositeReflectMarshaler is a reflectMarshaler
// for values that could fail to marshal: objects and arrays.
// It's parameterized over the kind of value it marshals.
//
// This is the same as primitiveReflectMarshaler,
// except addTo and appendTo can return an error.
type compositeReflectMarshaler[T any] struct {
	// Retrieves the native value from the reflected value.
	get func(reflect.Value) T

	// Adds the value returned by get to the ObjectEncoder.
	addTo func(ObjectEncoder, string, T) error

	// Appends the value returned by get to the ArrayEncoder.
	appendTo func(ArrayEncoder, T) error
}

var _ reflectMarshaler = (*compositeReflectMarshaler[any])(nil)

func (m *compositeReflectMarshaler[T]) AddTo(enc ObjectEncoder, name string, v reflect.Value) error {
	return m.addTo(enc, name, m.get(v))
}

func (m *compositeReflectMarshaler[T]) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	return m.appendTo(enc, m.get(v))
}

// ifaceReflectMarshaler is a reflectMarshaler that inspects the type
// of the value to delegate to the appropriate reflectMarshaler.
type ifaceReflectMarshaler struct{}

var _ifaceReflectMarshaler reflectMarshaler = &ifaceReflectMarshaler{}

func (*ifaceReflectMarshaler) AddTo(enc ObjectEncoder, k string, v reflect.Value) error {
	if v.IsNil() {
		return enc.AddReflected(k, nil) // TODO: AddNull
	}
	v = v.Elem()
	return reflectMarshalerFor(v.Type()).AddTo(enc, k, v)
}

func (*ifaceReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	if v.IsNil() {
		return enc.AppendReflected(nil) // TODO: AppendNull
	}
	v = v.Elem()
	return reflectMarshalerFor(v.Type()).AppendTo(enc, v)
}

type ptrReflectMarshaler struct{ elem reflectMarshaler }

var _ reflectMarshaler = (*ptrReflectMarshaler)(nil)

func (m *ptrReflectMarshaler) AddTo(enc ObjectEncoder, k string, v reflect.Value) error {
	if v.IsNil() {
		return enc.AddReflected(k, nil) // TODO: AddNull to the encoder
	}
	return m.elem.AddTo(enc, k, v.Elem())
}

func (m *ptrReflectMarshaler) AppendTo(enc ArrayEncoder, v reflect.Value) error {
	if v.IsNil() {
		return enc.AppendReflected(nil) // TODO: AppendNull to the encoder
	}
	return m.elem.AppendTo(enc, v.Elem())
}

// defaultTextMarshalerReflectMarshaler is a reflectMarshaler for types
// that implement encoding.TextMarshaler directly.
var defaultTextMarshalerReflectMarshaler = &compositeReflectMarshaler[encoding.TextMarshaler]{
	get: func(v reflect.Value) encoding.TextMarshaler {
		return v.Interface().(encoding.TextMarshaler)
	},
	addTo: func(enc ObjectEncoder, k string, v encoding.TextMarshaler) error {
		bs, err := v.MarshalText()
		if err != nil {
			return err
		}
		enc.AddBinary(k, bs)
		return nil
	},
	appendTo: func(enc ArrayEncoder, v encoding.TextMarshaler) error {
		bs, err := v.MarshalText()
		if err != nil {
			return err
		}
		enc.AppendByteString(bs)
		return nil
	},
}
