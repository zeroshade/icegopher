// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package expressions

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/zeroshade/icegopher"

	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/google/uuid"
)

type LiteralType interface {
	string | bool | int32 | float32 | int64 |
		float64 | []byte | uuid.UUID | decimal128.Num |
		icegopher.Time | icegopher.Date | icegopher.Timestamp
}

type OrderedNumericLiteralType interface {
	int32 | float32 | int64 | float64 | icegopher.Time | icegopher.Date | icegopher.Timestamp
}

func NewLiteral[L LiteralType](v L) Literal {
	switch v := any(v).(type) {
	case string:
		return StringLiteral(v)
	case bool:
		return BoolLiteral(v)
	case int32:
		return IntLiteral(v)
	case int64:
		return LongLiteral(v)
	case float32:
		return FloatLiteral(v)
	case float64:
		return DoubleLiteral(v)
	case []byte:
		return BytesLiteral(v)
	case uuid.UUID:
		return UUIDLiteral(v)
	case decimal128.Num:
		return DecimalLiteral(v)
	case icegopher.Time:
		return TimeLiteral(v)
	case icegopher.Date:
		return DateLiteral(v)
	case icegopher.Timestamp:
		return TimestampLiteral(v)
	}
	panic("unimplemented literal type")
}

var (
	ErrInvalidBinarySerialization = errors.New("invalid binary serialize")
)

func litFromBytes(typ icegopher.Type, data []byte) (Literal, error) {
	if data == nil {
		return nil, nil
	}

	switch typ.(type) {
	case icegopher.BooleanType:
		var v BoolLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.Int32Type:
		var v IntLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.Int64Type:
		var v LongLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.Float32Type:
		var v FloatLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.Float64Type:
		var v DoubleLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.BinaryType:
		return BytesLiteral(data), nil
	case icegopher.DateType:
		var v DateLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.DecimalType:
		var v DecimalLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.FixedType:
		var v FixedLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.TimeType:
		var v TimeLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.TimestampType:
		var v TimestampLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.TimestampTzType:
		var v TimestampLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	case icegopher.UUIDType:
		var v UUIDLiteral
		err := v.UnmarshalBinary(data)
		return v, err
	}

	return nil, nil
}

type Literal interface {
	literal()
}

type StringLiteral string

func (StringLiteral) literal()        {}
func (s StringLiteral) Value() string { return string(s) }
func (s *StringLiteral) UnmarshalBinary(data []byte) error {
	*s = StringLiteral(string(data))
	return nil
}

type BoolLiteral bool

func (BoolLiteral) literal()      {}
func (b BoolLiteral) Value() bool { return bool(b) }
func (b *BoolLiteral) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("%w: expected at least 1 byte for bool", ErrInvalidBinarySerialization)
	}
	*b = data[0] != 0
	return nil
}

type IntLiteral int32

func (IntLiteral) literal()       {}
func (i IntLiteral) Value() int32 { return int32(i) }
func (i *IntLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for int32 value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*i = IntLiteral(binary.LittleEndian.Uint32(data))
	return nil
}

type LongLiteral int64

func (LongLiteral) literal()       {}
func (l LongLiteral) Value() int64 { return int64(l) }
func (l *LongLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for int64 value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*l = LongLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

type FloatLiteral float32

func (FloatLiteral) literal()         {}
func (f FloatLiteral) Value() float32 { return float32(f) }
func (f *FloatLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for float value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*f = FloatLiteral(math.Float32frombits(binary.LittleEndian.Uint32(data)))
	return nil
}

type DoubleLiteral float64

func (DoubleLiteral) literal()         {}
func (d DoubleLiteral) Value() float64 { return float64(d) }
func (d *DoubleLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for double value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*d = DoubleLiteral(math.Float64frombits(binary.LittleEndian.Uint64(data)))
	return nil
}

type BytesLiteral []byte

func (BytesLiteral) literal()        {}
func (b BytesLiteral) Value() []byte { return []byte(b) }
func (b *BytesLiteral) UnmarshalBinary(data []byte) error {
	*b = BytesLiteral(data)
	return nil
}

type FixedLiteral []byte

func (FixedLiteral) literal()        {}
func (f FixedLiteral) Value() []byte { return []byte(f) }
func (f *FixedLiteral) UnmarshalBinary(data []byte) error {
	*f = FixedLiteral(data)
	return nil
}

type DateLiteral icegopher.Date

func (DateLiteral) literal()                {}
func (d DateLiteral) Value() icegopher.Date { return icegopher.Date(d) }
func (d *DateLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for date value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*d = DateLiteral(binary.LittleEndian.Uint32(data))
	return nil
}

type TimeLiteral icegopher.Time

func (TimeLiteral) literal()                {}
func (t TimeLiteral) Value() icegopher.Time { return icegopher.Time(t) }
func (t *TimeLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for time value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*t = TimeLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

type TimestampLiteral icegopher.Timestamp

func (TimestampLiteral) literal()                     {}
func (t TimestampLiteral) Value() icegopher.Timestamp { return icegopher.Timestamp(t) }
func (t *TimestampLiteral) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for timestamp value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*t = TimestampLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

type UUIDLiteral uuid.UUID

func (UUIDLiteral) literal()           {}
func (u UUIDLiteral) Value() uuid.UUID { return uuid.UUID(u) }
func (u *UUIDLiteral) UnmarshalBinary(data []byte) error {
	out, err := uuid.FromBytes(data)
	if err != nil {
		return err
	}
	*u = UUIDLiteral(out)
	return nil
}

type DecimalLiteral decimal128.Num

func (DecimalLiteral) literal()                {}
func (d DecimalLiteral) Value() decimal128.Num { return decimal128.Num(d) }
func (d *DecimalLiteral) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		*d = DecimalLiteral(decimal128.Num{})
		return nil
	}

	if int8(data[0]) >= 0 {
		*d = DecimalLiteral(decimal128.FromBigInt((&big.Int{}).SetBytes(data)))
		return nil
	}

	// convert 2's complement and remember it's negative
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = ^b
	}
	out[len(out)-1] += 1

	value := (&big.Int{}).SetBytes(out)
	*d = DecimalLiteral(decimal128.FromBigInt(value.Neg(value)))
	return nil
}

// func Cast[F, T LiteralType](from Literal[F]) (Literal[T], error) {}
