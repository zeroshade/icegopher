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
	"strconv"
	"time"

	"github.com/zeroshade/icegopher"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/google/uuid"
)

var (
	ErrInvalidCast = errors.New("invalid literal cast")
)

type LiteralType interface {
	string | bool | int32 | float32 | int64 |
		float64 | []byte | uuid.UUID | decimal128.Num |
		icegopher.Time | icegopher.Date | icegopher.Timestamp
}

type NumericLiteralType interface {
	int32 | float32 | int64 | float64 |
		icegopher.Time | icegopher.Date | icegopher.Timestamp
}

type OrderedNumericLiteralType interface {
	int32 | float32 | int64 | float64 | icegopher.Time |
		icegopher.Date | icegopher.Timestamp
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

func LiteralFromBytes(typ icegopher.Type, data []byte) (Literal, error) {
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
	case icegopher.StringType:
		var v StringLiteral
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
	case icegopher.TimestampType, icegopher.TimestampTzType:
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
	To(icegopher.Type) (Literal, error)
}

type IntAboveMax struct{}

func (IntAboveMax) String() string { return "Int32AboveMax" }
func (IntAboveMax) Value() int32   { return math.MaxInt32 }
func (IntAboveMax) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast IntAboveMax", ErrInvalidCast)
}

type IntBelowMin struct{}

func (IntBelowMin) String() string { return "Int32BelowMin" }
func (IntBelowMin) Value() int32   { return math.MinInt32 }
func (IntBelowMin) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast IntBelowMin", ErrInvalidCast)
}

type LongAboveMax struct{}

func (LongAboveMax) String() string { return "Int32AboveMax" }
func (LongAboveMax) Value() int64   { return math.MaxInt64 }
func (LongAboveMax) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast LongAboveMax", ErrInvalidCast)
}

type LongBelowMin struct{}

func (LongBelowMin) String() string { return "Int32BelowMin" }
func (LongBelowMin) Value() int64   { return math.MinInt64 }
func (LongBelowMin) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast LongBelowMin", ErrInvalidCast)
}

type FloatAboveMax struct{}

func (FloatAboveMax) String() string { return "Float32AboveMax" }
func (FloatAboveMax) Value() float32 { return math.MaxFloat32 }
func (FloatAboveMax) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast FloatAboveMax", ErrInvalidCast)
}

type FloatBelowMin struct{}

func (FloatBelowMin) String() string { return "Float32BelowMin" }
func (FloatBelowMin) Value() float32 { return -math.MaxFloat32 }
func (FloatBelowMin) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast FloatBelowMin", ErrInvalidCast)
}

type DoubleAboveMax struct{}

func (DoubleAboveMax) String() string { return "Float64AboveMax" }
func (DoubleAboveMax) Value() float64 { return math.MaxFloat64 }
func (DoubleAboveMax) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast DoubleAboveMax", ErrInvalidCast)
}

type DoubleBelowMin struct{}

func (DoubleBelowMin) String() string { return "Float64BelowMin" }
func (DoubleBelowMin) Value() float64 { return -math.MaxFloat64 }
func (DoubleBelowMin) To(icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: cannot cast DoubleBelowMin", ErrInvalidCast)
}

type StringLiteral string

func (s StringLiteral) Value() string { return string(s) }
func (s StringLiteral) MarshalBinary() (data []byte, err error) {
	// stored as UTF-8 bytes without length
	data = []byte(s)
	return
}

func (s *StringLiteral) UnmarshalBinary(data []byte) error {
	// stored as UTF-8 bytes without length
	*s = StringLiteral(string(data))
	return nil
}

func (s StringLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.StringType:
		return s, nil
	case icegopher.Int32Type:
		i, err := strconv.ParseInt(string(s), 10, 32)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if s[0] == '-' {
					return IntBelowMin{}, nil
				}
				return IntAboveMax{}, nil
			}
			break
		}
		return IntLiteral(i), nil
	case icegopher.Int64Type:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if s[0] == '-' {
					return LongBelowMin{}, nil
				}
				return LongAboveMax{}, nil
			}
			break
		}
		return LongLiteral(i), nil
	case icegopher.Float32Type:
		f, err := strconv.ParseFloat(string(s), 32)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if s[0] == '-' {
					return FloatBelowMin{}, nil
				}
				return FloatAboveMax{}, nil
			}
			break
		}
		return FloatLiteral(f), nil
	case icegopher.Float64Type:
		f, err := strconv.ParseFloat(string(s), 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if s[0] == '-' {
					return DoubleBelowMin{}, nil
				}
				return DoubleAboveMax{}, nil
			}
			break
		}
		return DoubleLiteral(f), nil
	case icegopher.DateType:
		tm, err := time.Parse("2006-01-02", string(s))
		if err != nil {
			break
		}
		return DateLiteral(arrow.Date32FromTime(tm)), nil
	case icegopher.TimeType:
		tm, err := arrow.Time64FromString(string(s), arrow.Microsecond)
		if err != nil {
			break
		}
		return TimeLiteral(tm), nil
	case icegopher.TimestampType:
		tm, err := time.Parse("2006-01-02T15:04:05.000000", string(s))
		if err != nil {
			break
		}
		ts, err := arrow.TimestampFromTime(tm, arrow.Microsecond)
		if err != nil {
			break
		}
		return TimestampLiteral(ts), nil
	case icegopher.TimestampTzType:
		tm, err := time.Parse("2006-01-02T15:04:05.000000-07:00", string(s))
		if err != nil {
			break
		}
		ts, err := arrow.TimestampFromTime(tm.UTC(), arrow.Microsecond)
		if err != nil {
			break
		}
		return TimestampLiteral(ts), nil
	case icegopher.UUIDType:
		u, err := uuid.Parse(string(s))
		if err != nil {
			break
		}
		return UUIDLiteral(u), nil
	case icegopher.DecimalType:
		n, err := decimal128.FromString(string(s), int32(t.Precision()), int32(t.Scale()))
		if err != nil {
			break
		}
		return DecimalLiteral(n), nil
	case icegopher.BooleanType:
		b, err := strconv.ParseBool(string(s))
		if err != nil {
			break
		}
		return BoolLiteral(b), nil
	}
	return nil, fmt.Errorf("%w: could not convert '%s' -> %s", ErrInvalidCast, s, t)
}

type BoolLiteral bool

func (b BoolLiteral) Value() bool { return bool(b) }

var (
	falseBin, trueBin = [1]byte{0x0}, [1]byte{0x1}
)

func (b BoolLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 0x00 for false and anything non-zero for True
	if b {
		return trueBin[:], nil
	}
	return falseBin[:], nil
}

func (b *BoolLiteral) UnmarshalBinary(data []byte) error {
	// stored as 0x00 for false and anything non-zero for True
	if len(data) < 1 {
		return fmt.Errorf("%w: expected at least 1 byte for bool", ErrInvalidBinarySerialization)
	}
	*b = data[0] != 0
	return nil
}

func (b BoolLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.BooleanType:
		return b, nil
	default:
		return nil, fmt.Errorf("%w: bool -> %s", ErrInvalidCast, t)
	}
}

type IntLiteral int32

func (i IntLiteral) Value() int32 { return int32(i) }

func (i IntLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 4 bytes in little endian order
	data = make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(i))
	return
}

func (i *IntLiteral) UnmarshalBinary(data []byte) error {
	// stored as 4 bytes in little endian order
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for int32 value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*i = IntLiteral(binary.LittleEndian.Uint32(data))
	return nil
}

func (i IntLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.Int32Type:
		return i, nil
	case icegopher.Int64Type:
		return LongLiteral(i), nil
	case icegopher.DateType:
		return DateLiteral(i), nil
	case icegopher.TimeType:
		return TimeLiteral(i), nil
	case icegopher.Float32Type:
		return FloatLiteral(i), nil
	case icegopher.Float64Type:
		return DoubleLiteral(i), nil
	case icegopher.DecimalType:

		return DecimalLiteral(decimal128.FromI64(int64(int32(i) * int32(math.Pow10(t.Scale()))))), nil
	default:
		return nil, fmt.Errorf("%w: int -> %s", ErrInvalidCast, t)
	}
}

type LongLiteral int64

func (l LongLiteral) Value() int64 { return int64(l) }

func (l LongLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 8 bytes in little-endian order
	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(l))
	return
}

func (l *LongLiteral) UnmarshalBinary(data []byte) error {
	// stored as 8 bytes in little-endian order
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for int64 value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*l = LongLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

func (l LongLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.Int32Type:
		if l < math.MinInt32 {
			return IntBelowMin{}, nil
		}
		if l > math.MaxInt32 {
			return IntAboveMax{}, nil
		}

		return IntLiteral(l), nil
	case icegopher.Int64Type:
		return l, nil
	case icegopher.DateType:
		return DateLiteral(l), nil
	case icegopher.TimeType:
		return TimeLiteral(l), nil
	case icegopher.Float32Type:
		return FloatLiteral(l), nil
	case icegopher.Float64Type:
		return DoubleLiteral(l), nil
	case icegopher.DecimalType:
		return DecimalLiteral(decimal128.FromI64(int64(l) * int64(math.Pow10(t.Scale())))), nil
	default:
		return nil, fmt.Errorf("%w: long -> %s", ErrInvalidCast, t)
	}
}

type FloatLiteral float32

func (f FloatLiteral) Value() float32 { return float32(f) }

func (f FloatLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 4 bytes in little endian order
	data = make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(f)))
	return
}

func (f *FloatLiteral) UnmarshalBinary(data []byte) error {
	// stored as 4 bytes in little endian order
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for float value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*f = FloatLiteral(math.Float32frombits(binary.LittleEndian.Uint32(data)))
	return nil
}

func (f FloatLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.Float32Type:
		return f, nil
	case icegopher.Float64Type:
		return DoubleLiteral(f), nil
	case icegopher.DecimalType:
		n, err := decimal128.FromFloat32(float32(f), int32(t.Precision()), int32(t.Scale()))
		if err != nil {
			return nil, err
		}
		return DecimalLiteral(n), nil
	default:
		return nil, fmt.Errorf("%w: float -> %s", ErrInvalidCast, t)
	}
}

type DoubleLiteral float64

func (d DoubleLiteral) Value() float64 { return float64(d) }

func (d DoubleLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 8 bytes in little-endian order
	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(float64(d)))
	return
}

func (d *DoubleLiteral) UnmarshalBinary(data []byte) error {
	// stored as 8 bytes in little-endian order
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for double value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*d = DoubleLiteral(math.Float64frombits(binary.LittleEndian.Uint64(data)))
	return nil
}

func (d DoubleLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.Float32Type:
		if d < -math.MaxFloat32 {
			return FloatBelowMin{}, nil
		}
		if d > math.MaxFloat32 {
			return FloatAboveMax{}, nil
		}

		return FloatLiteral(d), nil
	case icegopher.Float64Type:
		return d, nil
	case icegopher.DecimalType:
		n, err := decimal128.FromFloat64(float64(d), int32(t.Precision()), int32(t.Scale()))
		if err != nil {
			return nil, err
		}
		return DecimalLiteral(n), nil
	default:
		return nil, fmt.Errorf("%w: double -> %s", ErrInvalidCast, t)
	}
}

type BytesLiteral []byte

func (b BytesLiteral) Value() []byte { return []byte(b) }

func (b BytesLiteral) MarshalBinary() (data []byte, err error) {
	// stored directly as is
	data = b
	return
}

func (b *BytesLiteral) UnmarshalBinary(data []byte) error {
	// stored directly as is
	*b = BytesLiteral(data)
	return nil
}

func (b BytesLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.BinaryType:
		return b, nil
	case icegopher.FixedType:
		if len(b) != t.Len() {
			return nil, fmt.Errorf("%w: binary -> %s, different length: %d <> %d",
				ErrInvalidCast, t, len(b), t.Len())
		}
		return FixedLiteral(b), nil
	default:
		return nil, fmt.Errorf("%w: binary -> %s", ErrInvalidCast, t)
	}
}

type FixedLiteral []byte

func (f FixedLiteral) Value() []byte { return []byte(f) }

func (f FixedLiteral) MarshalBinary() (data []byte, err error) {
	// stored directly as is
	data = f
	return
}

func (f *FixedLiteral) UnmarshalBinary(data []byte) error {
	// stored directly as is
	*f = FixedLiteral(data)
	return nil
}

func (f FixedLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.BinaryType:
		return BytesLiteral(f), nil
	case icegopher.FixedType:
		if len(f) != t.Len() {
			return nil, fmt.Errorf("%w: fixed[%d] -> %s, different length: %d <> %d",
				ErrInvalidCast, len(f), t, len(f), t.Len())
		}
		return f, nil
	default:
		return nil, fmt.Errorf("%w: binary -> %s", ErrInvalidCast, t)
	}
}

type DateLiteral icegopher.Date

func (d DateLiteral) Value() icegopher.Date { return icegopher.Date(d) }

func (d DateLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 4-byte little-endian integer
	data = make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(d))
	return
}

func (d *DateLiteral) UnmarshalBinary(data []byte) error {
	// stored as 4-byte little-endian integer representing days from epoch
	if len(data) != 4 {
		return fmt.Errorf("%w: expected 4 bytes for date value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*d = DateLiteral(binary.LittleEndian.Uint32(data))
	return nil
}

func (d DateLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.DateType:
		return d, nil
	default:
		return nil, fmt.Errorf("%w: date -> %s", ErrInvalidCast, t)
	}
}

type TimeLiteral icegopher.Time

func (t TimeLiteral) Value() icegopher.Time { return icegopher.Time(t) }

func (d TimeLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 8-byte little-endian integer
	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(d))
	return
}

func (t *TimeLiteral) UnmarshalBinary(data []byte) error {
	// stored as 8 byte little-endian value representing microseconds
	// from midnight
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for time value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*t = TimeLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

func (t TimeLiteral) To(typ icegopher.Type) (Literal, error) {
	switch typ := typ.(type) {
	case icegopher.TimeType:
		return t, nil
	default:
		return nil, fmt.Errorf("%w: time -> %s", ErrInvalidCast, typ)
	}
}

type TimestampLiteral icegopher.Timestamp

func (t TimestampLiteral) Value() icegopher.Timestamp { return icegopher.Timestamp(t) }

func (d TimestampLiteral) MarshalBinary() (data []byte, err error) {
	// stored as 8-byte little-endian integer
	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(d))
	return
}

func (t *TimestampLiteral) UnmarshalBinary(data []byte) error {
	// stored as 8-byte little-endian value representing microseconds
	// since the epoch
	if len(data) != 8 {
		return fmt.Errorf("%w: expected 8 bytes for timestamp value, got %d", ErrInvalidBinarySerialization, len(data))
	}
	*t = TimestampLiteral(binary.LittleEndian.Uint64(data))
	return nil
}

func (t TimestampLiteral) To(typ icegopher.Type) (Literal, error) {
	switch typ := typ.(type) {
	case icegopher.DateType:
		v := icegopher.Date(time.UnixMicro(int64(t)).Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
		return DateLiteral(v), nil
	case icegopher.TimestampType, icegopher.TimestampTzType:
		return t, nil
	default:
		return nil, fmt.Errorf("%w: timestamp -> %s", ErrInvalidCast, typ)
	}
}

type UUIDLiteral uuid.UUID

func (u UUIDLiteral) Value() uuid.UUID { return uuid.UUID(u) }

func (u UUIDLiteral) MarshalBinary() (data []byte, err error) {
	return uuid.UUID(u).MarshalBinary()
}

func (u *UUIDLiteral) UnmarshalBinary(data []byte) error {
	// stored as 16-byte big-endian value
	out, err := uuid.FromBytes(data)
	if err != nil {
		return err
	}
	*u = UUIDLiteral(out)
	return nil
}

func (u UUIDLiteral) To(t icegopher.Type) (Literal, error) {
	switch t := t.(type) {
	case icegopher.UUIDType:
		return u, nil
	default:
		return nil, fmt.Errorf("%w: uuid -> %s", ErrInvalidCast, t)
	}
}

type DecimalLiteral decimal128.Num

func (d DecimalLiteral) Value() decimal128.Num { return decimal128.Num(d) }

func (d DecimalLiteral) MarshalBinary() (data []byte, err error) {
	// stored as unscaled value in two's complement big-endian values
	// using the minimum number of bytes for the values
	n := decimal128.Num(d).BigInt()
	// bytes gives absolute value as big-endian bytes
	data = n.Bytes()
	if n.Sign() < 0 {
		// convert to 2's complement for negative value
		for i, v := range data {
			data[i] = ^v
		}
		data[len(data)-1] += 1
	}
	return
}

func (d *DecimalLiteral) UnmarshalBinary(data []byte) error {
	// stored as an unscaled value in the form of two's complement
	// big-endian values using the minimum number of bytes for the values
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

func (d DecimalLiteral) To(t icegopher.Type) (Literal, error) {
	return nil, fmt.Errorf("%w: decimal -> %s", ErrInvalidCast, t)
}
