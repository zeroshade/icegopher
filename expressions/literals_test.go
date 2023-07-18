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

package expressions_test

import (
	"encoding"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/expressions"
)

func TestLiteralIdentityConversions(t *testing.T) {
	tests := []struct {
		lit expressions.Literal
		to  icegopher.Type
	}{
		{expressions.NewLiteral(int32(34)), icegopher.PrimitiveTypes.Int32},
		{expressions.NewLiteral(int64(34)), icegopher.PrimitiveTypes.Int64},
		{expressions.NewLiteral(float32(34.56)), icegopher.PrimitiveTypes.Float32},
		{expressions.NewLiteral(float64(34.56)), icegopher.PrimitiveTypes.Float64},
		{expressions.NewLiteral(true), icegopher.PrimitiveTypes.Bool},
		{expressions.NewLiteral("abc"), icegopher.PrimitiveTypes.String},
		{expressions.NewLiteral(uuid.New()), icegopher.PrimitiveTypes.UUID},
		{expressions.NewLiteral([]byte{0x1, 0x2, 0x3}), icegopher.FixedTypeOf(3)},
		{expressions.NewLiteral(icegopher.Date(1)), icegopher.PrimitiveTypes.Date},
		{expressions.NewLiteral(icegopher.Time(60*(60*(10)+10) + 10)), icegopher.PrimitiveTypes.Time},
		{expressions.NewLiteral(icegopher.Timestamp(8640000)), icegopher.PrimitiveTypes.Timestamp},
		{expressions.NewLiteral([]byte{0x1, 0x2, 0x3}), icegopher.PrimitiveTypes.Binary},
	}

	for _, tt := range tests {
		t.Run(tt.to.String(), func(t *testing.T) {
			conv, err := tt.lit.To(tt.to)
			require.NoError(t, err)

			assert.EqualValues(t, tt.lit, conv)
		})
	}
}

func TestLiteralSimpleConversions(t *testing.T) {
	tests := []struct {
		name string
		lit  expressions.Literal
		to   icegopher.Type
	}{
		{"int->int", expressions.NewLiteral(int32(34)), icegopher.PrimitiveTypes.Int32},
		{"int->long", expressions.NewLiteral(int32(34)), icegopher.PrimitiveTypes.Int64},
		{"int->float", expressions.NewLiteral(int32(34)), icegopher.PrimitiveTypes.Float32},
		{"int->double", expressions.NewLiteral(int32(34)), icegopher.PrimitiveTypes.Float64},
		{"int->date", expressions.NewLiteral(int32(1)), icegopher.PrimitiveTypes.Date},
		{"int->time", expressions.NewLiteral(int32(1)), icegopher.PrimitiveTypes.Time},
		{"long->long", expressions.NewLiteral(int64(34)), icegopher.PrimitiveTypes.Int64},
		{"long->int", expressions.NewLiteral(int64(34)), icegopher.PrimitiveTypes.Int32},
		{"long->float", expressions.NewLiteral(int64(34)), icegopher.PrimitiveTypes.Float32},
		{"long->double", expressions.NewLiteral(int64(34)), icegopher.PrimitiveTypes.Float64},
		{"long->date", expressions.NewLiteral(int64(1)), icegopher.PrimitiveTypes.Date},
		{"long->time", expressions.NewLiteral(int64(1)), icegopher.PrimitiveTypes.Time},
		{"float->float", expressions.NewLiteral(float32(34.56)), icegopher.PrimitiveTypes.Float32},
		{"float->double", expressions.NewLiteral(float32(34.56)), icegopher.PrimitiveTypes.Float64},
		{"double->double", expressions.NewLiteral(float64(34.56)), icegopher.PrimitiveTypes.Float64},
		{"double->float", expressions.NewLiteral(float64(34.56)), icegopher.PrimitiveTypes.Float32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv, err := tt.lit.To(tt.to)
			require.NoError(t, err)

			assert.EqualValues(t, tt.lit, conv)
		})
	}
}

func TestLiteralConvertOutsideBounds(t *testing.T) {
	lit := expressions.NewLiteral(int64(math.MaxInt32 + 1))
	conv, err := lit.To(icegopher.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, expressions.IntAboveMax{}, conv)

	lit = expressions.NewLiteral(int64(math.MinInt32 - 1))
	conv, err = lit.To(icegopher.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, expressions.IntBelowMin{}, conv)

	lit = expressions.NewLiteral(float64(math.MaxFloat32) + 1e37)
	conv, err = lit.To(icegopher.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, expressions.FloatAboveMax{}, conv)

	lit = expressions.NewLiteral(float64(-math.MaxFloat32) - 1e37)
	conv, err = lit.To(icegopher.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, expressions.FloatBelowMin{}, conv)
}

func TestLiteralLongToDecimalConversion(t *testing.T) {
	tests := []struct {
		typ icegopher.DecimalType
		str string
	}{
		{icegopher.DecimalTypeOf(9, 0), "34"},
		{icegopher.DecimalTypeOf(9, 2), "34.00"},
		{icegopher.DecimalTypeOf(9, 4), "34.0000"},
	}

	lit := expressions.NewLiteral(int32(34))
	longLit := expressions.NewLiteral(int64(34))
	for _, tt := range tests {
		val, err := lit.To(tt.typ)
		require.NoError(t, err)

		assert.Equal(t, tt.str, val.(expressions.DecimalLiteral).Value().ToString(int32(tt.typ.Scale())))

		val, err = longLit.To(tt.typ)
		require.NoError(t, err)

		assert.Equal(t, tt.str, val.(expressions.DecimalLiteral).Value().ToString(int32(tt.typ.Scale())))
	}
}

func TestLiteralFloatToDecimal(t *testing.T) {
	tests := []struct {
		typ icegopher.DecimalType
		str string
	}{
		{icegopher.DecimalTypeOf(9, 1), "34.6"},
		{icegopher.DecimalTypeOf(9, 2), "34.56"},
		{icegopher.DecimalTypeOf(9, 4), "34.5600"},
	}

	floatLit := expressions.NewLiteral(float32(34.56))
	dblLit := expressions.NewLiteral(float64(34.56))
	for _, tt := range tests {
		val, err := floatLit.To(tt.typ)
		require.NoError(t, err)
		assert.Equal(t, tt.str, val.(expressions.DecimalLiteral).Value().ToString(int32(tt.typ.Scale())))

		val, err = dblLit.To(tt.typ)
		require.NoError(t, err)
		assert.Equal(t, tt.str, val.(expressions.DecimalLiteral).Value().ToString(int32(tt.typ.Scale())))
	}
}

func TestBinaryLiteralConversions(t *testing.T) {
	val := expressions.NewLiteral([]byte{0x0, 0x1, 0x2})
	conv, err := val.To(icegopher.FixedTypeOf(3))
	require.NoError(t, err)

	assert.Equal(t, expressions.FixedLiteral([]byte{0x0, 0x1, 0x2}), conv)
}

func TestFixedLiteralConversions(t *testing.T) {
	val := expressions.FixedLiteral([]byte{0x0, 0x1, 0x2})
	conv, err := val.To(icegopher.FixedTypeOf(3))
	require.NoError(t, err)
	assert.Equal(t, val, conv)

	conv, err = val.To(icegopher.PrimitiveTypes.Binary)
	require.NoError(t, err)
	assert.Equal(t, []byte(val), []byte(conv.(expressions.BytesLiteral)))

	_, err = val.To(icegopher.FixedTypeOf(5))
	assert.ErrorIs(t, err, expressions.ErrInvalidCast)
}

func TestStringLiteral(t *testing.T) {
	tests := []struct {
		str      string
		typ      icegopher.Type
		expected expressions.Literal
	}{
		{"abc", icegopher.PrimitiveTypes.String, expressions.NewLiteral("abc")},
		{"3", icegopher.PrimitiveTypes.Int32, expressions.NewLiteral(int32(3))},
		{"3", icegopher.PrimitiveTypes.Int64, expressions.NewLiteral(int64(3))},
		{"3.141", icegopher.PrimitiveTypes.Float32, expressions.NewLiteral(float32(3.141))},
		{"3.141", icegopher.PrimitiveTypes.Float64, expressions.NewLiteral(float64(3.141))},
		{"1454-10-22", icegopher.PrimitiveTypes.Date, expressions.NewLiteral(icegopher.Date(-188171))},
		{"2017-08-18", icegopher.PrimitiveTypes.Date, expressions.NewLiteral(icegopher.Date(17396))},
		{"true", icegopher.PrimitiveTypes.Bool, expressions.NewLiteral(true)},
		{"14:21:01.919", icegopher.PrimitiveTypes.Time, expressions.NewLiteral(icegopher.Time(51661919000))},
		{"2017-08-18T14:21:01.919234", icegopher.PrimitiveTypes.Timestamp, expressions.NewLiteral(icegopher.Timestamp(1503066061919234))},
		{"2017-08-18T14:21:01.919234-07:00", icegopher.PrimitiveTypes.TimestampTz, expressions.NewLiteral(icegopher.Timestamp(1503091261919234))},
		{string(uuid.NameSpaceOID.String()), icegopher.PrimitiveTypes.UUID, expressions.NewLiteral(uuid.NameSpaceOID)},
		{"34.560", icegopher.DecimalTypeOf(9, 3), expressions.NewLiteral(decimal128.FromI64(34560))},
		{"2147483648", icegopher.PrimitiveTypes.Int32, expressions.IntAboveMax{}},
		{"-2147483649", icegopher.PrimitiveTypes.Int32, expressions.IntBelowMin{}},
		{"9223372036854775808", icegopher.PrimitiveTypes.Int64, expressions.LongAboveMax{}},
		{"-9223372036854775809", icegopher.PrimitiveTypes.Int64, expressions.LongBelowMin{}},
		{"4.40282346638528859811704183484516925440e+38", icegopher.PrimitiveTypes.Float32, expressions.FloatAboveMax{}},
		{"-4.40282346638528859811704183484516925440e+38", icegopher.PrimitiveTypes.Float32, expressions.FloatBelowMin{}},
		{"2.79769313486231570814527423731704356798070e+308", icegopher.PrimitiveTypes.Float64, expressions.DoubleAboveMax{}},
		{"-2.79769313486231570814527423731704356798070e+308", icegopher.PrimitiveTypes.Float64, expressions.DoubleBelowMin{}},
	}

	for _, tt := range tests {
		t.Run("string->"+tt.typ.String(), func(t *testing.T) {
			lit := expressions.NewLiteral(tt.str)
			conv, err := lit.To(tt.typ)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, conv)
		})
	}
}

func TestLiteralInvalidCasts(t *testing.T) {
	tests := []struct {
		from expressions.Literal
		to   []icegopher.Type
	}{
		{expressions.NewLiteral(true), []icegopher.Type{
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.Binary,
			icegopher.PrimitiveTypes.UUID,
			icegopher.FixedTypeOf(3),
		}},
		{expressions.NewLiteral(int32(34)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.FixedTypeOf(1),
			icegopher.PrimitiveTypes.Binary,
		}},
		{expressions.NewLiteral(int64(34)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.FixedTypeOf(1),
			icegopher.PrimitiveTypes.Binary,
		}},
		{expressions.NewLiteral(float32(34.11)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral(float64(34.11)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral(icegopher.Date(17396)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral(icegopher.Time(51661919000)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral(icegopher.Timestamp(1503066061919234)), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Time,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral("abc"), []icegopher.Type{
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.NewLiteral(uuid.New()), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.Binary,
			icegopher.FixedTypeOf(1),
		}},
		{expressions.FixedLiteral([]byte{0x0, 0x1, 0x2}), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
		}},
		{expressions.NewLiteral([]byte{0x0, 0x1, 0x2}), []icegopher.Type{
			icegopher.PrimitiveTypes.Bool,
			icegopher.PrimitiveTypes.Int32,
			icegopher.PrimitiveTypes.Int64,
			icegopher.PrimitiveTypes.Float32,
			icegopher.PrimitiveTypes.Float64,
			icegopher.PrimitiveTypes.Date,
			icegopher.PrimitiveTypes.Time,
			icegopher.PrimitiveTypes.Timestamp,
			icegopher.PrimitiveTypes.TimestampTz,
			icegopher.DecimalTypeOf(9, 2),
			icegopher.PrimitiveTypes.String,
			icegopher.PrimitiveTypes.UUID,
		}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.from), func(t *testing.T) {
			for _, to := range tt.to {
				t.Run("to "+to.String(), func(t *testing.T) {
					_, err := tt.from.To(to)
					assert.ErrorIs(t, err, expressions.ErrInvalidCast)
				})
			}
		})
	}
}

func TestLiteralRoundTripMarshal(t *testing.T) {
	tests := []struct {
		typ      icegopher.Type
		data     []byte
		expected expressions.Literal
	}{
		{icegopher.PrimitiveTypes.Bool, []byte{0x0}, expressions.NewLiteral(false)},
		{icegopher.PrimitiveTypes.Bool, []byte{0x1}, expressions.NewLiteral(true)},
		{icegopher.PrimitiveTypes.Int32, []byte{0xd2, 0x04, 0x0, 0x0}, expressions.NewLiteral(int32(1234))},
		{icegopher.PrimitiveTypes.Int64, []byte{0xd2, 0x04, 0, 0, 0, 0, 0, 0}, expressions.NewLiteral(int64(1234))},
		{icegopher.PrimitiveTypes.Float32, []byte{0x0, 0x0, 0x90, 0xc0}, expressions.NewLiteral(float32(-4.5))},
		{icegopher.PrimitiveTypes.Float32, []byte{0x19, 0x04, 0x9e, '?'}, expressions.NewLiteral(float32(1.2345))},
		{icegopher.PrimitiveTypes.Float64, []byte{0x8d, 0x97, 0x6e, 0x12, 0x83, 0xc0, 0xf3, 0x3f}, expressions.NewLiteral(float64(1.2345))},
		{icegopher.PrimitiveTypes.Date, []byte{0xe8, 0x03, 0, 0}, expressions.NewLiteral(icegopher.Date(1000))},
		{icegopher.PrimitiveTypes.Time, []byte{0, 0xe8, 'v', 'H', 0x17, 0, 0, 0}, expressions.NewLiteral(icegopher.Time(100000000000))},
		{icegopher.PrimitiveTypes.Timestamp, []byte{0x80, 0x1a, 0x06, 0, 0, 0, 0, 0}, expressions.NewLiteral(icegopher.Timestamp(400000))},
		{icegopher.PrimitiveTypes.String, []byte("ABC"), expressions.NewLiteral("ABC")},
		{icegopher.FixedTypeOf(3), []byte("foo"), expressions.FixedLiteral([]byte("foo"))},
		{icegopher.PrimitiveTypes.Binary, []byte("foo"), expressions.NewLiteral([]byte("foo"))},
		{icegopher.PrimitiveTypes.UUID, []byte{0xf7, 0x9c, '>', '\t', 'g', '|', 'K', 0xbd, 0xa4, 'y', '?', '4', 0x9c, 0xb7, 0x85, 0xe7},
			expressions.NewLiteral(uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7"))},
		{icegopher.DecimalTypeOf(5, 2), []byte{0x30, 0x39}, expressions.NewLiteral(decimal128.FromI64(12345))},
		{icegopher.DecimalTypeOf(7, 4), []byte{0xed, 0x29, 0x79}, expressions.NewLiteral(decimal128.FromI64(-1234567))},
		// decimal on 3 bytes to test we use the minimum number of bytes and not a power of 2
		{icegopher.DecimalTypeOf(7, 4), []byte{0x12, 0xd6, 0x87}, expressions.NewLiteral(decimal128.FromI64(1234567))},
		// test empty byte in decimal
		// 11 is 00001011 in binary
		{icegopher.DecimalTypeOf(10, 3), []byte{0x0b}, expressions.NewLiteral(decimal128.FromI64(11))},
		{icegopher.DecimalTypeOf(10, 3), []byte{}, expressions.NewLiteral(decimal128.Num{})},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			val, err := expressions.LiteralFromBytes(tt.typ, tt.data)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, val)

			data, err := val.(encoding.BinaryMarshaler).MarshalBinary()
			require.NoError(t, err)
			assert.Equal(t, tt.data, data)
		})
	}
}
