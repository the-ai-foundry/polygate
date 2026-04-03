package schema

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/polygate/polygate/internal/model"
)

// CoerceBatch converts all record values to their proper Go types
// based on the table schema. This ensures each sink receives correctly
// typed data (e.g., time.Time instead of a string for timestamps).
func (s *TableSchema) CoerceBatch(records []model.Record) ([]model.Record, error) {
	for i, rec := range records {
		for col, colType := range s.Columns {
			val, ok := rec[col]
			if !ok || val == nil {
				continue
			}
			coerced, err := coerceValue(val, colType)
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", i, col, err)
			}
			records[i][col] = coerced
		}
	}
	return records, nil
}

func coerceValue(val any, typ ColumnType) (any, error) {
	switch typ {
	case TypeTimestamp:
		return coerceTimestamp(val)
	case TypeInt32:
		return coerceInt(val, 32)
	case TypeInt64:
		return coerceInt(val, 64)
	case TypeFloat32:
		return coerceFloat(val, 32)
	case TypeFloat64:
		return coerceFloat(val, 64)
	case TypeBool:
		return coerceBool(val)
	case TypeBytes:
		return coerceBytes(val)
	case TypeString, TypeText, TypeJSON:
		return val, nil
	default:
		return val, nil
	}
}

func coerceTimestamp(val any) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try RFC3339 first, then common formats
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				return t, nil
			}
		}
		// Try unix epoch as string
		if epoch, err := strconv.ParseFloat(v, 64); err == nil {
			sec := int64(epoch)
			nsec := int64((epoch - float64(sec)) * 1e9)
			return time.Unix(sec, nsec).UTC(), nil
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", v)
	case float64:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	case int64:
		return time.Unix(v, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot coerce %T to timestamp", val)
	}
}

func coerceInt(val any, bits int) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("cannot losslessly convert %v to int%d", v, bits)
		}
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, bits)
	default:
		return 0, fmt.Errorf("cannot coerce %T to int%d", val, bits)
	}
}

func coerceFloat(val any, bits int) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, bits)
	default:
		return 0, fmt.Errorf("cannot coerce %T to float%d", val, bits)
	}
}

func coerceBool(val any) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			return true, nil
		case "false", "0", "no":
			return false, nil
		default:
			return false, fmt.Errorf("cannot parse %q as bool", v)
		}
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot coerce %T to bool", val)
	}
}

func coerceBytes(val any) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		return v, nil
	case string:
		s := strings.TrimPrefix(v, "0x")
		return hex.DecodeString(s)
	default:
		return nil, fmt.Errorf("cannot coerce %T to bytes", val)
	}
}
