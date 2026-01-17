package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
)

// PreparedStatementCache stores mapping from prepared ID to the original query
type PreparedStatementCache struct {
	mu          sync.RWMutex
	queries     map[string]string // prepared ID (hex) -> query string
	pendingPrep map[int16]string  // stream ID -> query string (pending PREPARE)
}

func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		queries:     make(map[string]string),
		pendingPrep: make(map[int16]string),
	}
}

// AddPendingPrepared stores a query pending for a PREPARE response (identified by stream ID)
func (c *PreparedStatementCache) AddPendingPrepared(streamID int16, query string) {
	c.mu.Lock()
	c.pendingPrep[streamID] = query
	c.mu.Unlock()
	log.Printf("[PREPARE] Pending prepared statement for stream %d: %q", streamID, query)
}

// CompletePrepared is called when RESULT_PREPARED is received
// It associates the prepared ID with the query from the pending PREPARE
func (c *PreparedStatementCache) CompletePrepared(streamID int16, preparedID []byte) string {
	c.mu.Lock()
	query, ok := c.pendingPrep[streamID]
	delete(c.pendingPrep, streamID)
	c.mu.Unlock()

	if !ok {
		log.Printf("[PREPARE] No pending prepared statement for stream %d", streamID)
		return ""
	}

	if len(preparedID) == 0 {
		log.Printf("[PREPARE] Empty prepared ID for stream %d", streamID)
		return query
	}

	idHex := fmt.Sprintf("%x", preparedID)
	c.mu.Lock()
	c.queries[idHex] = query
	c.mu.Unlock()
	log.Printf("[PREPARE] Completed: prepared ID=%s -> query: %q", idHex, query)
	return query
}

// AddPreparedStatement stores a prepared statement query for a given prepared ID
func (c *PreparedStatementCache) AddPreparedStatement(preparedID []byte, query string) {
	if len(preparedID) == 0 {
		return
	}
	idHex := fmt.Sprintf("%x", preparedID)
	c.mu.Lock()
	c.queries[idHex] = query
	c.mu.Unlock()
	log.Printf("[PREPARE] Cached prepared statement ID=%s for query: %q", idHex, query)
}

// GetPreparedQuery returns the original query for a given prepared ID
func (c *PreparedStatementCache) GetPreparedQuery(preparedID []byte) string {
	if len(preparedID) == 0 {
		return ""
	}
	idHex := fmt.Sprintf("%x", preparedID)
	c.mu.RLock()
	query, ok := c.queries[idHex]
	c.mu.RUnlock()
	if ok {
		log.Printf("[EXECUTE] Found cached query for prepared ID=%s: %q", idHex, query)
		return query
	}
	log.Printf("[EXECUTE] No cached query found for prepared ID=%s", idHex)
	return ""
}

// SetPreparedQuery associates a prepared ID (from server response) with a query
// This is used when we receive RESULT_PREPARED from the server
func (c *PreparedStatementCache) SetPreparedQuery(preparedID []byte, query string) {
	if len(preparedID) == 0 {
		return
	}
	idHex := fmt.Sprintf("%x", preparedID)
	c.mu.Lock()
	c.queries[idHex] = query
	c.mu.Unlock()
	log.Printf("[PREPARE] Set prepared ID=%s -> query: %q", idHex, query)
}

// CQL binary protocol opcodes
const (
	OpcodeError        = 0x00
	OpcodeStartup      = 0x01
	OpcodeReady        = 0x02
	OpcodeAuthenticate = 0x03
	OpcodeOptions      = 0x05
	OpcodeQuery        = 0x07
	OpcodeResult       = 0x08
	OpcodePrepare      = 0x09
	OpcodeExecute      = 0x0A
	OpcodeRegister     = 0x0B
)

// CQL result kinds
const (
	ResultKindVoid         = 0x0001
	ResultKindRows         = 0x0002
	ResultKindSetKeyspace  = 0x0003
	ResultKindPrepared     = 0x0004
	ResultKindSchemaChange = 0x0005
)

// CQL column types
const (
	ColumnTypeCustom    = 0x0000
	ColumnTypeASCII     = 0x0001
	ColumnTypeBigInt    = 0x0002
	ColumnTypeBlob      = 0x0003
	ColumnTypeBoolean   = 0x0004
	ColumnTypeCounter   = 0x0005
	ColumnTypeDecimal   = 0x0006
	ColumnTypeDouble    = 0x0007
	ColumnTypeFloat     = 0x0008
	ColumnTypeInt       = 0x0009
	ColumnTypeTimestamp = 0x000B
	ColumnTypeUUID      = 0x000C
	ColumnTypeVarchar   = 0x000D
	ColumnTypeVarInt    = 0x000E
	ColumnTypeTimeUUID  = 0x000F
	ColumnTypeInet      = 0x0010
	ColumnTypeDate      = 0x0011
	ColumnTypeSmallInt  = 0x0012
	ColumnTypeTinyInt   = 0x0013
	ColumnTypeList      = 0x0020
	ColumnTypeMap       = 0x0021
	ColumnTypeSet       = 0x0022
	ColumnTypeUDT       = 0x0030
	ColumnTypeTuple     = 0x0031
)

// CQLHeader represents the 9-byte CQL binary protocol header
type CQLHeader struct {
	Version byte
	Flags   byte
	Stream  int16
	Opcode  byte
	Length  uint32
}

// CQLMessage represents a parsed CQL message
type CQLMessage struct {
	Header     CQLHeader
	Body       []byte
	Query      string
	Params     [][]byte
	Columns    []ColumnSpec
	Rows       [][]byte
	PreparedID []byte // For EXECUTE: the prepared ID bytes
}

// ColumnSpec represents a column specification in RESULT message
type ColumnSpec struct {
	Name     string
	TypeCode uint16
}

// ParseCQLHeader parses the 9-byte CQL header from data
func ParseCQLHeader(data []byte) (*CQLHeader, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("not enough data for header: %d bytes", len(data))
	}

	return &CQLHeader{
		Version: data[0],
		Flags:   data[1],
		Stream:  int16(data[2])<<8 | int16(data[3]),
		Opcode:  data[4],
		Length:  binary.BigEndian.Uint32(data[5:9]),
	}, nil
}

// ParseCQLMessage parses a complete CQL message (header + body)
func ParseCQLMessage(data []byte) (*CQLMessage, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("not enough data for CQL message: %d bytes", len(data))
	}

	header, err := ParseCQLHeader(data)
	if err != nil {
		return nil, err
	}

	msg := &CQLMessage{
		Header: *header,
		Body:   data[9:],
	}

	// Parse body based on opcode
	switch header.Opcode {
	case OpcodeQuery:
		msg.parseQuery()
	case OpcodePrepare:
		msg.parsePrepare()
	case OpcodeExecute:
		msg.parseExecute()
	case OpcodeResult:
		msg.parseResult()
	}

	return msg, nil
}

// parsePrepare parses PREPARE request body
// Format: [query length:4][query string]
func (m *CQLMessage) parsePrepare() {
	if len(m.Body) < 4 {
		log.Printf("[CQL] PREPARE: not enough data for query length")
		return
	}

	queryLen := binary.BigEndian.Uint32(m.Body[:4])
	if int(queryLen)+4 > len(m.Body) {
		log.Printf("[CQL] PREPARE: query length %d exceeds body size %d", queryLen, len(m.Body)-4)
		return
	}

	m.Query = string(m.Body[4 : 4+int(queryLen)])
	log.Printf("[CQL] PREPARE parsed: %q", m.Query)
}

// parseQuery parses QUERY request body
// Format: [query length:4][query string][consistency:2][flags:4][page size:4][paging state...]
func (m *CQLMessage) parseQuery() {
	if len(m.Body) < 4 {
		log.Printf("[CQL] QUERY: not enough data for query length")
		return
	}

	queryLen := binary.BigEndian.Uint32(m.Body[:4])
	if int(queryLen)+4 > len(m.Body) {
		log.Printf("[CQL] QUERY: query length %d exceeds body size %d", queryLen, len(m.Body)-4)
		return
	}

	m.Query = string(m.Body[4 : 4+int(queryLen)])
	log.Printf("[CQL] QUERY parsed: %q", m.Query)

	// Parse parameters if present (after query string)
	pos := 4 + int(queryLen)
	if pos+2 <= len(m.Body) {
		consistency := binary.BigEndian.Uint16(m.Body[pos : pos+2])
		log.Printf("[CQL] QUERY consistency: %d", consistency)
		pos += 2

		// Check for query params flags
		if pos+4 <= len(m.Body) {
			flags := binary.BigEndian.Uint32(m.Body[pos : pos+4])
			log.Printf("[CQL] QUERY flags: 0x%08x", flags)
			pos += 4

			// Skip page size and paging state for now
			// In a full implementation, we'd parse the actual parameter values
		}
	}
}

// parseExecute parses EXECUTE request body
// Format: [prepared ID length:2][prepared ID][params count:2][params...][consistency:2][flags:4]...
func (m *CQLMessage) parseExecute() {
	if len(m.Body) < 2 {
		log.Printf("[CQL] EXECUTE: not enough data for prepared ID length")
		return
	}

	preparedIDLen := binary.BigEndian.Uint16(m.Body[:2])
	if int(preparedIDLen)+2+2 > len(m.Body) {
		log.Printf("[CQL] EXECUTE: prepared ID length %d exceeds body size", preparedIDLen)
		return
	}

	m.PreparedID = make([]byte, preparedIDLen)
	copy(m.PreparedID, m.Body[2:2+int(preparedIDLen)])
	log.Printf("[CQL] EXECUTE prepared ID: %x", m.PreparedID)

	pos := 2 + int(preparedIDLen)
	if pos+2 > len(m.Body) {
		log.Printf("[CQL] EXECUTE: not enough data for params count")
		return
	}

	paramsCount := binary.BigEndian.Uint16(m.Body[pos : pos+2])
	log.Printf("[CQL] EXECUTE params count: %d", paramsCount)
	pos += 2

	// Parse each parameter
	m.Params = make([][]byte, paramsCount)
	for i := 0; i < int(paramsCount); i++ {
		if pos+4 > len(m.Body) {
			log.Printf("[CQL] EXECUTE: not enough data for param %d length", i)
			break
		}
		paramLen := binary.BigEndian.Uint32(m.Body[pos : pos+4])
		pos += 4

		if int(paramLen) > len(m.Body)-pos {
			log.Printf("[CQL] EXECUTE: param %d length %d exceeds remaining data", i, paramLen)
			break
		}

		m.Params[i] = m.Body[pos : pos+int(paramLen)]
		log.Printf("[CQL] EXECUTE param %d: %d bytes", i, paramLen)
		pos += int(paramLen)
	}
}

// parseResult parses RESULT response body
// Format: [result kind:4][...varies by kind...]
func (m *CQLMessage) parseResult() {
	if len(m.Body) < 4 {
		log.Printf("[CQL] RESULT: not enough data for result kind")
		return
	}

	resultKind := binary.BigEndian.Uint32(m.Body[:4])
	log.Printf("[CQL] RESULT kind: 0x%08x", resultKind)

	switch resultKind {
	case ResultKindRows:
		m.parseRows()
	case ResultKindVoid:
		log.Printf("[CQL] RESULT: void response")
	case ResultKindSetKeyspace:
		log.Printf("[CQL] RESULT: set keyspace response")
	case ResultKindPrepared:
		m.parsePreparedResult()
	case ResultKindSchemaChange:
		log.Printf("[CQL] RESULT: schema change response")
	default:
		log.Printf("[CQL] RESULT: unknown kind 0x%08x", resultKind)
	}
}

// parsePreparedResult parses RESULT_PREPARED response
// Format: [result kind:4][prepared ID length:2][prepared ID][result metadata...]
func (m *CQLMessage) parsePreparedResult() {
	pos := 4 // skip result kind

	if pos+2 > len(m.Body) {
		log.Printf("[CQL] PREPARED: not enough data for prepared ID length")
		return
	}

	preparedIDLen := binary.BigEndian.Uint16(m.Body[pos : pos+2])
	pos += 2

	if int(preparedIDLen) > len(m.Body)-pos {
		log.Printf("[CQL] PREPARED: prepared ID length %d exceeds remaining data", preparedIDLen)
		return
	}

	m.PreparedID = make([]byte, preparedIDLen)
	copy(m.PreparedID, m.Body[pos:pos+int(preparedIDLen)])
	log.Printf("[CQL] PREPARED response: prepared ID=%x", m.PreparedID)
}

// parseRows parses RESULT_ROWS response
// Format: [columns count:4][column specs...][rows count:4][rows...]
func (m *CQLMessage) parseRows() {
	pos := 4 // skip result kind

	if pos+4 > len(m.Body) {
		log.Printf("[CQL] ROWS: not enough data for columns count")
		return
	}

	columnsCount := binary.BigEndian.Uint32(m.Body[pos : pos+4])
	log.Printf("[CQL] ROWS: %d columns", columnsCount)
	pos += 4

	// Parse column specifications
	m.Columns = make([]ColumnSpec, columnsCount)
	for i := 0; i < int(columnsCount); i++ {
		if pos+4 > len(m.Body) {
			log.Printf("[CQL] ROWS: not enough data for column %d name length", i)
			break
		}

		nameLen := binary.BigEndian.Uint32(m.Body[pos : pos+4])
		pos += 4

		if int(nameLen) > len(m.Body)-pos {
			log.Printf("[CQL] ROWS: column %d name length %d exceeds remaining data", i, nameLen)
			break
		}

		colName := string(m.Body[pos : pos+int(nameLen)])
		pos += int(nameLen)

		// Parse column type (variable length)
		colType, typeLen := m.parseColumnType(m.Body[pos:])
		m.Columns[i] = ColumnSpec{
			Name:     colName,
			TypeCode: colType,
		}
		log.Printf("[CQL] ROWS: column %d = %s (type 0x%04x)", i, colName, colType)
		pos += typeLen
	}

	// Find email column index
	emailColIdx := m.findEmailColumn()
	if emailColIdx >= 0 {
		log.Printf("[CQL] ROWS: email column found at index %d", emailColIdx)
	}

	// Parse rows
	if pos+4 > len(m.Body) {
		log.Printf("[CQL] ROWS: not enough data for rows count")
		return
	}

	rowsCount := binary.BigEndian.Uint32(m.Body[pos : pos+4])
	log.Printf("[CQL] ROWS: %d rows", rowsCount)
	pos += 4

	m.Rows = make([][]byte, rowsCount)
	for i := 0; i < int(rowsCount); i++ {
		if pos+4 > len(m.Body) {
			log.Printf("[CQL] ROWS: not enough data for row %d value length", i)
			break
		}

		valueLen := binary.BigEndian.Uint32(m.Body[pos : pos+4])
		pos += 4

		if int(valueLen) > len(m.Body)-pos {
			log.Printf("[CQL] ROWS: row %d value length %d exceeds remaining data", i, valueLen)
			break
		}

		m.Rows[i] = m.Body[pos : pos+int(valueLen)]
		log.Printf("[CQL] ROWS: row %d value length = %d bytes", i, valueLen)
		pos += int(valueLen)
	}
}

// parseColumnType parses a column type from the body
// Returns type code and number of bytes consumed
func (m *CQLMessage) parseColumnType(data []byte) (uint16, int) {
	if len(data) < 2 {
		return 0, 0
	}

	typeCode := binary.BigEndian.Uint16(data[:2])
	consumed := 2

	// Handle custom types (0x0000) which have a string name
	if typeCode == ColumnTypeCustom {
		if len(data) >= 4 {
			nameLen := binary.BigEndian.Uint16(data[2:4])
			consumed += 2 + int(nameLen)
		}
	}

	// Handle collection types (0x0020, 0x0021, 0x0022) which have nested types
	if typeCode >= 0x0020 && typeCode <= 0x0022 {
		if len(data) >= 4 {
			// Parse value type
			_, valLen := m.parseColumnType(data[2:])
			consumed += valLen
		}
	}

	// Handle UDT (0x0030) and Tuple (0x0031) which have multiple field types
	if typeCode == ColumnTypeUDT || typeCode == ColumnTypeTuple {
		if len(data) >= 4 {
			numFields := binary.BigEndian.Uint16(data[2:4])
			consumed += 2
			for j := 0; j < int(numFields); j++ {
				_, fieldLen := m.parseColumnType(data[consumed:])
				consumed += fieldLen
			}
		}
	}

	return typeCode, consumed
}

// findEmailColumn returns the index of the email column, or -1 if not found
func (m *CQLMessage) findEmailColumn() int {
	emailKeywords := []string{"email", "e-mail", "mail", "user_email", "contact_email"}

	for i, col := range m.Columns {
		lowerName := bytes.ToLower([]byte(col.Name))
		for _, kw := range emailKeywords {
			if bytes.Contains(lowerName, []byte(kw)) {
				return i
			}
		}
	}
	return -1
}

// FindPIIParamIndex returns the index of the first parameter that might contain PII
// based on the query string analysis
func (m *CQLMessage) FindPIIParamIndex() int {
	if m.Query == "" {
		return -1
	}

	// Common patterns for PII in queries
	piiPatterns := []string{
		"email",
		"phone",
		"ssn",
		"password",
		"address",
	}

	queryLower := bytes.ToLower([]byte(m.Query))

	for _, pattern := range piiPatterns {
		if bytes.Contains(queryLower, []byte(pattern)) {
			// If query mentions email, likely the first string param is the email
			if pattern == "email" && len(m.Params) > 0 {
				return 0
			}
		}
	}

	return -1
}

// IsEmailColumn returns true if the column at index is an email column
func (m *CQLMessage) IsEmailColumn(colIdx int) bool {
	if colIdx < 0 || colIdx >= len(m.Columns) {
		return false
	}

	col := m.Columns[colIdx]
	emailKeywords := []string{"email", "e-mail", "mail", "user_email", "contact_email"}

	lowerName := bytes.ToLower([]byte(col.Name))
	for _, kw := range emailKeywords {
		if bytes.Contains(lowerName, []byte(kw)) {
			return true
		}
	}

	// Also check column type - varchar/text types are likely email
	switch col.TypeCode {
	case ColumnTypeVarchar, ColumnTypeASCII:
		return false // Type alone is not enough, need name match
	}

	return false
}

// MaskPIIValues masks PII values in the message using the provided mask function
// Returns the modified message bytes and whether any masking was applied
func (m *CQLMessage) MaskPIIValues(maskFn func(columnName, value string) (string, error)) ([]byte, bool) {
	masked := false

	// For responses with rows, mask email column values
	if m.Header.Opcode == OpcodeResult && len(m.Columns) > 0 && len(m.Rows) > 0 {
		emailColIdx := m.findEmailColumn()
		if emailColIdx >= 0 {
			log.Printf("[CQL] Masking email column at index %d in %d rows", emailColIdx, len(m.Rows))
			for i, row := range m.Rows {
				if emailColIdx < len(m.Columns) {
					colName := m.Columns[emailColIdx].Name
					maskedValue, err := maskFn(colName, string(row))
					if err != nil {
						log.Printf("[CQL] Failed to mask value: %v", err)
					} else {
						m.Rows[i] = []byte(maskedValue)
						masked = true
					}
				}
			}
		}
	}

	// For requests with parameters, mask PII parameters
	if (m.Header.Opcode == OpcodeQuery || m.Header.Opcode == OpcodeExecute) && len(m.Params) > 0 {
		piiIdx := m.FindPIIParamIndex()
		if piiIdx >= 0 && piiIdx < len(m.Params) {
			colName := "pii_param"
			if m.Query != "" {
				// Try to infer column name from query
				colName = inferColumnNameFromQuery(m.Query, piiIdx)
			}
			log.Printf("[CQL] Masking PII parameter at index %d", piiIdx)
			maskedValue, err := maskFn(colName, string(m.Params[piiIdx]))
			if err != nil {
				log.Printf("[CQL] Failed to mask parameter: %v", err)
			} else {
				m.Params[piiIdx] = []byte(maskedValue)
				masked = true
			}
		}
	}

	if !masked {
		return nil, false
	}

	// Reconstruct the message with masked values
	result, err := m.reconstructMessage()
	if err != nil {
		log.Printf("[CQL] Failed to reconstruct message: %v", err)
		return nil, false
	}
	return result, true
}

// inferColumnNameFromQuery tries to guess the column name from the query
func inferColumnNameFromQuery(query string, paramIdx int) string {
	// Simple heuristic: look for patterns like "WHERE email = ?" or "email = ?"
	// This is a simplified version - a full implementation would parse the query
	lowerQuery := bytes.ToLower([]byte(query))
	keywords := []string{"email", "phone", "ssn", "password"}
	for _, kw := range keywords {
		if bytes.Contains(lowerQuery, []byte(kw)) {
			return kw
		}
	}
	return "pii_param"
}

// reconstructMessage rebuilds the CQL message from the parsed structure
func (m *CQLMessage) reconstructMessage() ([]byte, error) {
	var body []byte

	switch m.Header.Opcode {
	case OpcodeQuery:
		body = m.reconstructQueryBody()
	case OpcodeExecute:
		body = m.reconstructExecuteBody()
	case OpcodeResult:
		body = m.reconstructResultBody()
	default:
		// For other opcodes, return original body
		return m.Body, nil
	}

	// Build header
	header := make([]byte, 9)
	header[0] = m.Header.Version
	header[1] = m.Header.Flags
	header[2] = byte(m.Header.Stream >> 8)
	header[3] = byte(m.Header.Stream & 0xFF)
	header[4] = m.Header.Opcode
	binary.BigEndian.PutUint32(header[5:], uint32(len(body)))

	return append(header, body...), nil
}

func (m *CQLMessage) reconstructQueryBody() []byte {
	var body []byte

	// Query string length + query string
	body = append(body, byte(len(m.Query)>>24), byte(len(m.Query)>>16), byte(len(m.Query)>>8), byte(len(m.Query)&0xFF))
	body = append(body, []byte(m.Query)...)

	// For simplicity, we don't reconstruct the full query params here
	// In a full implementation, we'd need to track all the query parameters
	// For now, just append the original body after the query string
	queryLen := 4 + len(m.Query)
	if queryLen < len(m.Body) {
		body = append(body, m.Body[queryLen:]...)
	}

	return body
}

func (m *CQLMessage) reconstructExecuteBody() []byte {
	var body []byte
	var preparedIDLen uint16

	// Prepared ID (we need to track this - for now use original)
	// This is a simplified version
	if len(m.Body) >= 2 {
		preparedIDLen = binary.BigEndian.Uint16(m.Body[:2])
		body = append(body, m.Body[:2+int(preparedIDLen)]...)
	}

	// Params count + params
	body = append(body, byte(len(m.Params)>>8), byte(len(m.Params)&0xFF))
	for _, param := range m.Params {
		body = append(body, byte(len(param)>>24), byte(len(param)>>16), byte(len(param)>>8), byte(len(param)&0xFF))
		body = append(body, param...)
	}

	// Append rest of original body
	pos := 2 + int(preparedIDLen) + 2 + len(m.Params)*4
	for _, param := range m.Params {
		pos += len(param)
	}
	if pos < len(m.Body) {
		body = append(body, m.Body[pos:]...)
	}

	return body
}

func (m *CQLMessage) reconstructResultBody() []byte {
	var body []byte

	// Result kind
	body = append(body, byte(ResultKindRows>>24), byte(ResultKindRows>>16), byte(ResultKindRows>>8), byte(ResultKindRows&0xFF))

	// Columns count + column specs
	body = append(body, byte(len(m.Columns)>>24), byte(len(m.Columns)>>16), byte(len(m.Columns)>>8), byte(len(m.Columns)&0xFF))
	for _, col := range m.Columns {
		// Column name length + name
		body = append(body, byte(len(col.Name)>>24), byte(len(col.Name)>>16), byte(len(col.Name)>>8), byte(len(col.Name)&0xFF))
		body = append(body, []byte(col.Name)...)
		// Column type
		body = append(body, byte(col.TypeCode>>8), byte(col.TypeCode&0xFF))
	}

	// Rows count + rows
	body = append(body, byte(len(m.Rows)>>24), byte(len(m.Rows)>>16), byte(len(m.Rows)>>8), byte(len(m.Rows)&0xFF))
	for _, row := range m.Rows {
		body = append(body, byte(len(row)>>24), byte(len(row)>>16), byte(len(row)>>8), byte(len(row)&0xFF))
		body = append(body, row...)
	}

	return body
}
