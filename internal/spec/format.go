package spec

import (
	"bufio"
	"bytes"
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

func Format(spec Spec) ([]byte, error) {
	return encodeYAML(spec)
}

func FormatManifest(manifest Manifest) ([]byte, error) {
	return encodeYAML(manifest)
}

func FormatBase(base Base) ([]byte, error) {
	return encodeYAML(base)
}

func FormatCampaignsFile(campaigns CampaignsFile) ([]byte, error) {
	return encodeYAML(campaigns)
}

func FormatFiles(path string) ([]FormattedFile, error) {
	document, err := LoadDocumentFile(path)
	if err != nil {
		return nil, err
	}
	switch document.Type {
	case DocumentTypeConfig:
		content, err := Format(document.Spec)
		if err != nil {
			return nil, err
		}
		return []FormattedFile{{Path: path, Content: content}}, nil
	case DocumentTypeManifest:
		files := make([]FormattedFile, 0, 2+len(document.CampaignPaths))
		content, err := FormatManifest(document.Manifest)
		if err != nil {
			return nil, err
		}
		files = append(files, FormattedFile{Path: path, Content: content})

		baseDocument, err := LoadDocumentFile(document.BasePath)
		if err != nil {
			return nil, fmt.Errorf("format base %q: %w", document.BasePath, err)
		}
		baseContent, err := FormatBase(baseDocument.Base)
		if err != nil {
			return nil, err
		}
		files = append(files, FormattedFile{Path: document.BasePath, Content: baseContent})

		for _, campaignPath := range document.CampaignPaths {
			campaignDocument, err := LoadDocumentFile(campaignPath)
			if err != nil {
				return nil, fmt.Errorf("format campaigns file %q: %w", campaignPath, err)
			}
			campaignContent, err := FormatCampaignsFile(campaignDocument.Campaigns)
			if err != nil {
				return nil, err
			}
			files = append(files, FormattedFile{Path: campaignPath, Content: campaignContent})
		}
		return files, nil
	case DocumentTypeBase:
		content, err := FormatBase(document.Base)
		if err != nil {
			return nil, err
		}
		return []FormattedFile{{Path: path, Content: content}}, nil
	case DocumentTypeCampaigns:
		content, err := FormatCampaignsFile(document.Campaigns)
		if err != nil {
			return nil, err
		}
		return []FormattedFile{{Path: path, Content: content}}, nil
	default:
		return nil, fmt.Errorf("format %s: unsupported document type %q", path, document.Type)
	}
}

func encodeYAML(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return formatInlineFlowMappings(buffer.Bytes()), nil
}

type inlineFlowRecord struct {
	indent string
	fields []inlineFlowField
}

type inlineFlowField struct {
	key   string
	value string
}

func formatInlineFlowMappings(input []byte) []byte {
	scanner := bufio.NewScanner(bytes.NewReader(input))
	lines := make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return input
	}

	records := make([]*inlineFlowRecord, len(lines))
	for i, line := range lines {
		record, ok := parseInlineFlowMappingLine(line)
		if !ok {
			continue
		}
		records[i] = record
	}

	var output bytes.Buffer
	for index := 0; index < len(lines); {
		record := records[index]
		if record == nil {
			output.WriteString(lines[index])
			output.WriteByte('\n')
			index++
			continue
		}

		end := index + 1
		for end < len(lines) && canAlignInlineFlowRecord(record, records[end]) {
			end++
		}

		if aligned, ok := alignInlineFlowBlock(records[index:end]); ok {
			for _, line := range aligned {
				output.WriteString(line)
				output.WriteByte('\n')
			}
		} else {
			for _, line := range lines[index:end] {
				output.WriteString(line)
				output.WriteByte('\n')
			}
		}
		index = end
	}
	return output.Bytes()
}

func parseInlineFlowMappingLine(line string) (*inlineFlowRecord, bool) {
	trimmed := strings.TrimLeft(line, " ")
	if !strings.HasPrefix(trimmed, "- {") || !strings.HasSuffix(trimmed, "}") {
		return nil, false
	}

	indentWidth := len(line) - len(trimmed)
	if indentWidth < 0 || len(trimmed) < len("- {}") {
		return nil, false
	}

	body := strings.TrimSuffix(strings.TrimPrefix(trimmed, "- {"), "}")
	body = strings.TrimSpace(body)
	parts, ok := splitInlineFlowFields(body)
	if !ok || len(parts) == 0 {
		return nil, false
	}

	fields := make([]inlineFlowField, 0, len(parts))
	for _, part := range parts {
		key, value, ok := splitInlineFlowField(part)
		if !ok {
			return nil, false
		}
		fields = append(fields, inlineFlowField{key: key, value: value})
	}

	return &inlineFlowRecord{
		indent: line[:indentWidth],
		fields: fields,
	}, true
}

func splitInlineFlowFields(body string) ([]string, bool) {
	parts := make([]string, 0, 4)
	start := 0
	quote := byte(0)
	for index := 0; index < len(body); index++ {
		current := body[index]
		if quote != 0 {
			if current == quote {
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"':
			quote = current
		case ',':
			parts = append(parts, strings.TrimSpace(body[start:index]))
			start = index + 1
		}
	}
	if quote != 0 {
		return nil, false
	}
	parts = append(parts, strings.TrimSpace(body[start:]))
	return parts, true
}

func splitInlineFlowField(part string) (string, string, bool) {
	quote := byte(0)
	for index := 0; index+1 < len(part); index++ {
		current := part[index]
		if quote != 0 {
			if current == quote {
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"':
			quote = current
		case ':':
			if part[index+1] != ' ' {
				continue
			}
			key := strings.TrimSpace(part[:index])
			value := strings.TrimSpace(part[index+2:])
			if key == "" || value == "" {
				return "", "", false
			}
			return key, value, true
		}
	}
	return "", "", false
}

func canAlignInlineFlowRecord(left *inlineFlowRecord, right *inlineFlowRecord) bool {
	if left == nil || right == nil {
		return false
	}
	if left.indent != right.indent {
		return false
	}
	return inlineFlowRecordKind(left) == inlineFlowRecordKind(right)
}

func inlineFlowRecordKind(record *inlineFlowRecord) string {
	if record == nil {
		return ""
	}
	keys := make([]string, 0, len(record.fields))
	for _, field := range record.fields {
		keys = append(keys, field.key)
	}
	switch {
	case slices.Equal(keys, []string{"text", "match_type", "bid", "status"}):
		return "keyword"
	case slices.Equal(keys, []string{"text", "match_type", "status"}):
		return "negative"
	default:
		return ""
	}
}

func alignInlineFlowBlock(records []*inlineFlowRecord) ([]string, bool) {
	if len(records) == 0 {
		return nil, false
	}
	kind := inlineFlowRecordKind(records[0])
	if kind == "" {
		return nil, false
	}

	widths := make([]int, len(records[0].fields)-1)
	for _, record := range records {
		if inlineFlowRecordKind(record) != kind || len(record.fields) != len(records[0].fields) {
			return nil, false
		}
		for index := 0; index < len(record.fields)-1; index++ {
			widths[index] = max(widths[index], len(renderInlineFlowField(record.fields[index])))
		}
	}

	lines := make([]string, 0, len(records))
	for _, record := range records {
		var builder strings.Builder
		builder.WriteString(record.indent)
		builder.WriteString("- { ")
		for index, field := range record.fields {
			rendered := renderInlineFlowField(field)
			builder.WriteString(rendered)
			if index == len(record.fields)-1 {
				continue
			}
			builder.WriteString(",")
			builder.WriteString(strings.Repeat(" ", widths[index]-len(rendered)+1))
		}
		builder.WriteString(" }")
		lines = append(lines, builder.String())
	}
	return lines, true
}

func renderInlineFlowField(field inlineFlowField) string {
	return field.key + ": " + field.value
}
