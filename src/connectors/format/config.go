package main

import "time"

// FormatRunnerConfig defines the configuration for the format runner
type FormatRunnerConfig struct {
	// Ordered list of formatting operations
	Operations []FormatOperation `mapstructure:"operations" validate:"required,min=1,dive"`

	// Maximum total processing time
	Timeout time.Duration `mapstructure:"timeout" default:"30s" validate:"gt=0,lte=300s"`

	// Enable detailed logging
	Verbose bool `mapstructure:"verbose" default:"false"`
}

// FormatOperation defines a single formatting operation
type FormatOperation struct {
	// Operation type: template, serialize, multipart, encode, compress, extract, merge, split
	Type string `mapstructure:"type" validate:"required,oneof=template serialize multipart encode compress extract merge split"`

	// Input source: data, metadata, filesystem, parts
	Input string `mapstructure:"input" default:"data" validate:"oneof=data metadata filesystem parts"`

	// Output destination: data, metadata, filesystem, parts
	Output string `mapstructure:"output" default:"data" validate:"oneof=data metadata filesystem parts"`

	// Operation-specific options
	Options map[string]any `mapstructure:"options"`
}

// TemplateOptions defines options for template operations
type TemplateOptions struct {
	// Template engine: text, html
	Engine string `mapstructure:"engine" default:"text" validate:"oneof=text html"`

	// Template content
	Template string `mapstructure:"template"`

	// Template file path (alternative to inline template)
	TemplateFile string `mapstructure:"templateFile"`

	// Maximum template size
	MaxTemplateSize int `mapstructure:"maxTemplateSize" default:"100000" validate:"gt=0"`

	// Timeout for template execution
	Timeout time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

// SerializeOptions defines options for serialization operations
type SerializeOptions struct {
	// Source format: json, yaml, cbor, msgpack
	From string `mapstructure:"from" validate:"required,oneof=json yaml cbor msgpack"`

	// Target format: json, yaml, cbor, msgpack
	To string `mapstructure:"to" validate:"required,oneof=json yaml cbor msgpack"`

	// Pretty print (JSON only)
	Pretty bool `mapstructure:"pretty" default:"false"`

	// Indent string (JSON/YAML only)
	Indent string `mapstructure:"indent" default:"  "`
}

// EncodeOptions defines options for encoding operations
type EncodeOptions struct {
	// Encoding type: base64, base64url, hex, url
	Encoding string `mapstructure:"encoding" validate:"required,oneof=base64 base64url hex url"`

	// Operation: encode or decode
	Operation string `mapstructure:"operation" default:"encode" validate:"oneof=encode decode"`
}

// CompressOptions defines options for compression operations
type CompressOptions struct {
	// Algorithm: gzip, zstd
	Algorithm string `mapstructure:"algorithm" validate:"required,oneof=gzip zstd"`

	// Operation: compress or decompress
	Operation string `mapstructure:"operation" default:"compress" validate:"oneof=compress decompress"`

	// Compression level (algorithm-specific)
	Level int `mapstructure:"level" default:"6" validate:"gte=-1,lte=9"`
}

// PartConfig defines a part in multipart composition
type PartConfig struct {
	// Part name
	Name string `mapstructure:"name" validate:"required"`

	// Source: data, metadata, filesystem, template
	Source string `mapstructure:"source" validate:"required,oneof=data metadata filesystem template"`

	// Filesystem path (for source=filesystem)
	Path string `mapstructure:"path"`

	// Metadata keys to include (for source=metadata)
	Keys []string `mapstructure:"keys"`

	// Metadata format: json, yaml, text (for source=metadata)
	Format string `mapstructure:"format" default:"json" validate:"oneof=json yaml text"`

	// Content type
	ContentType string `mapstructure:"contentType"`

	// Filename (for file parts)
	Filename string `mapstructure:"filename"`

	// Compress part data
	Compress bool `mapstructure:"compress"`

	// Template content (for source=template)
	Template string `mapstructure:"template"`

	// Additional headers
	Headers map[string]string `mapstructure:"headers"`
}

// MultipartOptions defines options for multipart operations
type MultipartOptions struct {
	// Parts to compose
	Parts []PartConfig `mapstructure:"parts" validate:"required,min=1,dive"`

	// Clear existing parts before adding new ones
	ClearExisting bool `mapstructure:"clearExisting" default:"false"`
}

// ExtractOptions defines options for extraction operations
type ExtractOptions struct {
	// Source format: json, yaml
	Format string `mapstructure:"format" validate:"required,oneof=json yaml"`

	// Fields to extract
	Fields []ExtractField `mapstructure:"fields" validate:"required,min=1,dive"`
}

// ExtractField defines a field to extract
type ExtractField struct {
	// JSON path or YAML path to field
	Path string `mapstructure:"path" validate:"required"`

	// Destination: data or metadata
	Destination string `mapstructure:"destination" default:"data" validate:"oneof=data metadata"`

	// Metadata key (if destination=metadata)
	Key string `mapstructure:"key"`
}

// MergeOptions defines options for merge operations
type MergeOptions struct {
	// Sources to merge
	Sources []MergeSource `mapstructure:"sources" validate:"required,min=1,dive"`

	// Output format: json, yaml
	Format string `mapstructure:"format" default:"json" validate:"oneof=json yaml"`
}

// MergeSource defines a source to merge
type MergeSource struct {
	// Input source: data, metadata, filesystem
	Input string `mapstructure:"input" validate:"required,oneof=data metadata filesystem"`

	// Key in merged output
	Key string `mapstructure:"key" validate:"required"`

	// Filesystem path (for input=filesystem)
	Path string `mapstructure:"path"`
}

// SplitOptions defines options for split operations
type SplitOptions struct {
	// Delimiter to split on
	Delimiter string `mapstructure:"delimiter" validate:"required"`

	// Part name prefix
	PartNamePrefix string `mapstructure:"partNamePrefix" default:"part"`

	// Content type for parts
	ContentType string `mapstructure:"contentType" default:"text/plain"`

	// Maximum number of parts
	MaxParts int `mapstructure:"maxParts" default:"100" validate:"gt=0"`
}
