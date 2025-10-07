package connectors

const NewServiceMethodName = "NewService"
const NewServiceConfigName = "NewServiceConfig"

type Service interface {
	List() ([]string, error)
	IsValidMethod(string, []any) bool
	Call(string, []any) ([]byte, error)
	Close() error
}

type ServiceConfig struct {
	Name    string         `yaml:"name" json:"name"`
	Type    string         `yaml:"type" json:"type"`
	Options map[string]any `yaml:"options" json:"options"`
}
