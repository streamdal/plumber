package generic

type StructTemplateBuilder struct {
	structTemplate *StructTemplate
}

func DefineStruct(structName string) *StructTemplateBuilder {
	return &StructTemplateBuilder{
		structTemplate: &StructTemplate{
			structName: structName,
			generators: map[string]interface{}{
				"name": genName,
				"method": genMethod,
				"returnType": genReturnType,
			},
		},
	}
}

func (builder *StructTemplateBuilder) Source(source string) *StructTemplate {
	builder.structTemplate.templateSource = source
	return builder.structTemplate
}

type StructTemplate struct {
	structName string
	templateSource string
	generators map[string]interface{}
}